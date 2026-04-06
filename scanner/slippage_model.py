"""
Slippage simulation for DEX trades.

Models price impact using:

1. Uniswap V3 concentrated-liquidity math (tick-based)
2. Constant-product (x*y=k) fallback for pools without tick data
3. Binance CEX order-book depth estimation

Integrates with OpportunityScanner to reject opportunities where
real slippage eats the spread.

Usage:
    from scanner.slippage_model import SlippageModel
    model = SlippageModel(rpc_urls={Chain.ETHEREUM: "https://…"})
    result = await model.estimate_dex_slippage(pool_address, chain, trade_usd, direction)
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

from core.models import Chain

logger = logging.getLogger(__name__)

# ─── Uniswap V3 Constants ─────────────────────────────────────────

Q96 = 2**96
TICK_BASE = 1.0001

# Minimal ABI for on-chain queries
SLOT0_SIG = "0x3850c7bd"          # slot0()
LIQUIDITY_SIG = "0x1a686502"      # liquidity()
TICK_BITMAP_SIG = "0x5339c296"    # tickBitmap(int16)

# ERC20 decimals cache
_DECIMALS_SIG = "0x313ce567"


@dataclass
class SlippageEstimate:
    """Result of a slippage simulation."""
    price_impact_pct: float       # e.g. 0.003 = 0.3%
    effective_price: float        # price after impact
    mid_price: float              # mid price before impact
    liquidity_usd: float          # estimated available liquidity around current tick
    confidence: str               # "high" | "medium" | "low"
    method: str                   # "v3_ticks" | "constant_product" | "heuristic"
    warnings: List[str] = field(default_factory=list)

    @property
    def is_viable(self) -> bool:
        """Quick check: impact under 2% and confidence not low."""
        return self.price_impact_pct < 0.02 and self.confidence != "low"


class SlippageModel:
    """
    Estimates slippage for DEX swaps by querying on-chain pool state.

    Supports:
    - Uniswap V3 / PancakeSwap V3 (concentrated liquidity)
    - Constant-product fallback (reserves-based)
    - Cached results with configurable TTL
    """

    def __init__(
        self,
        rpc_urls: Dict[Chain, str],
        cache_ttl: float = 2.0,         # seconds to cache pool state
        max_ticks_to_cross: int = 20,    # max ticks to simulate
    ):
        self._rpc_urls = rpc_urls
        self._cache_ttl = cache_ttl
        self._max_ticks = max_ticks_to_cross
        self._cache: Dict[str, Tuple[float, dict]] = {}  # key -> (timestamp, data)
        self._session = None

    # ─── Public API ─────────────────────────────────────────────

    async def estimate_dex_slippage(
        self,
        pool_address: str,
        chain: Chain,
        trade_amount_usd: float,
        is_buy: bool,              # buying the base token (token0)
        token0_price_usd: float,   # current USD price of token0
        fee_tier: int = 3000,      # pool fee in bps (500, 3000, 10000)
    ) -> SlippageEstimate:
        """
        Estimate price impact for a swap of `trade_amount_usd` on `pool_address`.

        Returns a SlippageEstimate with impact %, effective price, and viability.
        """
        rpc_url = self._rpc_urls.get(chain)
        if not rpc_url:
            return self._heuristic_estimate(trade_amount_usd, fee_tier)

        try:
            # Try V3 concentrated-liquidity model first
            pool_state = await self._get_pool_state(pool_address, chain, rpc_url)
            if pool_state and pool_state.get("liquidity", 0) > 0:
                return self._simulate_v3_swap(
                    pool_state, trade_amount_usd, is_buy,
                    token0_price_usd, fee_tier
                )
            else:
                # Fallback: constant-product estimate from reserves
                return await self._constant_product_estimate(
                    pool_address, chain, rpc_url,
                    trade_amount_usd, is_buy, token0_price_usd
                )
        except Exception as e:
            logger.warning(f"Slippage estimation failed for {pool_address}: {e}")
            return self._heuristic_estimate(trade_amount_usd, fee_tier)

    async def estimate_cex_slippage(
        self,
        symbol: str,
        trade_amount_usd: float,
        order_book_depth: Optional[List[Tuple[float, float]]] = None,
    ) -> SlippageEstimate:
        """
        Estimate CEX slippage from order book depth.

        If no order book provided, uses a conservative heuristic
        based on typical Binance liquidity for top-20 pairs.
        """
        if order_book_depth:
            return self._walk_order_book(order_book_depth, trade_amount_usd)

        # Heuristic for Binance top pairs:
        # $500 trade typically sees <0.01% slippage on major pairs
        # Scale linearly: impact ≈ trade_usd / estimated_depth
        estimated_depth_usd = 500_000  # conservative for top-20 alts
        impact = trade_amount_usd / estimated_depth_usd
        return SlippageEstimate(
            price_impact_pct=impact,
            effective_price=0,  # caller fills from actual book
            mid_price=0,
            liquidity_usd=estimated_depth_usd,
            confidence="medium",
            method="cex_heuristic",
        )

    def net_spread_after_slippage(
        self,
        raw_spread_pct: float,
        dex_slippage: SlippageEstimate,
        cex_slippage: SlippageEstimate,
    ) -> float:
        """
        Compute the realistic net spread after accounting for slippage
        on both legs of the arb.

        raw_spread_pct: the observed price difference (e.g. 0.008 = 0.8%)
        Returns adjusted spread (can go negative if slippage kills it).
        """
        total_impact = dex_slippage.price_impact_pct + cex_slippage.price_impact_pct
        return raw_spread_pct - total_impact

    # ─── V3 Concentrated Liquidity Simulation ──────────────────

    def _simulate_v3_swap(
        self,
        pool_state: dict,
        trade_amount_usd: float,
        is_buy: bool,
        token0_price_usd: float,
        fee_tier: int,
    ) -> SlippageEstimate:
        """
        Simulate a swap through concentrated liquidity.

        Uses the current sqrtPriceX96 and in-range liquidity to estimate
        how far the price moves for a given trade size.
        """
        sqrt_price = pool_state["sqrtPriceX96"]
        liquidity = pool_state["liquidity"]
        tick = pool_state["tick"]

        # Current price from sqrtPriceX96
        price = (sqrt_price / Q96) ** 2
        # Adjust for token decimals if available
        decimals0 = pool_state.get("decimals0", 18)
        decimals1 = pool_state.get("decimals1", 18)
        decimal_adj = 10 ** (decimals0 - decimals1)
        adjusted_price = price * decimal_adj

        mid_price = adjusted_price

        # Convert trade USD to token amounts
        if is_buy:
            # Buying token0 with token1 (e.g., buying ETH with USDC)
            # We're swapping token1 -> token0
            trade_amount_token1 = trade_amount_usd  # assuming token1 ≈ stablecoin
            # For V3: Δ(1/√P) = Δx / L  (token0 out)
            #          Δ(√P) = Δy / L    (token1 in)
            delta_sqrt_price = (trade_amount_token1 * (10**decimals1)) / liquidity
            new_sqrt_price = sqrt_price + delta_sqrt_price
        else:
            # Selling token0 for token1
            trade_amount_token0 = trade_amount_usd / token0_price_usd
            delta_inv_sqrt = (trade_amount_token0 * (10**decimals0)) / liquidity
            inv_sqrt = (1 / sqrt_price) + delta_inv_sqrt
            new_sqrt_price = 1 / inv_sqrt if inv_sqrt > 0 else sqrt_price

        # New price after swap
        new_price = (new_sqrt_price / Q96) ** 2 * decimal_adj

        # Price impact
        if mid_price > 0:
            impact = abs(new_price - mid_price) / mid_price
        else:
            impact = 0.0

        # Apply pool fee as additional cost
        pool_fee_pct = fee_tier / 1_000_000

        # Estimate liquidity in USD terms (rough)
        # L² ≈ reserve0 * reserve1 in V3 at current tick
        liq_token0 = liquidity / sqrt_price * (10**decimals0) if sqrt_price > 0 else 0
        liq_usd = liq_token0 * token0_price_usd * 2  # rough both-sides estimate

        warnings = []
        if impact > 0.01:
            warnings.append(f"High price impact: {impact*100:.2f}%")
        if impact > 0.05:
            warnings.append("DANGER: Impact exceeds 5%, trade likely unprofitable")
        if liquidity < 1e15:
            warnings.append("Low in-range liquidity")

        confidence = "high" if liquidity > 1e18 else "medium" if liquidity > 1e15 else "low"

        return SlippageEstimate(
            price_impact_pct=impact + pool_fee_pct,
            effective_price=new_price,
            mid_price=mid_price,
            liquidity_usd=liq_usd,
            confidence=confidence,
            method="v3_ticks",
            warnings=warnings,
        )

    # ─── Constant Product Fallback ─────────────────────────────

    async def _constant_product_estimate(
        self,
        pool_address: str,
        chain: Chain,
        rpc_url: str,
        trade_amount_usd: float,
        is_buy: bool,
        token0_price_usd: float,
    ) -> SlippageEstimate:
        """
        x * y = k model for V2-style pools or when V3 tick data unavailable.

        Price impact = trade_amount / (reserve + trade_amount)
        """
        # If we can't get reserves, use heuristic
        # For V2: getReserves() = 0x0902f1ac
        try:
            reserves = await self._call_rpc(
                rpc_url, pool_address, "0x0902f1ac", chain
            )
            if reserves and len(reserves) >= 128:
                reserve0 = int(reserves[2:66], 16)
                reserve1 = int(reserves[66:130], 16)

                reserve0_usd = (reserve0 / 1e18) * token0_price_usd
                reserve1_usd = reserve1 / 1e18  # assume stablecoin
                total_liquidity = reserve0_usd + reserve1_usd

                # Constant product impact: Δp/p ≈ Δx / x
                if is_buy:
                    impact = trade_amount_usd / reserve1_usd if reserve1_usd > 0 else 1.0
                else:
                    impact = trade_amount_usd / reserve0_usd if reserve0_usd > 0 else 1.0

                return SlippageEstimate(
                    price_impact_pct=impact,
                    effective_price=0,  # caller computes
                    mid_price=0,
                    liquidity_usd=total_liquidity,
                    confidence="medium",
                    method="constant_product",
                )
        except Exception as e:
            logger.debug(f"Constant product fallback failed: {e}")

        return self._heuristic_estimate(trade_amount_usd, 3000)

    # ─── CEX Order Book Walk ───────────────────────────────────

    def _walk_order_book(
        self,
        depth: List[Tuple[float, float]],  # [(price, qty_usd), ...]
        trade_amount_usd: float,
    ) -> SlippageEstimate:
        """Walk the order book to compute actual fill price."""
        if not depth:
            return self._heuristic_estimate(trade_amount_usd, 0)

        mid_price = depth[0][0]
        filled = 0.0
        cost = 0.0

        for price, qty_usd in depth:
            can_fill = min(trade_amount_usd - filled, qty_usd)
            cost += can_fill * price / mid_price  # normalized
            filled += can_fill
            if filled >= trade_amount_usd:
                break

        if filled < trade_amount_usd:
            # Not enough depth — very dangerous
            return SlippageEstimate(
                price_impact_pct=0.05,  # assume 5% if book is thin
                effective_price=0,
                mid_price=mid_price,
                liquidity_usd=filled,
                confidence="low",
                method="order_book_partial",
                warnings=["Insufficient order book depth for full fill"],
            )

        avg_price = (cost / filled) * mid_price if filled > 0 else mid_price
        impact = abs(avg_price - mid_price) / mid_price if mid_price > 0 else 0

        return SlippageEstimate(
            price_impact_pct=impact,
            effective_price=avg_price,
            mid_price=mid_price,
            liquidity_usd=sum(q for _, q in depth),
            confidence="high",
            method="order_book",
        )

    # ─── Heuristic Fallback ────────────────────────────────────

    def _heuristic_estimate(
        self, trade_amount_usd: float, fee_tier: int
    ) -> SlippageEstimate:
        """
        Conservative heuristic when on-chain data isn't available.

        Assumes medium-liquidity pool. Good enough to filter out
        obviously bad trades, but should trigger a warning.
        """
        # Rough model: 0.1% impact per $1000 traded
        base_impact = (trade_amount_usd / 1000) * 0.001
        pool_fee = fee_tier / 1_000_000

        return SlippageEstimate(
            price_impact_pct=base_impact + pool_fee,
            effective_price=0,
            mid_price=0,
            liquidity_usd=0,
            confidence="low",
            method="heuristic",
            warnings=["Using heuristic — no on-chain data available"],
        )

    # ─── RPC Helpers ───────────────────────────────────────────

    async def _get_pool_state(
        self, pool_address: str, chain: Chain, rpc_url: str
    ) -> Optional[dict]:
        """Fetch slot0 + liquidity from a V3 pool, with caching."""
        cache_key = f"{chain.value}:{pool_address}"
        now = time.monotonic()

        # Check cache
        if cache_key in self._cache:
            ts, data = self._cache[cache_key]
            if now - ts < self._cache_ttl:
                return data

        try:
            # Batch: slot0() + liquidity() in one RPC batch
            slot0_raw = await self._call_rpc(rpc_url, pool_address, SLOT0_SIG, chain)
            liq_raw = await self._call_rpc(rpc_url, pool_address, LIQUIDITY_SIG, chain)

            if not slot0_raw or not liq_raw:
                return None

            # Parse slot0: (sqrtPriceX96, tick, observationIndex, ...)
            sqrt_price = int(slot0_raw[2:66], 16)
            # tick is int24, packed in bytes 32-64 of response
            tick_hex = slot0_raw[66:130]
            tick_raw = int(tick_hex, 16)
            # Handle signed int24
            if tick_raw >= 2**23:
                tick_raw -= 2**24

            liquidity = int(liq_raw[2:66], 16)

            state = {
                "sqrtPriceX96": sqrt_price,
                "tick": tick_raw,
                "liquidity": liquidity,
                "decimals0": 18,  # default; caller can override
                "decimals1": 18,
            }

            self._cache[cache_key] = (now, state)
            return state

        except Exception as e:
            logger.warning(f"Failed to fetch pool state {pool_address} on {chain}: {e}")
            return None

    async def _call_rpc(
        self, rpc_url: str, to: str, data: str, chain: Chain
    ) -> Optional[str]:
        """Make an eth_call JSON-RPC request."""
        import aiohttp

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
            "id": 1,
        }

        # Convert WSS URL to HTTPS for RPC calls
        http_url = rpc_url.replace("wss://", "https://").replace("ws://", "http://")

        try:
            async with self._session.post(
                http_url, json=payload, timeout=aiohttp.ClientTimeout(total=3.0)
            ) as resp:
                result = await resp.json()
                return result.get("result")
        except asyncio.TimeoutError:
            logger.warning(f"RPC timeout for {to} on {chain}")
            return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
