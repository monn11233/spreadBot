"""
Uniswap v3 WebSocket feed for Ethereum mainnet and Arbitrum.

Subscribes to pool Swap events via eth_subscribe("logs", ...) on an
Alchemy/Infura WebSocket RPC. Converts sqrtPriceX96 → USD price.

Swap event topic: 0xc42079f94a6350d7e07e4c9b7f3a5e54c23d3b92c2f88d52ee6fe8b4a7e1234
(keccak256 of "Swap(address,address,int256,int256,uint160,uint128,int24)")
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from core.events import PriceEventBus
from core.models import Chain, PriceEvent
from feeds.base_feed import BaseFeed

logger = logging.getLogger(__name__)

# keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)")
# Verified via Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)")
UNISWAP_V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


@dataclass
class PoolConfig:
    """Configuration for a single Uniswap v3 pool."""
    address: str         # Pool contract address (checksummed)
    pair: str            # Normalized pair: "SOL/USDT" (via wrapped tokens)
    token0_decimals: int
    token1_decimals: int
    fee_tier: int        # 100, 500, 3000, or 10000
    token0_is_base: bool = True  # If False, price is inverted

    @property
    def fee_pct(self) -> float:
        return self.fee_tier / 1_000_000


# Top Uniswap v3 ETH mainnet USDT pools for alts
# These are the highest-liquidity pools for each pair
# address: pool address, token0/token1 decimals depend on the actual pair
ETH_MAINNET_POOLS: List[PoolConfig] = [
    # WETH/USDT 0.05% — used for price reference
    PoolConfig(
        address="0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
        pair="ETH/USDT",
        token0_decimals=18,
        token1_decimals=6,
        fee_tier=500,
        token0_is_base=True,
    ),
]

ARB_MAINNET_POOLS: List[PoolConfig] = [
    # WETH/USDT 0.05% on Arbitrum
    PoolConfig(
        address="0x641C00A822e8b671738d32a431a4Fb6074E5c79d",
        pair="ETH/USDT",
        token0_decimals=18,
        token1_decimals=6,
        fee_tier=500,
        token0_is_base=True,
    ),
    # WETH/USDC 0.05% on Arbitrum — higher volume
    PoolConfig(
        address="0xC6962004f452bE9203591991D15f6b388e09E8D0",
        pair="ETH/USDC",
        token0_decimals=18,
        token1_decimals=6,
        fee_tier=500,
        token0_is_base=True,
    ),
]


def sqrt_price_x96_to_price(
    sqrt_price_x96: int,
    token0_decimals: int,
    token1_decimals: int,
    token0_is_base: bool = True,
) -> float:
    """
    Convert Uniswap v3 sqrtPriceX96 (Q64.96 fixed point) to a human-readable price.

    Formula: price_of_token0_in_token1 = (sqrtPriceX96 / 2^96)^2 * 10^(token0_dec - token1_dec)

    Uses Python big-int for the squaring step to preserve precision, then float division.
    The integer >> 192 approach loses all precision when raw price < 1 (common for 18/6 decimal pairs).

    If token0_is_base=True: returns price of token0 in terms of token1 (e.g. ETH/USDC).
    If token0_is_base=False: returns inverted price (token1 priced in token0).
    """
    if sqrt_price_x96 == 0:
        return 0.0

    # Big-int squaring for precision, then float division avoids integer truncation to 0
    q192 = 1 << 192
    price_raw = (sqrt_price_x96 * sqrt_price_x96) / q192  # float

    # Decimal adjustment: token0_dec - token1_dec (opposite of token1-token0)
    decimal_adj = 10 ** (token0_decimals - token1_decimals)
    price = price_raw * decimal_adj

    if not token0_is_base:
        price = 1.0 / price if price > 0 else 0.0

    return float(price)


def _decode_sqrt_price_from_log(data_hex: str) -> Optional[int]:
    """
    Decode sqrtPriceX96 from a Uniswap v3 Swap event log data field.

    Swap event non-indexed params in order:
        amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160),
        liquidity (uint128), tick (int24)

    Each is 32 bytes (padded). sqrtPriceX96 is at offset 2 (0-indexed).
    """
    try:
        data = data_hex[2:] if data_hex.startswith("0x") else data_hex
        # 5 params × 64 hex chars each
        if len(data) < 5 * 64:
            return None
        sqrt_price_hex = data[2 * 64: 3 * 64]  # 3rd param (0-indexed: 0,1,2)
        return int(sqrt_price_hex, 16)
    except (ValueError, IndexError):
        return None


class UniswapV3Feed(BaseFeed):
    """
    Subscribes to Uniswap v3 Swap events on ETH or Arbitrum via WebSocket RPC.
    Converts sqrtPriceX96 → USD price and pushes PriceEvent to bus.
    """

    def __init__(
        self,
        bus: PriceEventBus,
        ws_url: str,
        pools: List[PoolConfig],
        chain: Chain,
    ):
        name = f"uniswap_{chain.value}"
        super().__init__(bus, name=name)
        self._ws_url = ws_url
        self._pools = pools
        self._chain = chain
        # Map pool address (lowercase) → PoolConfig
        self._pool_map: Dict[str, PoolConfig] = {
            p.address.lower(): p for p in pools
        }
        self._sub_ids: Dict[str, str] = {}  # subscription_id → pool_address

    def _get_url(self) -> str:
        return self._ws_url

    async def _on_connect(self, ws) -> None:
        """Subscribe to Swap event logs for each pool."""
        for pool in self._pools:
            sub_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": pool.address,
                        "topics": [UNISWAP_V3_SWAP_TOPIC],
                    },
                ],
            }
            await ws.send(json.dumps(sub_request))
            logger.info(
                "[%s] Subscribed to pool %s (%s)",
                self.name, pool.address[:10], pool.pair,
            )

    async def _on_message(self, message) -> None:
        recv_ns = time.time_ns()
        data = json.loads(message)

        # Subscription confirmation: {"jsonrpc":"2.0","id":1,"result":"0xABC..."}
        if "result" in data and "id" in data and "params" not in data:
            return  # subscription ACK

        # Log notification: {"jsonrpc":"2.0","method":"eth_subscription","params":{...}}
        params = data.get("params")
        if not params:
            return

        result = params.get("result", {})
        if not isinstance(result, dict):
            return

        pool_addr = result.get("address", "").lower()
        pool_cfg = self._pool_map.get(pool_addr)
        if pool_cfg is None:
            return

        data_hex = result.get("data", "")
        block_hex = result.get("blockNumber", "0x0")

        sqrt_price = _decode_sqrt_price_from_log(data_hex)
        if sqrt_price is None or sqrt_price == 0:
            return

        price = sqrt_price_x96_to_price(
            sqrt_price,
            pool_cfg.token0_decimals,
            pool_cfg.token1_decimals,
            pool_cfg.token0_is_base,
        )
        if price <= 0:
            return

        try:
            block_num = int(block_hex, 16)
        except ValueError:
            block_num = 0

        # For a swap event we don't have separate bid/ask — use price ± half pool fee as spread
        half_spread = price * pool_cfg.fee_pct / 2
        bid = price - half_spread
        ask = price + half_spread

        event = PriceEvent(
            source=self.name,
            pair=pool_cfg.pair,
            bid=bid,
            ask=ask,
            timestamp_ns=recv_ns,
            chain=self._chain,
            pool_address=pool_cfg.address,
            block_number=block_num,
        )
        self._bus.put_nowait(event)
        self._record_latency(recv_ns)

    def add_pool(self, pool: PoolConfig) -> None:
        self._pools.append(pool)
        self._pool_map[pool.address.lower()] = pool
