"""
Fee model: computes the all-in cost for a CEX-DEX arb route.

Components:
  1. Binance taker fee (CEX leg)
  2. DEX pool fee (pool-tier dependent, immutable per pool)
  3. Gas cost in USD (cached from gas/*.py trackers)
  4. Slippage estimate (based on trade size vs. pool liquidity)
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

from core.models import Chain, FeeModel, GasState, PriceEvent

logger = logging.getLogger(__name__)


class FeeCalculator:
    """
    Computes FeeModel for a given (cex_event, dex_event, notional_usd) tuple.

    Gas states are injected by the gas trackers and cached here.
    Pool fee tiers are pre-registered (static, from pool config).
    """

    def __init__(self, cex_taker_fee: float = 0.001):
        """
        Args:
            cex_taker_fee: Binance taker fee fraction (default 0.1%).
        """
        self._cex_taker_fee = cex_taker_fee
        # chain → GasState (updated by gas trackers)
        self._gas_states: Dict[str, GasState] = {}
        # pool_address → pool_fee_fraction
        self._pool_fees: Dict[str, float] = {}

    def register_gas_state(self, chain: Chain, state: GasState) -> None:
        self._gas_states[chain.value] = state

    def register_pool_fee(self, pool_address: str, fee_tier: int) -> None:
        """Register a pool's fee tier (in bps × 100, e.g. 500 = 0.05%)."""
        self._pool_fees[pool_address.lower()] = fee_tier / 1_000_000

    def compute(
        self,
        dex_event: PriceEvent,
        notional_usd: float,
        slippage_estimate: float = 0.0,
    ) -> FeeModel:
        """
        Build a FeeModel for an arb leg on the given DEX event.

        Args:
            dex_event: The DEX-side PriceEvent.
            notional_usd: Intended trade size in USD.
            slippage_estimate: Estimated slippage as a fraction (e.g. 0.001 = 0.1%).
        """
        chain_val = dex_event.chain.value if dex_event.chain else "eth"
        gas_state = self._gas_states.get(chain_val)
        gas_usd = gas_state.gas_usd_per_swap if gas_state else self._fallback_gas(chain_val)

        pool_addr = (dex_event.pool_address or "").lower()
        dex_pool_fee = self._pool_fees.get(pool_addr, self._default_pool_fee(chain_val))

        return FeeModel(
            cex_taker_fee=self._cex_taker_fee,
            dex_pool_fee=dex_pool_fee,
            gas_cost_usd=gas_usd,
            notional_usd=notional_usd,
            slippage_estimate=slippage_estimate,
        )

    @staticmethod
    def _fallback_gas(chain: str) -> float:
        """Conservative fallback gas costs when tracker hasn't connected yet."""
        return {
            "eth": 20.0,   # ETH mainnet — expensive
            "arb": 0.20,   # Arbitrum — cheap
            "bsc": 0.15,   # BSC — cheap
            "sol": 0.001,  # Solana — nearly free
        }.get(chain, 5.0)

    @staticmethod
    def _default_pool_fee(chain: str) -> float:
        """Default pool fee when pool isn't pre-registered."""
        return {
            "eth": 0.003,   # Uniswap v3 0.3% most common pool
            "arb": 0.0005,  # Arbitrum has many 0.05% pools
            "bsc": 0.0025,  # PancakeSwap 0.25%
            "sol": 0.0005,  # Jupiter ~0.05%
        }.get(chain, 0.003)

    def estimate_slippage(
        self,
        notional_usd: float,
        pool_tvl_usd: float,
        pool_fee: float,
    ) -> float:
        """
        Estimate price impact (slippage) using a simplified CFMM model.
        For Uniswap v3 concentrated liquidity:
            impact ≈ notional / (2 * L)  where L ≈ pool_tvl / (2 * price_range_factor)

        For unknown TVL, returns a conservative 0.1% flat.
        """
        if pool_tvl_usd <= 0:
            return 0.001  # 0.1% conservative default

        # Simplified: trade size / TVL gives rough price impact
        impact = (notional_usd / pool_tvl_usd) * 2
        return min(impact, 0.05)  # cap at 5% — if higher, don't trade
