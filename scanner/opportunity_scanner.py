"""
Opportunity scanner — the hot loop.

Consumes PriceEvents from the bus, updates the registry, and emits
Opportunity objects whenever a viable CEX-DEX spread is detected.

Architecture:
    All feeds → asyncio.Queue (PriceEventBus) → OpportunityScanner → asyncio.Queue (opportunities)

The scanner runs in a single asyncio coroutine. It never blocks —
all I/O (DB writes, gas updates) is dispatched to background tasks.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Dict, List, Optional, TYPE_CHECKING

from core.events import PriceEventBus
from core.models import Chain, Direction, FeeModel, Opportunity, PriceEvent, VolatilityState
from core.registry import PriceRegistry
from scanner.fee_model import FeeCalculator
from scanner.slippage_model import SlippageModel
from scanner.volatility_tracker import VolatilityTracker
if TYPE_CHECKING:
    from scanner.adaptive_tuner import AdaptiveTuner

logger = logging.getLogger(__name__)

# Sources that are DEX-side (not Binance CEX)
DEX_SOURCES = {"uniswap_eth", "uniswap_arb", "pancakeswap_bsc", "jupiter_sol"}
CEX_SOURCE = "binance"


class OpportunityScanner:
    """
    Reads PriceEvents from the bus, checks all cross-venue pairs,
    and emits Opportunity objects when net_profit >= dynamic threshold.
    """

    def __init__(
        self,
        bus: PriceEventBus,
        fee_calculator: FeeCalculator,
        vol_tracker: VolatilityTracker,
        min_profit_pct: float = 0.005,
        max_position_usd: float = 500.0,
        min_pool_volume_usd: float = 100_000.0,
        on_opportunity: Optional[Callable[[Opportunity], None]] = None,
        adaptive_tuner: Optional["AdaptiveTuner"] = None,
        rpc_urls: Optional[Dict[Chain, str]] = None,
    ):
        self._bus = bus
        self._fee_calc = fee_calculator
        self._vol = vol_tracker
        self._min_profit_pct = min_profit_pct
        self._max_position_usd = max_position_usd
        self._min_volume_usd = min_pool_volume_usd
        self._on_opportunity = on_opportunity
        self._tuner = adaptive_tuner
        self._registry = PriceRegistry()
        self._slippage_model = SlippageModel(rpc_urls=rpc_urls or {}, cache_ttl=2.0)

        # Stats
        self._events_processed = 0
        self._opportunities_found = 0
        self._opportunities_viable = 0
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main consume loop. Runs until stop() is called."""
        self._running = True
        logger.info("[scanner] Starting opportunity scanner")
        while self._running:
            try:
                event = await asyncio.wait_for(self._bus.get(), timeout=1.0)
                await self._process_event(event)
                self._bus.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("[scanner] Unexpected error: %s", exc, exc_info=True)

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Hot path
    # ------------------------------------------------------------------

    async def _process_event(self, event: PriceEvent) -> None:
        self._events_processed += 1

        # Update vol tracker for all events (CEX + DEX)
        vol_state = self._vol.update(event)

        # Update registry
        self._registry.update(event)

        # Only check for opportunities when we have both a CEX and DEX quote
        if event.source == CEX_SOURCE:
            # New Binance quote: check against all DEX sources for this pair
            await self._check_all_dex(event, vol_state)
        elif event.source in DEX_SOURCES:
            # New DEX quote: check against latest Binance quote for this pair
            cex_event = self._registry.get_binance(event.pair)
            if cex_event:
                await self._check_pair(cex_event, event, vol_state)

    async def _check_all_dex(self, cex_event: PriceEvent, vol_state: VolatilityState) -> None:
        """Check a fresh Binance quote against every DEX source for the same pair."""
        dex_events = self._registry.get_latest_for_pair(cex_event.pair)
        for source, dex_event in dex_events.items():
            if source in DEX_SOURCES:
                await self._check_pair(cex_event, dex_event, vol_state)

    async def _check_pair(
        self,
        cex_event: PriceEvent,
        dex_event: PriceEvent,
        vol_state: VolatilityState,
    ) -> None:
        """
        Core opportunity detection for a single (cex, dex) price pair.

        Checks both directions:
          A) BUY_CEX_SELL_DEX: buy at CEX ask, sell at DEX bid equivalent
          B) BUY_DEX_SELL_CEX: buy at DEX ask equivalent, sell at CEX bid
        """
        # Volume filter: skip illiquid DEX pools
        if dex_event.volume_24h_usd < self._min_volume_usd and dex_event.volume_24h_usd > 0:
            return

        notional = self._max_position_usd
        fee_model = self._fee_calc.compute(dex_event, notional)
        vol_threshold = vol_state.dynamic_min_profit
        chain_val = (dex_event.chain.value if dex_event.chain else "eth")

        # Direction A: buy on CEX, sell on DEX
        spread_a = (dex_event.bid - cex_event.ask) / cex_event.ask
        threshold_a = self._effective_threshold(
            cex_event.pair, chain_val, Direction.BUY_CEX_SELL_DEX.name, vol_threshold
        )
        await self._emit_if_viable(
            pair=cex_event.pair,
            direction=Direction.BUY_CEX_SELL_DEX,
            cex_price=cex_event.ask,
            dex_price=dex_event.bid,
            raw_spread_pct=spread_a,
            fee_model=fee_model,
            threshold=threshold_a,
            cex_event=cex_event,
            dex_event=dex_event,
            vol_state=vol_state,
            notional=notional,
        )

        # Direction B: buy on DEX, sell on CEX
        spread_b = (cex_event.bid - dex_event.ask) / dex_event.ask
        threshold_b = self._effective_threshold(
            cex_event.pair, chain_val, Direction.BUY_DEX_SELL_CEX.name, vol_threshold
        )
        await self._emit_if_viable(
            pair=cex_event.pair,
            direction=Direction.BUY_DEX_SELL_CEX,
            cex_price=cex_event.bid,
            dex_price=dex_event.ask,
            raw_spread_pct=spread_b,
            fee_model=fee_model,
            threshold=threshold_b,
            cex_event=cex_event,
            dex_event=dex_event,
            vol_state=vol_state,
            notional=notional,
        )

    async def _emit_if_viable(
        self,
        pair: str,
        direction: Direction,
        cex_price: float,
        dex_price: float,
        raw_spread_pct: float,
        fee_model: FeeModel,
        threshold: float,
        cex_event: PriceEvent,
        dex_event: PriceEvent,
        vol_state: VolatilityState,
        notional: float,
    ) -> None:
        net_profit_pct = raw_spread_pct - fee_model.total_cost_pct
        is_viable = net_profit_pct >= threshold

        self._opportunities_found += 1

        slippage_dex_pct = 0.0
        slippage_cex_pct = 0.0
        adjusted_spread_pct = net_profit_pct
        slippage_confidence = "unknown"

        if is_viable:
            fee_tier = int(fee_model.dex_pool_fee * 1_000_000)
            dex_slip = await self._slippage_model.estimate_dex_slippage(
                pool_address=dex_event.pool_address or "",
                chain=dex_event.chain or Chain.ETHEREUM,
                trade_amount_usd=notional,
                is_buy=(direction == Direction.BUY_DEX_SELL_CEX),
                token0_price_usd=cex_price,
                fee_tier=fee_tier,
            )
            cex_slip = await self._slippage_model.estimate_cex_slippage(
                symbol=pair,
                trade_amount_usd=notional,
            )
            adjusted_spread_pct = self._slippage_model.net_spread_after_slippage(
                raw_spread_pct=net_profit_pct,
                dex_slippage=dex_slip,
                cex_slippage=cex_slip,
            )
            slippage_dex_pct = dex_slip.price_impact_pct
            slippage_cex_pct = cex_slip.price_impact_pct
            slippage_confidence = dex_slip.confidence

            # Only hard-block when we have real slippage data (medium/high confidence).
            # Low-confidence (heuristic) estimates are attached for display but
            # don't kill the opportunity — we can't trust them enough to block.
            if slippage_confidence != "low" and not dex_slip.is_viable:
                logger.debug(
                    "[scanner] Opportunity killed by slippage (high-confidence) pair=%s dex=%.3f%% cex=%.3f%% adj=%.3f%%",
                    pair, slippage_dex_pct * 100, slippage_cex_pct * 100, adjusted_spread_pct * 100,
                )
                is_viable = False
            elif adjusted_spread_pct < threshold:
                logger.debug(
                    "[scanner] Opportunity killed by slippage-adjusted spread pair=%s adj=%.3f%% threshold=%.3f%%",
                    pair, adjusted_spread_pct * 100, threshold * 100,
                )
                is_viable = False

        if is_viable:
            self._opportunities_viable += 1

        opp = Opportunity(
            pair=pair,
            direction=direction,
            cex_price=cex_price,
            dex_price_usd=dex_price,
            raw_spread_pct=raw_spread_pct,
            net_profit_pct=net_profit_pct,
            fee_model=fee_model,
            chain=dex_event.chain or Chain.ETHEREUM,
            pool_address=dex_event.pool_address or "",
            detected_at_ns=time.time_ns(),
            cex_event=cex_event,
            dex_event=dex_event,
            vol_state=vol_state,
            is_viable=is_viable,
            notional_usd=notional,
            slippage_dex_pct=slippage_dex_pct,
            slippage_cex_pct=slippage_cex_pct,
            adjusted_spread_pct=adjusted_spread_pct,
            slippage_confidence=slippage_confidence,
        )

        if is_viable and self._on_opportunity:
            self._on_opportunity(opp)

    def _effective_threshold(
        self, pair: str, chain: str, direction: str, vol_threshold: float
    ) -> float:
        """Combine volatility threshold with adaptive tuner threshold."""
        if self._tuner:
            tuner_t = self._tuner.get_threshold(pair, chain, direction)
            return max(vol_threshold, tuner_t)  # inf from cooldown passes through
        return vol_threshold

    # ------------------------------------------------------------------
    # Config updates (used by param sweep)
    # ------------------------------------------------------------------

    def set_min_profit(self, threshold: float) -> None:
        """Update the minimum profit threshold (used during backtest sweeps)."""
        self._min_profit_pct = threshold
        self._vol.reset_base_threshold(threshold) if hasattr(self._vol, 'reset_base_threshold') else None

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        return {
            "events_processed": self._events_processed,
            "opportunities_found": self._opportunities_found,
            "opportunities_viable": self._opportunities_viable,
            "viable_rate": (
                self._opportunities_viable / self._opportunities_found
                if self._opportunities_found > 0 else 0.0
            ),
            "registry_size": len(self._registry),
            "bus_stats": self._bus.stats,
        }
