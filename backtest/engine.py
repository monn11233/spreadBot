"""
Backtesting engine.

Replays stored tick data through the PRODUCTION scanner and paper trader.
No separate backtest logic — same code path as live ensures results are meaningful.

Key design:
  - SimulatedClock advances time as events are replayed
  - Events injected directly into the PriceEventBus
  - Scanner + paper trader process them identically to real-time
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional

from core.clock import SimulatedClock
from core.events import PriceEventBus
from core.models import PriceEvent, TradeMode
from data.tick_store import TickStore
from data.trade_store import TradeStore
from execution.paper_trader import PaperTrader
from scanner.fee_model import FeeCalculator
from scanner.opportunity_scanner import OpportunityScanner
from scanner.volatility_tracker import VolatilityTracker

logger = logging.getLogger(__name__)


class BacktestEngine:
    """
    Replays stored ticks and runs the full scanner + paper trader pipeline.
    """

    def __init__(
        self,
        tick_store: TickStore,
        trade_store: TradeStore,
        fee_calculator: FeeCalculator,
        min_profit_pct: float = 0.005,
        max_position_usd: float = 500.0,
        min_pool_volume_usd: float = 0.0,  # Relax volume filter for backtest
        tag: str = "",
    ):
        self._tick_store = tick_store
        self._trade_store = trade_store
        self._fee_calc = fee_calculator
        self._min_profit_pct = min_profit_pct
        self._max_position_usd = max_position_usd
        self._min_pool_volume_usd = min_pool_volume_usd
        self._tag = tag

    async def run(
        self,
        start_dt: datetime,
        end_dt: datetime,
    ) -> dict:
        """
        Run a backtest over the given date range.
        Returns summary stats.
        """
        start_ns = int(start_dt.timestamp() * 1e9)
        end_ns = int(end_dt.timestamp() * 1e9)

        logger.info(
            "[backtest] Running %s → %s (threshold=%.4f%%)",
            start_dt.date(), end_dt.date(), self._min_profit_pct * 100,
        )

        # Load all ticks for this period
        events: List[PriceEvent] = await self._tick_store.read_all_pairs_range(start_ns, end_ns)
        logger.info("[backtest] Loaded %d ticks", len(events))

        if not events:
            logger.warning("[backtest] No tick data found for this period")
            return {"ticks": 0, "trades": 0}

        # Build a fresh bus, vol tracker, scanner, and paper trader
        bus = PriceEventBus(maxsize=50_000)
        clock = SimulatedClock(start_ns=start_ns)
        vol_tracker = VolatilityTracker(min_profit_base=self._min_profit_pct)

        paper_trader = PaperTrader(
            trade_store=self._trade_store,
            mode=TradeMode.BACKTEST,
            min_profit_threshold=self._min_profit_pct,
        )

        scanner = OpportunityScanner(
            bus=bus,
            fee_calculator=self._fee_calc,
            vol_tracker=vol_tracker,
            min_profit_pct=self._min_profit_pct,
            max_position_usd=self._max_position_usd,
            min_pool_volume_usd=self._min_pool_volume_usd,
            on_opportunity=lambda opp: asyncio.ensure_future(
                paper_trader.on_opportunity(opp)
            ),
        )

        # Run scanner in background task
        scanner_task = asyncio.create_task(scanner.run())

        # Replay all events
        for event in events:
            clock.set_time(event.timestamp_ns)
            bus.put_nowait(event)

        # Drain the queue
        await asyncio.sleep(0.1)
        await scanner.stop()

        # Give any pending opportunity futures time to complete
        await asyncio.sleep(0.05)
        scanner_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            pass

        # Flush any remaining DB writes
        await self._tick_store.flush()

        stats = scanner.stats
        trader_stats = paper_trader.stats
        result = {
            "threshold": self._min_profit_pct,
            "tag": self._tag,
            "start": str(start_dt.date()),
            "end": str(end_dt.date()),
            "ticks": len(events),
            **stats,
            **trader_stats,
        }
        logger.info(
            "[backtest] Done: threshold=%.4f%% trades=%d pnl=$%.4f",
            self._min_profit_pct * 100,
            trader_stats["total_trades"],
            trader_stats["total_pnl_usd"],
        )
        return result
