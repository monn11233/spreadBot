"""
Parameter sweep: run the backtesting engine across multiple
min_profit_pct thresholds and collect comparative metrics.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import List

from backtest.engine import BacktestEngine
from data.tick_store import TickStore
from data.trade_store import TradeStore
from scanner.fee_model import FeeCalculator

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLDS = [0.001, 0.0025, 0.005, 0.01, 0.02]


class ParamSweep:
    """
    Runs BacktestEngine for each threshold in the sweep list.
    Results are collected in memory and passed to the reporter.
    """

    def __init__(
        self,
        tick_store: TickStore,
        trade_store: TradeStore,
        fee_calculator: FeeCalculator,
        thresholds: List[float] = None,
        max_position_usd: float = 500.0,
    ):
        self._tick_store = tick_store
        self._trade_store = trade_store
        self._fee_calc = fee_calculator
        self._thresholds = thresholds or DEFAULT_THRESHOLDS
        self._max_position_usd = max_position_usd

    async def run(
        self,
        start_dt: datetime,
        end_dt: datetime,
    ) -> List[dict]:
        """
        Run backtest for each threshold sequentially.
        Sequential (not concurrent) to avoid SQLite contention.
        Returns list of result dicts ordered by threshold.
        """
        results = []
        for threshold in self._thresholds:
            engine = BacktestEngine(
                tick_store=self._tick_store,
                trade_store=self._trade_store,
                fee_calculator=self._fee_calc,
                min_profit_pct=threshold,
                max_position_usd=self._max_position_usd,
                tag=f"sweep_{threshold:.4f}",
            )
            result = await engine.run(start_dt, end_dt)
            results.append(result)

        return results
