"""
Paper trader — simulates trade execution without placing real orders.

Tracks running P&L and persists TradeRecord to SQLite.
Used for both live paper trading and backtest replay.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional

from core.models import Direction, Opportunity, TradeMode, TradeRecord, TradeStatus
from data.trade_store import TradeStore

logger = logging.getLogger(__name__)


class PaperTrader:
    """
    Simulates fills at the price observed at signal time.
    No slippage modeled in paper mode (conservative: we assume we get the seen price).
    """

    def __init__(
        self,
        trade_store: TradeStore,
        mode: TradeMode = TradeMode.PAPER,
        min_profit_threshold: float = 0.005,
    ):
        self._store = trade_store
        self._mode = mode
        self._min_profit_threshold = min_profit_threshold

        # Running stats
        self._total_trades = 0
        self._winning_trades = 0
        self._total_pnl_usd = 0.0
        self._total_invested_usd = 0.0

        # Cooldown: avoid re-entering same pair+direction within N seconds
        self._last_trade_ns: Dict[str, int] = {}
        self._cooldown_ns = 2_000_000_000  # 2 seconds

    async def on_opportunity(self, opp: Opportunity) -> Optional[TradeRecord]:
        """Called by the scanner when a viable opportunity is detected.
        Returns the TradeRecord if a trade was simulated, else None."""
        if not opp.is_viable:
            return None

        # Cooldown check: don't spam the same pair
        cooldown_key = f"{opp.pair}:{opp.direction.name}"
        now_ns = time.time_ns()
        last_ns = self._last_trade_ns.get(cooldown_key, 0)
        if (now_ns - last_ns) < self._cooldown_ns:
            return None
        self._last_trade_ns[cooldown_key] = now_ns

        # Simulate fill at observed prices
        record = self._simulate_fill(opp)
        await self._store.insert(record)
        self._update_stats(record)

        direction = "CEX→DEX" if opp.direction == Direction.BUY_CEX_SELL_DEX else "DEX→CEX"
        logger.info(
            "[paper] %s %s (%s) net=%.3f%% $%.4f | total P&L: $%.2f",
            opp.pair,
            direction,
            opp.chain.value,
            opp.net_profit_pct * 100,
            record.net_pnl_usd,
            self._total_pnl_usd,
        )
        return record

    def _simulate_fill(self, opp: Opportunity) -> TradeRecord:
        """
        Simulate a paper fill.
        Assumes fill at the exact bid/ask prices observed at signal time.
        """
        notional = opp.notional_usd
        fee_model = opp.fee_model

        # Gross profit = raw spread × notional
        gross_pnl = opp.raw_spread_pct * notional

        # All fees
        cex_fee_usd = fee_model.cex_taker_fee * notional
        dex_fee_usd = fee_model.dex_pool_fee * notional
        gas_usd = fee_model.gas_cost_usd
        fees_usd = cex_fee_usd + dex_fee_usd
        net_pnl = gross_pnl - fees_usd - gas_usd

        return TradeRecord(
            mode=self._mode,
            pair=opp.pair,
            direction=opp.direction.name,
            chain=opp.chain.value,
            pool_address=opp.pool_address,
            entry_time_ns=opp.detected_at_ns,
            exit_time_ns=time.time_ns(),
            cex_fill_price=opp.cex_price,
            dex_fill_price=opp.dex_price_usd,
            notional_usd=notional,
            gross_pnl_usd=gross_pnl,
            fees_usd=fees_usd,
            gas_usd=gas_usd,
            net_pnl_usd=net_pnl,
            net_pnl_pct=net_pnl / notional if notional > 0 else 0.0,
            min_profit_threshold_used=self._min_profit_threshold,
            raw_spread_at_entry=opp.raw_spread_pct,
            realized_vol_at_entry=opp.vol_state.realized_vol_pct,
            slippage_usd=0.0,  # Not modeled in paper mode
            status=TradeStatus.FILLED,
        )

    def _update_stats(self, record: TradeRecord) -> None:
        self._total_trades += 1
        self._total_pnl_usd += record.net_pnl_usd
        self._total_invested_usd += record.notional_usd
        if record.net_pnl_usd > 0:
            self._winning_trades += 1

    @property
    def stats(self) -> dict:
        win_rate = (
            self._winning_trades / self._total_trades if self._total_trades > 0 else 0.0
        )
        return {
            "mode": self._mode.value,
            "total_trades": self._total_trades,
            "winning_trades": self._winning_trades,
            "win_rate": win_rate,
            "total_pnl_usd": self._total_pnl_usd,
            "total_invested_usd": self._total_invested_usd,
            "roi_pct": (
                self._total_pnl_usd / self._total_invested_usd * 100
                if self._total_invested_usd > 0 else 0.0
            ),
        }

    def set_min_profit_threshold(self, threshold: float) -> None:
        self._min_profit_threshold = threshold
