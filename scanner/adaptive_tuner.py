"""
Adaptive tuner — learns per-(pair, chain, direction) performance over time
and adjusts min_profit thresholds automatically.

Algorithm:
  - After every paper trade, update EMA win_rate, avg_profit, avg_loss
  - Recompute ev_score = win_rate * avg_profit - (1 - win_rate) * avg_loss
  - Adjust adaptive_threshold:
      win_rate > 70%  (n >= MIN_SAMPLES) → lower by 10%  (be more aggressive)
      win_rate < 40%  (n >= MIN_SAMPLES) → raise by 20%  (be more selective)
      win_rate < 20%  (n >= MIN_SAMPLES) → cooldown 30 min
  - get_threshold() is synchronous (hot-path safe) — reads from in-memory dict
  - Persist to SQLite every 60s in background loop
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, NamedTuple

from core.models import TradeRecord
from data.db import Database

logger = logging.getLogger(__name__)


class PairKey(NamedTuple):
    pair: str
    chain: str
    direction: str


@dataclass
class LearningState:
    key: PairKey
    trade_count: int = 0
    win_rate: float = 0.5
    avg_profit: float = 0.0   # EMA of net_pnl_pct when trade is a win (positive)
    avg_loss: float = 0.0     # EMA of abs(net_pnl_pct) when trade is a loss (positive)
    ev_score: float = 0.0
    adaptive_threshold: float = 0.005
    cooldown_until_ns: int = 0
    last_trade_ns: int = 0
    _dirty: bool = field(default=False, repr=False, compare=False)

    def is_in_cooldown(self) -> bool:
        return time.time_ns() < self.cooldown_until_ns

    @property
    def cooldown_remaining_s(self) -> float:
        remaining = (self.cooldown_until_ns - time.time_ns()) / 1e9
        return max(0.0, remaining)

    def to_dict(self) -> dict:
        return {
            "pair": self.key.pair,
            "chain": self.key.chain,
            "direction": self.key.direction,
            "trade_count": self.trade_count,
            "win_rate": self.win_rate,
            "avg_profit_pct": self.avg_profit,
            "avg_loss_pct": self.avg_loss,
            "ev_score": self.ev_score,
            "adaptive_threshold": self.adaptive_threshold,
            "in_cooldown": self.is_in_cooldown(),
            "cooldown_remaining_s": self.cooldown_remaining_s,
        }


_UPSERT_SQL = """
    INSERT INTO learning_state
        (pair, chain, direction, trade_count, win_rate, avg_profit, avg_loss,
         ev_score, adaptive_threshold, cooldown_until_ns, last_trade_ns, updated_at_ns)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(pair, chain, direction) DO UPDATE SET
        trade_count         = excluded.trade_count,
        win_rate            = excluded.win_rate,
        avg_profit          = excluded.avg_profit,
        avg_loss            = excluded.avg_loss,
        ev_score            = excluded.ev_score,
        adaptive_threshold  = excluded.adaptive_threshold,
        cooldown_until_ns   = excluded.cooldown_until_ns,
        last_trade_ns       = excluded.last_trade_ns,
        updated_at_ns       = excluded.updated_at_ns
"""


class AdaptiveTuner:
    EMA_ALPHA = 0.15            # ~7-trade memory
    HIGH_WIN_RATE = 0.70
    LOW_WIN_RATE = 0.40
    VERY_LOW_WIN_RATE = 0.20
    MIN_SAMPLES = 10
    LOWER_FACTOR = 0.90         # reduce threshold by 10%
    RAISE_FACTOR = 1.20         # raise threshold by 20%
    THRESHOLD_FLOOR = 0.001     # never below 0.1%
    THRESHOLD_CEIL = 0.05       # never above 5%
    COOLDOWN_NS = 30 * 60 * 1_000_000_000   # 30 minutes
    PERSIST_INTERVAL_S = 60.0

    def __init__(self, db: Database, base_threshold: float = 0.005):
        self._db = db
        self._base = base_threshold
        self._states: Dict[PairKey, LearningState] = {}
        self._running = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def load(self) -> None:
        """Load persisted learning state from SQLite into memory."""
        rows = await self._db.fetchall(
            "SELECT pair, chain, direction, trade_count, win_rate, avg_profit, avg_loss, "
            "ev_score, adaptive_threshold, cooldown_until_ns, last_trade_ns FROM learning_state"
        )
        for row in rows:
            key = PairKey(row["pair"], row["chain"], row["direction"])
            self._states[key] = LearningState(
                key=key,
                trade_count=row["trade_count"],
                win_rate=row["win_rate"],
                avg_profit=row["avg_profit"],
                avg_loss=row["avg_loss"],
                ev_score=row["ev_score"],
                adaptive_threshold=row["adaptive_threshold"],
                cooldown_until_ns=row["cooldown_until_ns"],
                last_trade_ns=row["last_trade_ns"],
            )
        logger.info("[tuner] Loaded %d learning states from DB", len(self._states))

    # ------------------------------------------------------------------
    # Hot-path query (synchronous, no I/O)
    # ------------------------------------------------------------------

    def get_threshold(self, pair: str, chain: str, direction: str) -> float:
        """
        Returns the adaptive threshold for this key.
        Returns float('inf') if in cooldown — scanner skips the pair.
        Falls back to base_threshold for unseen pairs.
        """
        key = PairKey(pair, chain, direction)
        state = self._states.get(key)
        if state is None:
            return self._base
        if state.is_in_cooldown():
            return float("inf")
        return state.adaptive_threshold

    def is_in_cooldown(self, pair: str, chain: str, direction: str) -> bool:
        state = self._states.get(PairKey(pair, chain, direction))
        return state.is_in_cooldown() if state else False

    # ------------------------------------------------------------------
    # Learning — called after each trade
    # ------------------------------------------------------------------

    async def record_outcome(self, record: TradeRecord) -> None:
        """Update EMA stats and adjust threshold based on this trade outcome."""
        key = PairKey(record.pair, record.chain, record.direction)
        state = self._states.get(key)
        if state is None:
            state = LearningState(key=key, adaptive_threshold=self._base)
            self._states[key] = state

        is_win = record.net_pnl_usd > 0
        pnl_pct = abs(record.net_pnl_pct)

        # EMA updates
        state.win_rate = self._ema(state.win_rate, 1.0 if is_win else 0.0)
        if is_win:
            state.avg_profit = self._ema(state.avg_profit, pnl_pct) if state.avg_profit > 0 else pnl_pct
        else:
            state.avg_loss = self._ema(state.avg_loss, pnl_pct) if state.avg_loss > 0 else pnl_pct

        state.trade_count += 1
        state.last_trade_ns = record.entry_time_ns
        state.ev_score = (
            state.win_rate * state.avg_profit
            - (1.0 - state.win_rate) * state.avg_loss
        )

        # Threshold adjustment (only after enough samples)
        if state.trade_count >= self.MIN_SAMPLES:
            if state.win_rate < self.VERY_LOW_WIN_RATE:
                # Terrible performance — put in cooldown
                state.cooldown_until_ns = time.time_ns() + self.COOLDOWN_NS
                logger.info(
                    "[tuner] %s|%s|%s: win_rate=%.1f%% → COOLDOWN 30min",
                    key.pair, key.chain, key.direction, state.win_rate * 100,
                )
            elif state.win_rate < self.LOW_WIN_RATE:
                old = state.adaptive_threshold
                state.adaptive_threshold = min(
                    old * self.RAISE_FACTOR, self.THRESHOLD_CEIL
                )
                logger.debug(
                    "[tuner] %s|%s: threshold %.4f%%→%.4f%% (low win %.1f%%)",
                    key.pair, key.chain,
                    old * 100, state.adaptive_threshold * 100, state.win_rate * 100,
                )
            elif state.win_rate > self.HIGH_WIN_RATE:
                old = state.adaptive_threshold
                state.adaptive_threshold = max(
                    old * self.LOWER_FACTOR, self.THRESHOLD_FLOOR
                )
                logger.debug(
                    "[tuner] %s|%s: threshold %.4f%%→%.4f%% (high win %.1f%%)",
                    key.pair, key.chain,
                    old * 100, state.adaptive_threshold * 100, state.win_rate * 100,
                )

        state._dirty = True

    # ------------------------------------------------------------------
    # Dashboard queries
    # ------------------------------------------------------------------

    def get_favorites(self, top_n: int = 10) -> List[LearningState]:
        """Top N pairs by EV score (highest expected value first)."""
        scored = [s for s in self._states.values() if s.trade_count >= 3]
        return sorted(scored, key=lambda s: s.ev_score, reverse=True)[:top_n]

    def get_all_states(self) -> List[LearningState]:
        return sorted(self._states.values(), key=lambda s: s.ev_score, reverse=True)

    # ------------------------------------------------------------------
    # Background persist loop
    # ------------------------------------------------------------------

    async def run_persist_loop(self) -> None:
        self._running = True
        while self._running:
            await asyncio.sleep(self.PERSIST_INTERVAL_S)
            await self._flush_dirty()

    async def stop(self) -> None:
        self._running = False
        await self._flush_dirty()

    async def _flush_dirty(self) -> None:
        dirty = [s for s in self._states.values() if s._dirty]
        if not dirty:
            return
        rows = [
            (
                s.key.pair, s.key.chain, s.key.direction,
                s.trade_count, s.win_rate, s.avg_profit, s.avg_loss,
                s.ev_score, s.adaptive_threshold, s.cooldown_until_ns,
                s.last_trade_ns, time.time_ns(),
            )
            for s in dirty
        ]
        await self._db.executemany(_UPSERT_SQL, rows)
        await self._db.commit()
        for s in dirty:
            s._dirty = False
        logger.debug("[tuner] Persisted %d learning states", len(dirty))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ema(self, current: float, sample: float) -> float:
        return (1.0 - self.EMA_ALPHA) * current + self.EMA_ALPHA * sample
