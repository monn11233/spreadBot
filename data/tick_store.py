"""
Write and read raw PriceEvent ticks to/from SQLite.
Writes are batched to minimize I/O overhead.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional

from core.models import Chain, PriceEvent
from data.db import Database

logger = logging.getLogger(__name__)

_INSERT_SQL = """
    INSERT INTO tick_data
        (source, pair, bid, ask, ts_ns, chain, pool_addr, block_num, vol_24h_usd)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class TickStore:
    """
    Batches PriceEvent writes and flushes to SQLite periodically.
    Designed to be called from a background executor so the hot loop never blocks.
    """

    def __init__(self, db: Database, batch_size: int = 500, flush_interval: float = 1.0):
        self._db = db
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._buffer: List[tuple] = []
        self._last_flush = time.monotonic()
        self._total_written = 0

    def _to_row(self, event: PriceEvent) -> tuple:
        return (
            event.source,
            event.pair,
            event.bid,
            event.ask,
            event.timestamp_ns,
            event.chain.value if event.chain else None,
            event.pool_address,
            event.block_number,
            event.volume_24h_usd,
        )

    async def write(self, event: PriceEvent) -> None:
        """Buffer an event. Flushes automatically when batch is full or interval elapsed."""
        self._buffer.append(self._to_row(event))
        now = time.monotonic()
        if len(self._buffer) >= self._batch_size or (now - self._last_flush) >= self._flush_interval:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        rows = self._buffer[:]
        self._buffer.clear()
        self._last_flush = time.monotonic()
        await self._db.executemany(_INSERT_SQL, rows)
        await self._db.commit()
        self._total_written += len(rows)

    async def read_range(
        self,
        pair: str,
        start_ns: int,
        end_ns: int,
        source: Optional[str] = None,
    ) -> List[PriceEvent]:
        """Read ticks for backtesting replay."""
        if source:
            sql = (
                "SELECT * FROM tick_data WHERE pair=? AND ts_ns BETWEEN ? AND ? AND source=? "
                "ORDER BY ts_ns ASC"
            )
            rows = await self._db.fetchall(sql, (pair, start_ns, end_ns, source))
        else:
            sql = (
                "SELECT * FROM tick_data WHERE pair=? AND ts_ns BETWEEN ? AND ? "
                "ORDER BY ts_ns ASC"
            )
            rows = await self._db.fetchall(sql, (pair, start_ns, end_ns))

        events = []
        for row in rows:
            chain_val = row["chain"]
            evt = PriceEvent(
                source=row["source"],
                pair=row["pair"],
                bid=row["bid"],
                ask=row["ask"],
                timestamp_ns=row["ts_ns"],
                chain=Chain(chain_val) if chain_val else None,
                pool_address=row["pool_addr"],
                block_number=row["block_num"],
                volume_24h_usd=row["vol_24h_usd"] or 0.0,
            )
            events.append(evt)
        return events

    async def read_all_pairs_range(
        self, start_ns: int, end_ns: int
    ) -> List[PriceEvent]:
        """Read all ticks in a time range, ordered by timestamp (for backtest engine)."""
        sql = (
            "SELECT * FROM tick_data WHERE ts_ns BETWEEN ? AND ? ORDER BY ts_ns ASC"
        )
        rows = await self._db.fetchall(sql, (start_ns, end_ns))
        events = []
        for row in rows:
            chain_val = row["chain"]
            evt = PriceEvent(
                source=row["source"],
                pair=row["pair"],
                bid=row["bid"],
                ask=row["ask"],
                timestamp_ns=row["ts_ns"],
                chain=Chain(chain_val) if chain_val else None,
                pool_address=row["pool_addr"],
                block_number=row["block_num"],
                volume_24h_usd=row["vol_24h_usd"] or 0.0,
            )
            events.append(evt)
        return events

    @property
    def total_written(self) -> int:
        return self._total_written
