"""
Persist and query TradeRecord objects in SQLite.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from core.models import TradeRecord, TradeMode
from data.db import Database

logger = logging.getLogger(__name__)

_INSERT_SQL = """
    INSERT INTO trade_records (
        mode, pair, direction, chain, pool_address,
        entry_time_ns, exit_time_ns,
        cex_fill_price, dex_fill_price, notional_usd,
        gross_pnl_usd, fees_usd, gas_usd, net_pnl_usd, net_pnl_pct,
        min_profit_threshold_used, raw_spread_at_entry,
        realized_vol_at_entry, slippage_usd, status, notes
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _record_to_row(r: TradeRecord) -> tuple:
    return (
        r.mode.value,
        r.pair,
        r.direction,
        r.chain,
        r.pool_address,
        r.entry_time_ns,
        r.exit_time_ns,
        r.cex_fill_price,
        r.dex_fill_price,
        r.notional_usd,
        r.gross_pnl_usd,
        r.fees_usd,
        r.gas_usd,
        r.net_pnl_usd,
        r.net_pnl_pct,
        r.min_profit_threshold_used,
        r.raw_spread_at_entry,
        r.realized_vol_at_entry,
        r.slippage_usd,
        r.status.value,
        r.notes,
    )


def _row_to_record(row) -> TradeRecord:
    from core.models import TradeStatus
    return TradeRecord(
        id=row["id"],
        mode=TradeMode(row["mode"]),
        pair=row["pair"],
        direction=row["direction"],
        chain=row["chain"],
        pool_address=row["pool_address"],
        entry_time_ns=row["entry_time_ns"],
        exit_time_ns=row["exit_time_ns"],
        cex_fill_price=row["cex_fill_price"],
        dex_fill_price=row["dex_fill_price"],
        notional_usd=row["notional_usd"],
        gross_pnl_usd=row["gross_pnl_usd"],
        fees_usd=row["fees_usd"],
        gas_usd=row["gas_usd"],
        net_pnl_usd=row["net_pnl_usd"],
        net_pnl_pct=row["net_pnl_pct"],
        min_profit_threshold_used=row["min_profit_threshold_used"],
        raw_spread_at_entry=row["raw_spread_at_entry"],
        realized_vol_at_entry=row["realized_vol_at_entry"],
        slippage_usd=row["slippage_usd"],
        status=TradeStatus(row["status"]),
        notes=row["notes"] or "",
    )


class TradeStore:
    def __init__(self, db: Database):
        self._db = db

    async def insert(self, record: TradeRecord) -> int:
        """Insert a trade record and return its row ID."""
        cur = await self._db.execute(_INSERT_SQL, _record_to_row(record))
        await self._db.commit()
        record.id = cur.lastrowid
        return cur.lastrowid

    async def query_by_mode(self, mode: TradeMode) -> List[TradeRecord]:
        rows = await self._db.fetchall(
            "SELECT * FROM trade_records WHERE mode=? ORDER BY entry_time_ns ASC",
            (mode.value,),
        )
        return [_row_to_record(r) for r in rows]

    async def query_by_threshold(self, threshold: float) -> List[TradeRecord]:
        rows = await self._db.fetchall(
            "SELECT * FROM trade_records WHERE min_profit_threshold_used=? ORDER BY entry_time_ns ASC",
            (threshold,),
        )
        return [_row_to_record(r) for r in rows]

    async def query_by_pair(self, pair: str, mode: Optional[str] = None) -> List[TradeRecord]:
        if mode:
            rows = await self._db.fetchall(
                "SELECT * FROM trade_records WHERE pair=? AND mode=? ORDER BY entry_time_ns ASC",
                (pair, mode),
            )
        else:
            rows = await self._db.fetchall(
                "SELECT * FROM trade_records WHERE pair=? ORDER BY entry_time_ns ASC",
                (pair,),
            )
        return [_row_to_record(r) for r in rows]

    async def summary_by_threshold(self) -> List[dict]:
        """Aggregate stats grouped by min_profit_threshold_used — used by backtest reporter."""
        sql = """
            SELECT
                min_profit_threshold_used  AS threshold,
                COUNT(*)                   AS total_trades,
                SUM(CASE WHEN net_pnl_usd > 0 THEN 1 ELSE 0 END) AS winning,
                SUM(net_pnl_usd)           AS total_net_pnl,
                AVG(net_pnl_pct)           AS avg_net_pnl_pct,
                MIN(net_pnl_usd)           AS min_pnl,
                MAX(net_pnl_usd)           AS max_pnl
            FROM trade_records
            WHERE mode IN ('backtest', 'paper')
            GROUP BY min_profit_threshold_used
            ORDER BY min_profit_threshold_used ASC
        """
        rows = await self._db.fetchall(sql)
        return [dict(r) for r in rows]
