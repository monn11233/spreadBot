"""
SQLite connection and schema management.
Uses WAL mode for concurrent reads during backtest replay.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS tick_data (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT    NOT NULL,
    pair        TEXT    NOT NULL,
    bid         REAL    NOT NULL,
    ask         REAL    NOT NULL,
    ts_ns       INTEGER NOT NULL,
    chain       TEXT,
    pool_addr   TEXT,
    block_num   INTEGER,
    vol_24h_usd REAL    DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tick_pair_ts     ON tick_data(pair, ts_ns);
CREATE INDEX IF NOT EXISTS idx_tick_source_ts   ON tick_data(source, ts_ns);
CREATE INDEX IF NOT EXISTS idx_tick_ts          ON tick_data(ts_ns);

CREATE TABLE IF NOT EXISTS trade_records (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    mode                        TEXT    NOT NULL,
    pair                        TEXT    NOT NULL,
    direction                   TEXT    NOT NULL,
    chain                       TEXT    NOT NULL,
    pool_address                TEXT    NOT NULL,
    entry_time_ns               INTEGER NOT NULL,
    exit_time_ns                INTEGER,
    cex_fill_price              REAL    NOT NULL,
    dex_fill_price              REAL    NOT NULL,
    notional_usd                REAL    NOT NULL,
    gross_pnl_usd               REAL    NOT NULL,
    fees_usd                    REAL    NOT NULL,
    gas_usd                     REAL    NOT NULL,
    net_pnl_usd                 REAL    NOT NULL,
    net_pnl_pct                 REAL    NOT NULL,
    min_profit_threshold_used   REAL    NOT NULL,
    raw_spread_at_entry         REAL    NOT NULL,
    realized_vol_at_entry       REAL    NOT NULL,
    slippage_usd                REAL    NOT NULL DEFAULT 0,
    status                      TEXT    NOT NULL DEFAULT 'filled',
    notes                       TEXT    DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_trade_pair       ON trade_records(pair);
CREATE INDEX IF NOT EXISTS idx_trade_mode_ts    ON trade_records(mode, entry_time_ns);
CREATE INDEX IF NOT EXISTS idx_trade_threshold  ON trade_records(min_profit_threshold_used);
CREATE INDEX IF NOT EXISTS idx_trade_chain      ON trade_records(chain);

CREATE TABLE IF NOT EXISTS gas_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    chain           TEXT    NOT NULL,
    ts_ns           INTEGER NOT NULL,
    gas_gwei        REAL    NOT NULL,
    gas_usd_est     REAL    NOT NULL,
    block_num       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_gas_chain_ts ON gas_snapshots(chain, ts_ns);

CREATE TABLE IF NOT EXISTS learning_state (
    pair                TEXT    NOT NULL,
    chain               TEXT    NOT NULL,
    direction           TEXT    NOT NULL,
    trade_count         INTEGER NOT NULL DEFAULT 0,
    win_rate            REAL    NOT NULL DEFAULT 0.5,
    avg_profit          REAL    NOT NULL DEFAULT 0.0,
    avg_loss            REAL    NOT NULL DEFAULT 0.0,
    ev_score            REAL    NOT NULL DEFAULT 0.0,
    adaptive_threshold  REAL    NOT NULL DEFAULT 0.005,
    cooldown_until_ns   INTEGER NOT NULL DEFAULT 0,
    last_trade_ns       INTEGER NOT NULL DEFAULT 0,
    updated_at_ns       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pair, chain, direction)
);
CREATE INDEX IF NOT EXISTS idx_ls_ev ON learning_state(ev_score DESC);

CREATE TABLE IF NOT EXISTS pairs_cache (
    symbol          TEXT PRIMARY KEY,
    base_asset      TEXT NOT NULL,
    quote_asset     TEXT NOT NULL,
    rank            INTEGER,
    updated_at_ns   INTEGER NOT NULL
);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(SCHEMA_SQL)
        await self._conn.commit()
        logger.info("Database connected: %s", self.db_path)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected — call await db.connect() first")
        return self._conn

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        return await self.conn.execute(sql, params)

    async def executemany(self, sql: str, params_seq) -> None:
        await self.conn.executemany(sql, params_seq)

    async def commit(self) -> None:
        await self.conn.commit()

    async def fetchall(self, sql: str, params: tuple = ()):
        async with self.conn.execute(sql, params) as cur:
            return await cur.fetchall()

    async def fetchone(self, sql: str, params: tuple = ()):
        async with self.conn.execute(sql, params) as cur:
            return await cur.fetchone()
