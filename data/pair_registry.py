"""
Fetch and cache the top-N altcoin pairs from Binance.
Filters to USDT-quoted pairs, excludes BTC and ETH (focus on alts),
and stores in SQLite for offline use.
"""
from __future__ import annotations

import logging
import time
from typing import List

import aiohttp

from data.db import Database

logger = logging.getLogger(__name__)

# Stablecoins to exclude — keep ETH and BTC since DEX pools are ETH-based
_EXCLUDE_BASE = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP"}


class PairRegistry:
    def __init__(self, db: Database, top_n: int = 20):
        self._db = db
        self._top_n = top_n
        self._pairs: List[str] = []

    async def refresh(self) -> List[str]:
        """
        Fetch current top-N USDT alt pairs from Binance by 24h quote volume.
        Falls back to cached list in SQLite if the API call fails.
        """
        try:
            pairs = await self._fetch_from_binance()
            await self._cache(pairs)
            self._pairs = pairs
            logger.info("PairRegistry: loaded %d pairs from Binance", len(pairs))
        except Exception as exc:
            logger.warning("PairRegistry: Binance fetch failed (%s), using cache", exc)
            self._pairs = await self._load_from_cache()
            logger.info("PairRegistry: loaded %d pairs from cache", len(self._pairs))
        return self._pairs

    async def _fetch_from_binance(self) -> List[str]:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                tickers = await resp.json()

        # Filter: USDT-quoted, exclude majors/stables, active trading
        candidates = []
        for t in tickers:
            symbol: str = t["symbol"]
            if not symbol.endswith("USDT"):
                continue
            base = symbol[:-4]  # strip USDT suffix
            if base in _EXCLUDE_BASE:
                continue
            try:
                volume = float(t["quoteVolume"])
                candidates.append((symbol, base, volume))
            except (ValueError, KeyError):
                continue

        # Sort by 24h quote volume descending, take top N
        candidates.sort(key=lambda x: x[2], reverse=True)
        top = candidates[: self._top_n]
        return [f"{base}/USDT" for _, base, _ in top]

    async def _cache(self, pairs: List[str]) -> None:
        now_ns = time.time_ns()
        rows = []
        for rank, pair in enumerate(pairs):
            base, quote = pair.split("/")
            rows.append((pair.replace("/", ""), base, quote, rank + 1, now_ns))
        await self._db.executemany(
            """
            INSERT OR REPLACE INTO pairs_cache
                (symbol, base_asset, quote_asset, rank, updated_at_ns)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self._db.commit()

    async def _load_from_cache(self) -> List[str]:
        rows = await self._db.fetchall(
            "SELECT base_asset, quote_asset FROM pairs_cache ORDER BY rank ASC LIMIT ?",
            (self._top_n,),
        )
        return [f"{r['base_asset']}/{r['quote_asset']}" for r in rows]

    @property
    def pairs(self) -> List[str]:
        """Currently tracked pairs."""
        return self._pairs

    def binance_symbols(self) -> List[str]:
        """Return Binance-formatted symbols (e.g. SOLUSDT) for WebSocket subscription."""
        return [p.replace("/", "").lower() for p in self._pairs]
