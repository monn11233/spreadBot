"""
CEX price feed — Kraken REST polling (primary).

Uses https://api.kraken.com/0/public/Ticker — pure HTTPS, works from any
cloud provider with zero geo-restrictions.
Polls every 500ms for all supported pairs.

At startup, queries Kraken's AssetPairs endpoint to dynamically discover
which of our tracked symbols they support, instead of a hardcoded list.

Prices emitted as source='binance' so no downstream changes are needed.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional

import aiohttp

from core.events import PriceEventBus
from core.models import PriceEvent

logger = logging.getLogger(__name__)

_KRAKEN_REST = "https://api.kraken.com/0/public/Ticker"
_KRAKEN_ASSET_PAIRS = "https://api.kraken.com/0/public/AssetPairs"
_POLL_INTERVAL = 0.5  # seconds

# Kraken uses non-standard internal names for some assets — map back to canonical
_KRAKEN_KEY_MAP = {
    "XDGUSDT": "DOGEUSDT",
    "XBTUSDT": "BTCUSDT",
    "XETHUSDT": "ETHUSDT",
    "XXRPUSDT": "XRPUSDT",
    "XXLMUSDT": "XLMUSDT",
}


def _normalize(symbol: str) -> Optional[str]:
    """'ETHUSDT' → 'ETH/USDT'"""
    sym = symbol.upper()
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if sym.endswith(quote):
            return sym[: -len(quote)] + "/" + quote
    return None


async def _discover_kraken_pairs(
    session: aiohttp.ClientSession,
    candidates: List[str],
) -> Dict[str, str]:
    """
    Query Kraken's AssetPairs endpoint to find which of our candidate symbols
    they support. Returns {kraken_altname: normalized_pair} e.g. {"XDGUSDT": "DOGE/USDT"}.

    Falls back to an empty dict on error (caller should use static fallback).
    """
    try:
        async with session.get(
            _KRAKEN_ASSET_PAIRS,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            data = await resp.json(content_type=None)
    except Exception as exc:
        logger.warning("[binance] AssetPairs discovery failed: %s", exc)
        return {}

    # Build mapping: normalized_symbol → kraken_altname for all USDT pairs
    # altname is the "user-friendly" Kraken name (e.g. "SOLUSDT", "XDGUSDT")
    kraken_alt_to_canonical: Dict[str, str] = {}
    for info in data.get("result", {}).values():
        alt = info.get("altname", "")
        if not alt.endswith("USDT"):
            continue
        # Map Kraken altname to canonical symbol (e.g. XDGUSDT → DOGEUSDT)
        canonical = _KRAKEN_KEY_MAP.get(alt, alt)
        kraken_alt_to_canonical[alt] = canonical

    # Intersect with our candidate symbols
    candidates_up = {s.upper() for s in candidates}
    result: Dict[str, str] = {}
    for alt, canonical in kraken_alt_to_canonical.items():
        if canonical in candidates_up:
            pair = _normalize(canonical)
            if pair:
                result[alt] = pair  # {"SOLUSDT": "SOL/USDT", "XDGUSDT": "DOGE/USDT", ...}

    return result


class BinanceFeed:
    """
    Polls Kraken REST /Ticker every 500ms and pushes PriceEvents as source='binance'.
    Discovers supported pairs dynamically at startup via /AssetPairs.
    """

    def __init__(self, bus: PriceEventBus, symbols: List[str]):
        self._bus = bus
        self._running = False
        self._connected = False
        self._msg_count = 0
        self._reconnect_count = 0
        self.name = "binance"

        # Always include ETH — our DEX pools are ETH-based
        all_symbols = list(symbols)
        if "ethusdt" not in [s.lower() for s in all_symbols]:
            all_symbols.insert(0, "ETHUSDT")

        self._candidates = [s.upper() for s in all_symbols]
        # _sym_to_pair is populated at start() time after discovery
        self._sym_to_pair: Dict[str, str] = {}
        self._kraken_query: str = ""

    async def start(self) -> None:
        self._running = True

        async with aiohttp.ClientSession() as session:
            # Discover which of our pairs Kraken supports
            discovered = await _discover_kraken_pairs(session, self._candidates)

            if discovered:
                self._sym_to_pair = discovered
                self._kraken_query = ",".join(discovered.keys())
                logger.info(
                    "[binance] Kraken supports %d/%d tracked pairs: %s",
                    len(discovered), len(self._candidates),
                    sorted(discovered.values()),
                )
            else:
                logger.error("[binance] Kraken pair discovery returned nothing — feed inactive")
                return

            self._connected = True
            self._reconnect_count = 1

            while self._running:
                t0 = time.monotonic()
                try:
                    async with session.get(
                        _KRAKEN_REST,
                        params={"pair": self._kraken_query},
                        timeout=aiohttp.ClientTimeout(total=4),
                    ) as resp:
                        if resp.status != 200:
                            logger.warning("[binance] Kraken HTTP %d", resp.status)
                            await asyncio.sleep(5)
                            continue
                        body = await resp.json(content_type=None)
                        errors = body.get("error", [])
                        if errors:
                            logger.warning("[binance] Kraken API error: %s", errors)
                            await asyncio.sleep(5)
                            continue
                        recv_ns = time.time_ns()
                        for kraken_sym, data in body.get("result", {}).items():
                            pair = self._sym_to_pair.get(kraken_sym)
                            if not pair:
                                continue
                            try:
                                bid = float(data["b"][0])
                                ask = float(data["a"][0])
                            except (KeyError, IndexError, ValueError):
                                continue
                            if bid <= 0 or ask <= 0 or ask < bid:
                                continue
                            self._bus.put_nowait(PriceEvent(
                                source="binance", pair=pair,
                                bid=bid, ask=ask, timestamp_ns=recv_ns,
                            ))
                            self._msg_count += 1

                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("[binance] Kraken poll error: %s", exc)
                    self._connected = False
                    await asyncio.sleep(5)
                    self._connected = True
                    continue

                await asyncio.sleep(max(0.0, _POLL_INTERVAL - (time.monotonic() - t0)))

        self._connected = False
        logger.info("[binance] Kraken REST poller stopped")

    async def stop(self) -> None:
        self._running = False

    @property
    def stats(self) -> dict:
        return {
            "name": self.name,
            "connected": self._connected,
            "reconnects": self._reconnect_count,
            "messages": self._msg_count,
        }
