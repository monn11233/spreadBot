"""
CEX price feed — Kraken REST polling (primary).

Uses https://api.kraken.com/0/public/Ticker — pure HTTPS, works from any
cloud provider with zero geo-restrictions.
Polls every 500ms for all supported pairs.

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
_POLL_INTERVAL = 0.5  # seconds

# Kraken returns non-standard keys for some symbols — map back to standard
_KRAKEN_KEY_MAP = {
    "XDGUSDT": "DOGEUSDT",
    "XBTUSDT": "BTCUSDT",
}

# Pairs Kraken supports in USDT — extend as needed
_KRAKEN_SUPPORTED = {
    "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "ADAUSDT",
    "DOGEUSDT", "BTCUSDT", "DOTUSDT", "LINKUSDT", "AVAXUSDT",
    "TRXUSDT", "XAUTUSDT",
}


def _normalize(symbol: str) -> Optional[str]:
    """'ETHUSDT' → 'ETH/USDT'"""
    sym = symbol.upper()
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if sym.endswith(quote):
            return sym[: -len(quote)] + "/" + quote
    return None


class BinanceFeed:
    """
    Polls Kraken REST /Ticker every 500ms and pushes PriceEvents as source='binance'.
    Kraken REST is HTTPS-only, works from Render/AWS/GCP without restrictions.
    """

    def __init__(self, bus: PriceEventBus, symbols: List[str]):
        self._bus = bus
        self._running = False
        self._connected = False
        self._msg_count = 0
        self._reconnect_count = 0
        self.name = "binance"

        # Always include ETH — our DEX pools are all ETH-based
        all_symbols = list(symbols)
        if "ethusdt" not in [s.lower() for s in all_symbols]:
            all_symbols.insert(0, "ethusdt")

        # Only keep symbols Kraken supports
        supported = []
        self._sym_to_pair: Dict[str, str] = {}  # "ETHUSDT" → "ETH/USDT"
        for sym in all_symbols:
            sym_up = sym.upper()
            if sym_up in _KRAKEN_SUPPORTED:
                pair = _normalize(sym_up)
                if pair:
                    supported.append(sym_up)
                    self._sym_to_pair[sym_up] = pair

        # Kraken pair format for REST: comma-separated e.g. "ETHUSDT,SOLUSDT"
        self._kraken_query = ",".join(supported)
        logger.info("[binance] Kraken REST: tracking %d pairs: %s",
                    len(supported), supported)

    async def start(self) -> None:
        self._running = True
        if not self._kraken_query:
            logger.error("[binance] No supported Kraken pairs — feed inactive")
            return

        logger.info("[binance] Starting Kraken REST poller")
        async with aiohttp.ClientSession() as session:
            self._connected = True
            self._reconnect_count = 1
            logger.info("[binance] Kraken REST poller connected")

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
                        body = await resp.json()
                        errors = body.get("error", [])
                        if errors:
                            logger.warning("[binance] Kraken API error: %s", errors)
                            await asyncio.sleep(5)
                            continue
                        recv_ns = time.time_ns()
                        for kraken_sym, data in body.get("result", {}).items():
                            # Kraken uses non-standard keys for some pairs:
                            # DOGE → XDGUSDT, normalize back to DOGEUSDT
                            clean = _KRAKEN_KEY_MAP.get(kraken_sym, kraken_sym)
                            pair = self._sym_to_pair.get(clean) or self._sym_to_pair.get(kraken_sym)
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
