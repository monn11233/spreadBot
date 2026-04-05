"""
Binance price feed — REST polling via aiohttp.

Uses /api/v3/ticker/bookTicker to get best bid/ask for all tracked pairs.
Polls every 500ms — works from any cloud IP (no WebSocket geo-restrictions).
Rate limit: 2 weight per call, weight budget is 6000/min → safe at 500ms interval.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List

import aiohttp

from core.events import PriceEventBus
from core.models import PriceEvent

logger = logging.getLogger(__name__)

_REST_URL = "https://data-api.binance.vision/api/v3/ticker/bookTicker"
_POLL_INTERVAL = 0.5   # seconds


class BinanceFeed:
    """
    Polls Binance REST bookTicker every 500ms.
    Compatible with all cloud providers — no WebSocket geo-restrictions.
    """

    def __init__(self, bus: PriceEventBus, symbols: List[str]):
        self._bus = bus
        self._symbols = [s.upper() for s in symbols]
        self._sym_to_pair: Dict[str, str] = {
            s: self._normalize(s) for s in self._symbols
        }
        self._running = False
        self._connected = False
        self._msg_count = 0
        self._reconnect_count = 0
        self.name = "binance"

    @staticmethod
    def _normalize(symbol: str) -> str:
        if symbol.endswith("USDT"):
            return symbol[:-4] + "/USDT"
        if symbol.endswith("BTC"):
            return symbol[:-3] + "/BTC"
        if symbol.endswith("ETH"):
            return symbol[:-3] + "/ETH"
        return symbol

    async def start(self) -> None:
        self._running = True
        logger.info("[binance] Starting REST poller for %d symbols", len(self._symbols))

        # Build query string: ?symbols=["ETHUSDT","SOLUSDT",...]
        sym_json = "[" + ",".join(f'"{s}"' for s in self._symbols) + "]"
        params = {"symbols": sym_json}

        async with aiohttp.ClientSession() as session:
            self._connected = True
            self._reconnect_count = 1
            logger.info("[binance] REST poller connected")

            while self._running:
                t0 = time.monotonic()
                try:
                    async with session.get(_REST_URL, params=params, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            recv_ns = time.time_ns()
                            for item in data:
                                sym = item.get("symbol", "")
                                pair = self._sym_to_pair.get(sym)
                                if not pair:
                                    continue
                                try:
                                    bid = float(item["bidPrice"])
                                    ask = float(item["askPrice"])
                                except (KeyError, ValueError):
                                    continue
                                if bid <= 0 or ask <= 0 or ask < bid:
                                    continue
                                self._bus.put_nowait(PriceEvent(
                                    source="binance",
                                    pair=pair,
                                    bid=bid,
                                    ask=ask,
                                    timestamp_ns=recv_ns,
                                ))
                                self._msg_count += 1
                        else:
                            logger.warning("[binance] REST status %d", resp.status)
                            self._connected = False
                            await asyncio.sleep(5)
                            self._connected = True
                            continue

                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("[binance] Poll error: %s", exc)
                    self._connected = False
                    await asyncio.sleep(5)
                    self._connected = True
                    continue

                # Sleep for remainder of interval
                elapsed = time.monotonic() - t0
                sleep_for = max(0.0, _POLL_INTERVAL - elapsed)
                await asyncio.sleep(sleep_for)

        self._connected = False
        logger.info("[binance] REST poller stopped")

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
