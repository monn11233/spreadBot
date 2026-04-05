"""
Bybit spot price feed — REST polling via aiohttp.

Bybit's public API has no cloud IP restrictions (unlike Binance which blocks AWS/Render).
Polls /v5/market/tickers every 500ms for all tracked pairs.
No API key required for market data.

Prices are published as "binance" source so the rest of the system needs no changes.
Bybit and Binance spot prices for major pairs typically differ by < 0.05%.
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

# Bybit V5 linear/spot tickers — no auth, no IP restrictions
_REST_URL = "https://api.bybit.com/v5/market/tickers"
_POLL_INTERVAL = 0.5   # seconds


class BinanceFeed:
    """
    Polls Bybit spot tickers every 500ms and pushes them as 'binance' source events.
    Bybit is used because it works from any cloud IP; Binance blocks AWS/Render.
    Prices are functionally equivalent for CEX-DEX spread detection.
    """

    def __init__(self, bus: PriceEventBus, symbols: List[str]):
        self._bus = bus
        # Convert binance-style symbols to Bybit format (same: ETHUSDT, SOLUSDT, etc.)
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
        logger.info("[binance] Starting Bybit REST poller for %d symbols", len(self._symbols))

        async with aiohttp.ClientSession() as session:
            self._connected = True
            self._reconnect_count = 1
            logger.info("[binance] Bybit REST poller connected")

            while self._running:
                t0 = time.monotonic()
                try:
                    # Bybit returns all spot tickers in one call — filter client-side
                    async with session.get(
                        _REST_URL,
                        params={"category": "spot"},
                        timeout=aiohttp.ClientTimeout(total=4),
                    ) as resp:
                        if resp.status == 200:
                            body = await resp.json()
                            recv_ns = time.time_ns()
                            items = body.get("result", {}).get("list", [])
                            for item in items:
                                sym = item.get("symbol", "")
                                if sym not in self._sym_to_pair:
                                    continue
                                try:
                                    bid = float(item["bid1Price"])
                                    ask = float(item["ask1Price"])
                                except (KeyError, ValueError):
                                    continue
                                if bid <= 0 or ask <= 0 or ask < bid:
                                    continue
                                self._bus.put_nowait(PriceEvent(
                                    source="binance",
                                    pair=self._sym_to_pair[sym],
                                    bid=bid,
                                    ask=ask,
                                    timestamp_ns=recv_ns,
                                ))
                                self._msg_count += 1
                        else:
                            logger.warning("[binance] Bybit status %d", resp.status)
                            await asyncio.sleep(5)
                            continue

                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("[binance] Poll error: %s", exc)
                    self._connected = False
                    await asyncio.sleep(5)
                    self._connected = True
                    continue

                elapsed = time.monotonic() - t0
                await asyncio.sleep(max(0.0, _POLL_INTERVAL - elapsed))

        self._connected = False
        logger.info("[binance] Bybit REST poller stopped")

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
