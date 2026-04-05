"""
Binance WebSocket feed — subscribes to bookTicker streams for all tracked pairs.

Uses the combined stream endpoint so all pairs share a single WebSocket connection,
reducing file descriptor overhead and connection latency.

Stream format: wss://stream.binance.com:9443/stream?streams=solusdt@bookTicker/...
"""
from __future__ import annotations

import json
import logging
import time
from typing import Dict, List

from core.events import PriceEventBus
from core.models import PriceEvent
from feeds.base_feed import BaseFeed

logger = logging.getLogger(__name__)

# binance.vision is Binance's CDN-backed market data endpoint — works from cloud IPs
# stream.binance.com is often blocked on AWS/GCP/Render
_BASE_URL = "wss://data-stream.binance.vision/stream"


class BinanceFeed(BaseFeed):
    """
    Subscribes to Binance bookTicker streams for a list of pairs.
    Pushes PriceEvent(source='binance', ...) to the event bus.

    bookTicker payload:
        {
          "stream": "solusdt@bookTicker",
          "data": {
            "u": 123,        # order book update id
            "s": "SOLUSDT",  # symbol
            "b": "23.12",    # best bid price
            "B": "10.521",   # best bid qty
            "a": "23.13",    # best ask price
            "A": "8.342"     # best ask qty
          }
        }
    """

    def __init__(self, bus: PriceEventBus, symbols: List[str]):
        """
        Args:
            bus: The shared event bus.
            symbols: Binance symbols in lowercase (e.g. ['solusdt', 'bnbusdt']).
        """
        super().__init__(bus, name="binance")
        self._symbols = symbols
        # Precompute symbol → normalized pair map: "SOLUSDT" -> "SOL/USDT"
        self._sym_to_pair: Dict[str, str] = {
            sym.upper(): self._normalize(sym) for sym in symbols
        }

    @staticmethod
    def _normalize(symbol: str) -> str:
        """Convert 'solusdt' → 'SOL/USDT'."""
        sym = symbol.upper()
        if sym.endswith("USDT"):
            return sym[:-4] + "/USDT"
        if sym.endswith("BTC"):
            return sym[:-3] + "/BTC"
        if sym.endswith("ETH"):
            return sym[:-3] + "/ETH"
        return sym  # fallback

    def _get_url(self) -> str:
        # Binance allows up to 1024 streams per combined connection
        stream_names = "/".join(f"{sym}@bookTicker" for sym in self._symbols)
        return f"{_BASE_URL}?streams={stream_names}"

    async def _on_connect(self, ws) -> None:
        # Combined stream: subscriptions are encoded in the URL; no additional message needed
        logger.info("[binance] Subscribed to %d bookTicker streams", len(self._symbols))

    async def _on_message(self, message) -> None:
        recv_ns = time.time_ns()
        data = json.loads(message)

        # Combined stream wraps payload in {"stream": "...", "data": {...}}
        payload = data.get("data", data)

        symbol = payload.get("s", "")
        bid_str = payload.get("b", "")
        ask_str = payload.get("a", "")

        if not (symbol and bid_str and ask_str):
            return

        try:
            bid = float(bid_str)
            ask = float(ask_str)
        except ValueError:
            return

        if bid <= 0 or ask <= 0 or ask < bid:
            return

        pair = self._sym_to_pair.get(symbol)
        if pair is None:
            return

        event = PriceEvent(
            source="binance",
            pair=pair,
            bid=bid,
            ask=ask,
            timestamp_ns=recv_ns,
        )
        self._bus.put_nowait(event)
        self._record_latency(recv_ns)

    def update_symbols(self, symbols: List[str]) -> None:
        """
        Update the tracked symbol list. The feed must be restarted for
        URL-encoded subscriptions to take effect.
        """
        self._symbols = symbols
        self._sym_to_pair = {
            sym.upper(): self._normalize(sym) for sym in symbols
        }
