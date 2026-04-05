"""
CEX price feed — Kraken WebSocket v2 (primary) + OKX WebSocket (fallback).

Kraken has no cloud IP restrictions — works from Render/AWS/GCP.
Covers all DEX-matched pairs: ETH/USDT (Uniswap ETH+Arb), SOL/USDT (Jupiter),
plus XRP, BNB, ADA for additional spread scanning.

Prices emitted as source='binance' — no downstream changes needed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional

import websockets

from core.events import PriceEventBus
from core.models import PriceEvent

logger = logging.getLogger(__name__)

# Kraken WebSocket v2 — no geo-restrictions, port 443
_KRAKEN_WS = "wss://ws.kraken.com/v2"

# Kraken uses "ETH/USDT" format — map to our internal "ETH/USDT"
# Only include pairs Kraken actually supports
_KRAKEN_SUPPORTED = {
    "ETH/USDT", "SOL/USDT", "XRP/USDT", "BNB/USDT",
    "ADA/USDT", "DOGE/USDT", "BTC/USDT", "DOT/USDT",
    "LINK/USDT", "AVAX/USDT", "MATIC/USDT", "TRX/USDT",
    "XAUT/USDT",
}


def _binance_sym_to_slash(sym: str) -> Optional[str]:
    """'ETHUSDT' → 'ETH/USDT', returns None if quote not recognised."""
    sym = sym.upper()
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if sym.endswith(quote):
            return sym[: -len(quote)] + "/" + quote
    return None


class BinanceFeed:
    """
    Streams CEX best bid/ask from Kraken WebSocket v2.
    Falls back to OKX WebSocket if Kraken fails.
    Emits PriceEvents with source='binance'.
    """

    def __init__(self, bus: PriceEventBus, symbols: List[str]):
        self._bus = bus
        self._running = False
        self._connected = False
        self._msg_count = 0
        self._reconnect_count = 0
        self.name = "binance"

        # Convert Binance-style symbols to slash format
        slash_pairs = []
        for sym in symbols:
            sp = _binance_sym_to_slash(sym)
            if sp:
                slash_pairs.append(sp)

        # Only subscribe to pairs Kraken supports
        self._kraken_pairs = [p for p in slash_pairs if p in _KRAKEN_SUPPORTED]
        # All pairs for OKX fallback (ETH-USDT format)
        self._okx_inst: Dict[str, str] = {}
        for sp in slash_pairs:
            base, quote = sp.split("/")
            self._okx_inst[f"{base}-{quote}"] = sp

        # slash_pair → slash_pair (identity, used as lookup)
        self._pair_set = set(slash_pairs)

        logger.info(
            "[binance] Kraken pairs (%d): %s",
            len(self._kraken_pairs), self._kraken_pairs,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        while self._running:
            # Try Kraken first
            try:
                await self._run_kraken()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[binance] Kraken error: %s — trying OKX fallback", exc)
                self._connected = False

            if not self._running:
                break

            # OKX fallback — timeout 30s in case data doesn't flow
            try:
                await asyncio.wait_for(self._run_okx(), timeout=30)
            except asyncio.TimeoutError:
                logger.warning("[binance] OKX sent no data in 30s — retrying Kraken")
                self._connected = False
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[binance] OKX error: %s — retrying Kraken in 5s", exc)
                self._connected = False
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Kraken WebSocket v2
    # ------------------------------------------------------------------

    async def _run_kraken(self) -> None:
        if not self._kraken_pairs:
            raise RuntimeError("No Kraken-supported pairs to subscribe to")

        async with websockets.connect(_KRAKEN_WS, open_timeout=10,
                                      ping_interval=20, compression=None) as ws:
            sub = json.dumps({
                "method": "subscribe",
                "params": {"channel": "ticker", "symbol": self._kraken_pairs},
            })
            await ws.send(sub)
            self._connected = True
            self._reconnect_count += 1
            logger.info("[binance] Kraken connected (reconnect #%d), %d pairs",
                        self._reconnect_count, len(self._kraken_pairs))

            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get("channel") != "ticker":
                    continue
                recv_ns = time.time_ns()
                for item in msg.get("data", []):
                    pair = item.get("symbol", "")
                    if pair not in self._pair_set:
                        continue
                    try:
                        bid = float(item["bid"])
                        ask = float(item["ask"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    if bid <= 0 or ask <= 0 or ask < bid:
                        continue
                    self._bus.put_nowait(PriceEvent(
                        source="binance", pair=pair,
                        bid=bid, ask=ask, timestamp_ns=recv_ns,
                    ))
                    self._msg_count += 1

        self._connected = False

    # ------------------------------------------------------------------
    # OKX WebSocket fallback
    # ------------------------------------------------------------------

    async def _run_okx(self) -> None:
        url = "wss://ws.okx.com/ws/v5/public"
        args = [{"channel": "tickers", "instId": inst} for inst in self._okx_inst]
        sub = json.dumps({"op": "subscribe", "args": args})

        async with websockets.connect(url, open_timeout=10,
                                      ping_interval=20, compression=None) as ws:
            await ws.send(sub)
            self._connected = True
            self._reconnect_count += 1
            logger.info("[binance] OKX fallback connected (reconnect #%d)", self._reconnect_count)

            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                recv_ns = time.time_ns()
                for item in msg.get("data", []):
                    inst_id = item.get("instId", "")
                    pair = self._okx_inst.get(inst_id)
                    if not pair:
                        continue
                    try:
                        bid = float(item["bidPx"])
                        ask = float(item["askPx"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    if bid <= 0 or ask <= 0 or ask < bid:
                        continue
                    self._bus.put_nowait(PriceEvent(
                        source="binance", pair=pair,
                        bid=bid, ask=ask, timestamp_ns=recv_ns,
                    ))
                    self._msg_count += 1

        self._connected = False

    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        return {
            "name": self.name,
            "connected": self._connected,
            "reconnects": self._reconnect_count,
            "messages": self._msg_count,
        }
