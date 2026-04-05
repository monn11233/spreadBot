"""
CEX spot price feed — tries multiple exchanges until one works.

Priority order (all cloud-IP friendly):
  1. OKX     — wss://ws.okx.com:8443/ws/v5/public  (best cloud support)
  2. Bybit   — https://api.bybit.com/v5/market/tickers
  3. Kraken  — https://api.kraken.com/0/public/Ticker

Prices published as 'binance' source so the rest of the system needs no changes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, List

import aiohttp
import websockets

from core.events import PriceEventBus
from core.models import PriceEvent

logger = logging.getLogger(__name__)


def _normalize(symbol: str) -> str:
    sym = symbol.upper().replace("-", "").replace("_", "")
    if sym.endswith("USDT"):
        return sym[:-4] + "/USDT"
    if sym.endswith("USDC"):
        return sym[:-4] + "/USDC"
    if sym.endswith("BTC"):
        return sym[:-3] + "/BTC"
    return sym


class BinanceFeed:
    """
    Fetches CEX best bid/ask from OKX WebSocket (primary) with Bybit REST fallback.
    Emits PriceEvents with source='binance' — no changes needed downstream.
    """

    def __init__(self, bus: PriceEventBus, symbols: List[str]):
        self._bus = bus
        self._symbols_upper = [s.upper() for s in symbols]
        # Build lookup: OKX uses "ETH-USDT", Bybit uses "ETHUSDT"
        self._okx_to_pair: Dict[str, str] = {}
        self._bybit_to_pair: Dict[str, str] = {}
        for sym in self._symbols_upper:
            pair = _normalize(sym)
            # OKX: insert dash before quote currency
            for quote in ("USDT", "USDC", "BTC", "ETH"):
                if sym.endswith(quote):
                    base = sym[: -len(quote)]
                    self._okx_to_pair[f"{base}-{quote}"] = pair
                    break
            self._bybit_to_pair[sym] = pair

        self._running = False
        self._connected = False
        self._msg_count = 0
        self._reconnect_count = 0
        self.name = "binance"

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                logger.info("[binance] Trying OKX WebSocket...")
                await self._run_okx()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[binance] OKX failed (%s), falling back to Bybit REST", exc)
            if not self._running:
                break
            try:
                await self._run_bybit()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[binance] Bybit failed (%s), retrying in 10s", exc)
                self._connected = False
                await asyncio.sleep(10)

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # OKX WebSocket — primary
    # ------------------------------------------------------------------

    async def _run_okx(self) -> None:
        url = "wss://ws.okx.com:8443/ws/v5/public"
        # Subscribe to tickers for all pairs
        args = [{"channel": "tickers", "instId": inst} for inst in self._okx_to_pair]
        sub_msg = json.dumps({"op": "subscribe", "args": args})

        async with websockets.connect(url, open_timeout=10, ping_interval=20, compression=None) as ws:
            await ws.send(sub_msg)
            self._connected = True
            self._reconnect_count += 1
            logger.info("[binance] OKX WebSocket connected (reconnect #%d)", self._reconnect_count)

            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                data = msg.get("data")
                if not data:
                    continue
                recv_ns = time.time_ns()
                for item in data:
                    inst_id = item.get("instId", "")
                    pair = self._okx_to_pair.get(inst_id)
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
                        source="binance",
                        pair=pair,
                        bid=bid,
                        ask=ask,
                        timestamp_ns=recv_ns,
                    ))
                    self._msg_count += 1

        self._connected = False

    # ------------------------------------------------------------------
    # Bybit REST — fallback
    # ------------------------------------------------------------------

    async def _run_bybit(self) -> None:
        url = "https://api.bybit.com/v5/market/tickers"
        async with aiohttp.ClientSession() as session:
            self._connected = True
            self._reconnect_count += 1
            logger.info("[binance] Bybit REST fallback connected (reconnect #%d)", self._reconnect_count)
            while self._running:
                t0 = time.monotonic()
                try:
                    async with session.get(url, params={"category": "spot"},
                                           timeout=aiohttp.ClientTimeout(total=4)) as resp:
                        if resp.status != 200:
                            logger.warning("[binance] Bybit HTTP %d", resp.status)
                            await asyncio.sleep(5)
                            continue
                        body = await resp.json()
                        recv_ns = time.time_ns()
                        items = body.get("result", {}).get("list", [])
                        logger.debug("[binance] Bybit returned %d tickers", len(items))
                        for item in items:
                            sym = item.get("symbol", "")
                            pair = self._bybit_to_pair.get(sym)
                            if not pair:
                                continue
                            try:
                                bid = float(item["bid1Price"])
                                ask = float(item["ask1Price"])
                            except (KeyError, ValueError):
                                continue
                            if bid <= 0 or ask <= 0 or ask < bid:
                                continue
                            self._bus.put_nowait(PriceEvent(
                                source="binance", pair=pair,
                                bid=bid, ask=ask, timestamp_ns=recv_ns,
                            ))
                            self._msg_count += 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("[binance] Bybit poll error: %s", exc)
                    self._connected = False
                    await asyncio.sleep(5)
                    self._connected = True
                    continue
                await asyncio.sleep(max(0.0, 0.5 - (time.monotonic() - t0)))
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
