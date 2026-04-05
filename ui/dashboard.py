"""
Web dashboard server — aiohttp HTTP + WebSocket.
Serves a live dashboard at http://host:PORT/
Pushes full state JSON every second to all WebSocket clients.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Deque, Dict, List, Optional, Set

from aiohttp import web

if TYPE_CHECKING:
    from execution.paper_trader import PaperTrader
    from feeds.base_feed import BaseFeed
    from gas.arb_gas import ArbGasTracker
    from gas.eth_gas import EthGasTracker
    from gas.solana_priority_fee import SolanaPriorityFeeTracker
    from scanner.adaptive_tuner import AdaptiveTuner
    from scanner.opportunity_scanner import OpportunityScanner

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


class DashboardServer:
    """
    Runs the web dashboard.
    Call await server.run() as an asyncio task.
    Call server.on_opportunity(opp) and server.on_trade(record) from the bot loop.
    """

    MAX_OPPORTUNITIES = 50
    MAX_TRADES = 30
    MAX_PNL_SERIES = 7200   # 2 hours at 1 point/second

    def __init__(
        self,
        scanner: "OpportunityScanner",
        paper_trader: "PaperTrader",
        feeds: List["BaseFeed"],
        gas_trackers: Dict,  # {"eth": EthGasTracker, "arb": ..., "sol": ...}
        tuner: Optional["AdaptiveTuner"],
        host: str = "0.0.0.0",
        port: int = 8080,
    ):
        self._scanner = scanner
        self._trader = paper_trader
        self._feeds = feeds
        self._gas = gas_trackers
        self._tuner = tuner
        self._host = host
        self._port = port
        self._start_time = time.time()

        # Bounded in-memory buffers
        self._recent_opps: Deque[dict] = deque(maxlen=self.MAX_OPPORTUNITIES)
        self._recent_trades: Deque[dict] = deque(maxlen=self.MAX_TRADES)
        self._pnl_series: Deque[dict] = deque(maxlen=self.MAX_PNL_SERIES)

        # WebSocket client tracking
        self._ws_clients: Set[web.WebSocketResponse] = set()

        # Track last pnl for series delta
        self._last_pnl = 0.0
        self._last_pnl_ts = time.time()

    # ------------------------------------------------------------------
    # Callbacks called from bot loop
    # ------------------------------------------------------------------

    def on_opportunity(self, opp) -> None:
        from core.models import Direction
        self._recent_opps.appendleft({
            "ts": opp.detected_at_ns / 1e9,
            "pair": opp.pair,
            "direction": "CEX→DEX" if opp.direction == Direction.BUY_CEX_SELL_DEX else "DEX→CEX",
            "chain": opp.chain.value,
            "raw_spread_pct": round(opp.raw_spread_pct * 100, 4),
            "net_profit_pct": round(opp.net_profit_pct * 100, 4),
            "notional_usd": opp.notional_usd,
            "viable": opp.is_viable,
        })

    def on_trade(self, record) -> None:
        self._recent_trades.appendleft({
            "id": record.id,
            "pair": record.pair,
            "direction": record.direction,
            "chain": record.chain,
            "entry_time_ns": record.entry_time_ns,
            "net_pnl_usd": round(record.net_pnl_usd, 4),
            "net_pnl_pct": round(record.net_pnl_pct * 100, 4),
            "gross_pnl_usd": round(record.gross_pnl_usd, 4),
            "fees_usd": round(record.fees_usd, 4),
            "gas_usd": round(record.gas_usd, 4),
            "notional_usd": record.notional_usd,
            "raw_spread_at_entry": round(record.raw_spread_at_entry * 100, 4),
        })
        # Append to P&L series
        pnl = self._trader.stats["total_pnl_usd"]
        self._pnl_series.append({"ts": time.time(), "cumulative_pnl": round(pnl, 4)})

    # ------------------------------------------------------------------
    # State snapshot
    # ------------------------------------------------------------------

    def _build_state(self) -> dict:
        trader_stats = self._trader.stats
        scanner_stats = self._scanner.stats

        gas_state = {}
        for chain_key, tracker in self._gas.items():
            if tracker:
                s = tracker.state
                gas_state[chain_key] = {
                    "gwei": round(s.gas_gwei, 3),
                    "usd_per_swap": round(s.gas_usd_per_swap, 4),
                    "block": s.block_number,
                    "stale": s.is_stale(),
                }

        feeds_data = []
        for feed in self._feeds:
            s = feed.stats
            feeds_data.append({
                "name": s["name"],
                "connected": s["connected"],
                "messages": s["messages"],
                "reconnects": s.get("reconnects", 0),
                "p50_us": round(s.get("p50_us", 0), 1),
            })

        learning_data = []
        if self._tuner:
            for state in self._tuner.get_all_states():
                learning_data.append(state.to_dict())

        return {
            "ts": time.time(),
            "uptime_s": int(time.time() - self._start_time),
            "bot_mode": "paper",
            "scanner": {
                "events_processed": scanner_stats["events_processed"],
                "opportunities_found": scanner_stats["opportunities_found"],
                "opportunities_viable": scanner_stats["opportunities_viable"],
                "viable_rate": round(scanner_stats["viable_rate"] * 100, 3),
                "registry_size": scanner_stats["registry_size"],
            },
            "trader": {
                "total_trades": trader_stats["total_trades"],
                "winning_trades": trader_stats["winning_trades"],
                "win_rate": round(trader_stats["win_rate"] * 100, 1),
                "total_pnl_usd": round(trader_stats["total_pnl_usd"], 4),
                "total_invested_usd": round(trader_stats["total_invested_usd"], 2),
                "roi_pct": round(trader_stats["roi_pct"], 4),
            },
            "feeds": feeds_data,
            "gas": gas_state,
            "recent_opportunities": list(self._recent_opps),
            "recent_trades": list(self._recent_trades),
            "learning": learning_data,
            "pnl_series": list(self._pnl_series),
        }

    # ------------------------------------------------------------------
    # aiohttp routes
    # ------------------------------------------------------------------

    async def _handle_index(self, request: web.Request) -> web.Response:
        index_path = STATIC_DIR / "index.html"
        return web.FileResponse(index_path)

    async def _handle_api_state(self, request: web.Request) -> web.Response:
        """JSON state endpoint — used as Render health check too."""
        state = self._build_state()
        return web.json_response(state)

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        self._ws_clients.add(ws)
        logger.info("[dashboard] WebSocket client connected (%d total)", len(self._ws_clients))
        try:
            # Send initial state immediately
            await ws.send_str(json.dumps(self._build_state()))
            async for msg in ws:
                pass  # we only push; ignore any client messages
        except Exception:
            pass
        finally:
            self._ws_clients.discard(ws)
            logger.info("[dashboard] WebSocket client disconnected (%d remaining)", len(self._ws_clients))
        return ws

    # ------------------------------------------------------------------
    # Broadcast loop
    # ------------------------------------------------------------------

    async def _broadcast_loop(self) -> None:
        """Push state to all WebSocket clients every second."""
        while True:
            await asyncio.sleep(1.0)
            if not self._ws_clients:
                continue
            state_json = json.dumps(self._build_state())
            dead = set()
            for ws in list(self._ws_clients):
                try:
                    await ws.send_str(state_json)
                except Exception:
                    dead.add(ws)
            self._ws_clients -= dead

    async def _keepalive_loop(self) -> None:
        """Self-ping every 4 minutes to prevent Render free tier from sleeping."""
        import os
        import aiohttp
        await asyncio.sleep(60)  # wait for server to fully start
        url = f"http://localhost:{self._port}/api/state"
        # On Render, prefer the public URL so the external health check also fires
        render_url = os.environ.get("RENDER_EXTERNAL_URL")
        if render_url:
            url = f"{render_url}/api/state"
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        logger.debug("[dashboard] keepalive ping %d", r.status)
            except Exception as exc:
                logger.debug("[dashboard] keepalive error: %s", exc)
            await asyncio.sleep(240)  # every 4 minutes

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    async def run(self) -> None:
        app = web.Application()
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/api/state", self._handle_api_state)
        app.router.add_get("/ws", self._handle_ws)
        app.router.add_static("/static", STATIC_DIR, show_index=False)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self._host, self._port)
        await site.start()
        logger.info("[dashboard] Serving at http://%s:%d", self._host, self._port)

        await asyncio.gather(
            self._broadcast_loop(),
            self._keepalive_loop(),
        )
