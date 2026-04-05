"""
Jupiter (Solana) price feed via Helius WebSocket.

Jupiter aggregates all Solana DEX liquidity. We use the Jupiter Price API v2
for initial prices and Helius `transactionSubscribe` or account subscriptions
for real-time updates.

Fallback: poll Jupiter's HTTP price API on a short interval via aiohttp
(1-2 second latency, acceptable for Solana given ~400ms block times).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, List

import aiohttp

from core.events import PriceEventBus
from core.models import Chain, PriceEvent
from feeds.base_feed import BaseFeed

logger = logging.getLogger(__name__)

JUPITER_PRICE_API = "https://price.jup.ag/v6/price"

# Solana token mint addresses for common alts (needed for Jupiter price API)
# Maps normalized pair → mint address of the base token
SOL_TOKEN_MINTS: Dict[str, str] = {
    "SOL/USDT":  "So11111111111111111111111111111111111111112",
    "JUP/USDT":  "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "BONK/USDT": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF/USDT":  "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "PYTH/USDT": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
}

# USDC mint (Jupiter quotes against USDC; we convert to USDT equiv 1:1)
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class JupiterFeed(BaseFeed):
    """
    Jupiter Solana price feed.

    Uses a hybrid approach:
    1. If a Helius WebSocket URL is provided: subscribe to program logs for
       price-impacting accounts (real-time, ~100ms latency).
    2. Fallback: poll Jupiter HTTP Price API every 2 seconds.

    The poll fallback is reliable and sufficient for most arb opportunities
    given Solana's 400ms block time.
    """

    def __init__(
        self,
        bus: PriceEventBus,
        pairs: List[str],
        ws_url: str = "",
        poll_interval: float = 2.0,
    ):
        super().__init__(bus, name="jupiter_sol")
        self._pairs = pairs
        self._ws_url = ws_url
        self._poll_interval = poll_interval
        self._use_ws = bool(ws_url)
        self._mints = {pair: SOL_TOKEN_MINTS[pair] for pair in pairs if pair in SOL_TOKEN_MINTS}

    def _get_url(self) -> str:
        return self._ws_url or "wss://api.mainnet-beta.solana.com"

    async def _on_connect(self, ws) -> None:
        # Subscribe to slot updates for timing (lightweight)
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "slotSubscribe",
        }))
        logger.info("[jupiter_sol] Helius WebSocket connected — polling Jupiter API alongside")

    async def _on_message(self, message) -> None:
        # Slot updates are just timing signals — trigger a price fetch
        await self._fetch_and_push()

    # ------------------------------------------------------------------
    # Override start() for poll-based mode
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._use_ws:
            await super().start()
        else:
            await self._poll_loop()

    async def _poll_loop(self) -> None:
        """Poll Jupiter HTTP API at a fixed interval."""
        self._running = True
        logger.info("[jupiter_sol] Starting poll mode (interval=%.1fs)", self._poll_interval)
        while self._running:
            try:
                await self._fetch_and_push()
            except Exception as exc:
                logger.debug("[jupiter_sol] Poll error: %s", exc)
            await asyncio.sleep(self._poll_interval)

    async def _fetch_and_push(self) -> None:
        """Fetch prices for all tracked pairs from Jupiter API and push to bus."""
        if not self._mints:
            return

        ids = ",".join(self._mints.values())
        url = f"{JUPITER_PRICE_API}?ids={ids}&vsToken={USDC_MINT}"

        fetch_ns = time.time_ns()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=3)
                ) as resp:
                    if resp.status != 200:
                        return
                    payload = await resp.json()
        except Exception:
            return

        data = payload.get("data", {})
        recv_ns = time.time_ns()

        for pair, mint in self._mints.items():
            entry = data.get(mint)
            if not entry:
                continue
            try:
                price = float(entry["price"])
            except (KeyError, ValueError):
                continue

            if price <= 0:
                continue

            # Jupiter gives mid price; use 0.05% as synthetic spread
            half_spread = price * 0.0005
            event = PriceEvent(
                source="jupiter_sol",
                pair=pair,
                bid=price - half_spread,
                ask=price + half_spread,
                timestamp_ns=recv_ns,
                chain=Chain.SOLANA,
                pool_address=mint,
            )
            self._bus.put_nowait(event)

        self._msg_count += 1
        self._record_latency(fetch_ns)
