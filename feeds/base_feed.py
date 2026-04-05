"""
Abstract base class for all price feeds.
Handles reconnection, heartbeat, exponential backoff, and latency tracking.
"""
from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from core.events import PriceEventBus

logger = logging.getLogger(__name__)


class ExponentialBackoff:
    def __init__(self, initial: float = 0.5, max_delay: float = 60.0, factor: float = 2.0):
        self._initial = initial
        self._max = max_delay
        self._factor = factor
        self._current = initial

    def next(self) -> float:
        delay = self._current
        self._current = min(self._current * self._factor, self._max)
        return delay

    def reset(self) -> None:
        self._current = self._initial


class BaseFeed(ABC):
    """
    Base class for all WebSocket price feeds.

    Subclasses must implement:
        _get_url() -> str
        _on_connect(ws) -> None  (send subscription messages)
        _on_message(msg: str | bytes) -> None  (parse and push to bus)
    """

    def __init__(self, bus: PriceEventBus, name: str):
        self._bus = bus
        self.name = name
        self._running = False
        self._connected = False
        self._reconnect_count = 0
        self._msg_count = 0
        self._latencies_us: Deque[float] = deque(maxlen=1000)  # last 1000 message latencies
        self._last_msg_at: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the feed. Runs indefinitely until stop() is called."""
        self._running = True
        backoff = ExponentialBackoff()
        while self._running:
            try:
                await self._run_once(backoff)
            except asyncio.CancelledError:
                logger.info("[%s] Feed cancelled", self.name)
                break
            except Exception as exc:
                delay = backoff.next()
                logger.warning(
                    "[%s] Unexpected error: %s — reconnecting in %.1fs",
                    self.name, exc, delay,
                )
                await asyncio.sleep(delay)

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run_once(self, backoff: ExponentialBackoff) -> None:
        url = self._get_url()
        logger.info("[%s] Connecting to %s", self.name, url)
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=15,
                max_size=2 ** 20,          # 1 MB max message
                compression=None,          # disable per-frame compression — saves ~100µs
                open_timeout=10,
            ) as ws:
                backoff.reset()
                self._connected = True
                self._reconnect_count += 1
                logger.info("[%s] Connected (reconnect #%d)", self.name, self._reconnect_count)

                await self._on_connect(ws)

                async for message in ws:
                    if not self._running:
                        break
                    self._last_msg_at = time.monotonic()
                    self._msg_count += 1
                    try:
                        await self._on_message(message)
                    except Exception as exc:
                        logger.debug("[%s] Message parse error: %s", self.name, exc)

        except ConnectionClosed as exc:
            self._connected = False
            delay = backoff.next()
            logger.warning(
                "[%s] Connection closed: %s — reconnecting in %.1fs",
                self.name, exc, delay,
            )
            await asyncio.sleep(delay)
        except OSError as exc:
            self._connected = False
            delay = backoff.next()
            logger.warning("[%s] OS error: %s — retrying in %.1fs", self.name, exc, delay)
            await asyncio.sleep(delay)

    def _record_latency(self, event_ts_ns: int) -> None:
        """Record end-to-end latency from event timestamp to now."""
        latency_us = (time.time_ns() - event_ts_ns) / 1000.0
        self._latencies_us.append(latency_us)

    @property
    def latency_stats(self) -> dict:
        if not self._latencies_us:
            return {}
        import statistics
        lats = list(self._latencies_us)
        return {
            "p50_us": statistics.median(lats),
            "p95_us": sorted(lats)[int(len(lats) * 0.95)],
            "p99_us": sorted(lats)[int(len(lats) * 0.99)],
            "mean_us": statistics.mean(lats),
        }

    @property
    def stats(self) -> dict:
        return {
            "name": self.name,
            "connected": self._connected,
            "reconnects": self._reconnect_count,
            "messages": self._msg_count,
            **self.latency_stats,
        }

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_url(self) -> str:
        """Return the WebSocket URL to connect to."""

    @abstractmethod
    async def _on_connect(self, ws) -> None:
        """Called after connection is established. Send subscription messages here."""

    @abstractmethod
    async def _on_message(self, message) -> None:
        """Called for each incoming message. Parse and push PriceEvent to bus."""
