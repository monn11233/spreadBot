"""
Typed async event bus backed by asyncio.Queue.
Feeds push PriceEvent objects; the scanner consumes them.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from core.models import PriceEvent

logger = logging.getLogger(__name__)


class PriceEventBus:
    """
    Central queue that all feeds push to, and the scanner reads from.

    If the queue is full (scanner lagging), the oldest event is dropped
    rather than blocking the feed — stale prices are worse than no prices.
    """

    def __init__(self, maxsize: int = 10_000):
        self._queue: asyncio.Queue[PriceEvent] = asyncio.Queue(maxsize=maxsize)
        self._dropped = 0
        self._total_pushed = 0
        self._total_consumed = 0

    def put_nowait(self, event: PriceEvent) -> bool:
        """Push an event. Returns False and drops if queue is full."""
        try:
            self._queue.put_nowait(event)
            self._total_pushed += 1
            return True
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                logger.warning(
                    "PriceEventBus queue full — dropped %d events total", self._dropped
                )
            return False

    async def put(self, event: PriceEvent) -> None:
        """Push an event, awaiting space if the queue is full (use sparingly)."""
        await self._queue.put(event)
        self._total_pushed += 1

    async def get(self) -> PriceEvent:
        """Consume the next event. Blocks until one is available."""
        event = await self._queue.get()
        self._total_consumed += 1
        return event

    def task_done(self) -> None:
        self._queue.task_done()

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def stats(self) -> dict:
        return {
            "queued": self._queue.qsize(),
            "pushed": self._total_pushed,
            "consumed": self._total_consumed,
            "dropped": self._dropped,
        }
