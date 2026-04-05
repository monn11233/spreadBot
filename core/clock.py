"""
Abstract clock — allows the scanner to run identically in real-time
and backtest (simulated time) modes.
"""
from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod


class BaseClock(ABC):
    @abstractmethod
    def now_ns(self) -> int:
        """Return current time in nanoseconds."""

    @abstractmethod
    async def sleep(self, seconds: float) -> None:
        """Sleep for a duration (real or simulated)."""


class RealTimeClock(BaseClock):
    """Wall-clock time."""

    def now_ns(self) -> int:
        return time.time_ns()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class SimulatedClock(BaseClock):
    """
    Simulated clock for backtesting.
    Time advances as tick data is replayed — never sleeps.
    """

    def __init__(self, start_ns: int = 0):
        self._current_ns = start_ns

    def set_time(self, ns: int) -> None:
        """Advance simulated time to this timestamp."""
        self._current_ns = ns

    def advance(self, delta_ns: int) -> None:
        self._current_ns += delta_ns

    def now_ns(self) -> int:
        return self._current_ns

    async def sleep(self, seconds: float) -> None:
        # Backtest: advance time instead of waiting
        self._current_ns += int(seconds * 1_000_000_000)
        await asyncio.sleep(0)  # yield to event loop
