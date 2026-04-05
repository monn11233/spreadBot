"""
In-memory price registry.
Stores the latest PriceEvent for each (source, pair) combination.
Thread-safe reads; single-writer (event loop).
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

from core.models import PriceEvent


RegistryKey = Tuple[str, str]  # (source, pair)


class PriceRegistry:
    """
    Stores the latest quote per (source, pair).
    The scanner reads from here to compute cross-venue spreads.
    """

    def __init__(self):
        self._store: Dict[RegistryKey, PriceEvent] = {}

    def update(self, event: PriceEvent) -> None:
        """Store the latest event for this (source, pair)."""
        self._store[(event.source, event.pair)] = event

    def get(self, source: str, pair: str) -> Optional[PriceEvent]:
        return self._store.get((source, pair))

    def get_latest_for_pair(self, pair: str) -> Dict[str, PriceEvent]:
        """Return all latest events for a given pair, keyed by source."""
        return {
            src: evt
            for (src, p), evt in self._store.items()
            if p == pair
        }

    def get_binance(self, pair: str) -> Optional[PriceEvent]:
        return self._store.get(("binance", pair))

    def all_pairs(self) -> List[str]:
        return list({pair for (_, pair) in self._store})

    def all_sources(self) -> List[str]:
        return list({src for (src, _) in self._store})

    def staleness_check(self, max_age_seconds: float = 5.0) -> Dict[RegistryKey, float]:
        """Return {(source, pair): age_seconds} for all stale entries."""
        now_ns = time.time_ns()
        threshold_ns = int(max_age_seconds * 1_000_000_000)
        return {
            key: (now_ns - evt.timestamp_ns) / 1e9
            for key, evt in self._store.items()
            if (now_ns - evt.timestamp_ns) > threshold_ns
        }

    def __len__(self) -> int:
        return len(self._store)

    def __repr__(self) -> str:
        return f"PriceRegistry({len(self._store)} entries)"
