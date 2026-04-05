"""
Volatility tracker: maintains a VolatilityState per pair and updates it
from the price event stream. Also provides a registry so the scanner can
look up vol state by pair.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

from core.models import PriceEvent, VolatilityState

logger = logging.getLogger(__name__)


class VolatilityTracker:
    """
    Maintains a VolatilityState for each pair.
    Updated by the scanner on every PriceEvent.
    """

    def __init__(
        self,
        window_seconds: int = 300,
        min_profit_base: float = 0.005,
        vol_multiplier: float = 2.0,
    ):
        self._window = window_seconds
        self._min_profit_base = min_profit_base
        self._vol_multiplier = vol_multiplier
        self._states: Dict[str, VolatilityState] = {}

    def get_or_create(self, pair: str) -> VolatilityState:
        if pair not in self._states:
            self._states[pair] = VolatilityState(
                pair=pair,
                window_seconds=self._window,
                min_profit_base=self._min_profit_base,
                vol_multiplier=self._vol_multiplier,
            )
        return self._states[pair]

    def update(self, event: PriceEvent) -> VolatilityState:
        """Update vol state for the pair in this event."""
        state = self.get_or_create(event.pair)
        state.update(event.mid, event.timestamp_ns)
        return state

    def get(self, pair: str) -> Optional[VolatilityState]:
        return self._states.get(pair)

    def summary(self) -> Dict[str, float]:
        """Return {pair: realized_vol_pct} for all tracked pairs."""
        return {pair: s.realized_vol_pct for pair, s in self._states.items()}
