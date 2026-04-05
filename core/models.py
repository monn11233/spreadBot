"""
Core data models for the spreadBot CEX-DEX arbitrage bot.
All hot-path objects use __slots__ to minimize allocation overhead.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class Chain(str, Enum):
    ETHEREUM = "eth"
    BSC = "bsc"
    ARBITRUM = "arb"
    SOLANA = "sol"


class Direction(Enum):
    BUY_CEX_SELL_DEX = auto()  # CEX ask < DEX bid equivalent → buy on CEX, sell on DEX
    BUY_DEX_SELL_CEX = auto()  # DEX ask < CEX bid equivalent → buy on DEX, sell on CEX


class TradeMode(str, Enum):
    PAPER = "paper"
    BACKTEST = "backtest"
    LIVE = "live"
    COLLECT = "collect"


class TradeStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Hot-path objects — use __slots__ for minimal allocation
# ---------------------------------------------------------------------------

class PriceEvent:
    """Raw price update from any feed. The fundamental unit flowing through the system."""
    __slots__ = (
        "source", "pair", "bid", "ask", "timestamp_ns",
        "chain", "pool_address", "block_number", "volume_24h_usd",
    )

    def __init__(
        self,
        source: str,
        pair: str,
        bid: float,
        ask: float,
        timestamp_ns: Optional[int] = None,
        chain: Optional[Chain] = None,
        pool_address: Optional[str] = None,
        block_number: Optional[int] = None,
        volume_24h_usd: float = 0.0,
    ):
        self.source = source
        self.pair = pair
        self.bid = bid
        self.ask = ask
        self.timestamp_ns = timestamp_ns if timestamp_ns is not None else time.time_ns()
        self.chain = chain
        self.pool_address = pool_address
        self.block_number = block_number
        self.volume_24h_usd = volume_24h_usd

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    def __repr__(self) -> str:
        return (
            f"PriceEvent(source={self.source!r}, pair={self.pair!r}, "
            f"bid={self.bid:.6f}, ask={self.ask:.6f})"
        )


class FeeModel:
    """
    All-in cost model for a specific arb route.
    All fee values as fractions (e.g. 0.001 = 0.1%).
    """
    __slots__ = (
        "cex_taker_fee", "dex_pool_fee", "gas_cost_usd",
        "notional_usd", "slippage_estimate",
    )

    def __init__(
        self,
        cex_taker_fee: float,   # e.g. 0.001 for 0.1%
        dex_pool_fee: float,    # e.g. 0.0005 for 0.05%
        gas_cost_usd: float,    # absolute USD amount
        notional_usd: float,    # trade size in USD
        slippage_estimate: float = 0.0,
    ):
        self.cex_taker_fee = cex_taker_fee
        self.dex_pool_fee = dex_pool_fee
        self.gas_cost_usd = gas_cost_usd
        self.notional_usd = notional_usd
        self.slippage_estimate = slippage_estimate

    @property
    def gas_pct(self) -> float:
        if self.notional_usd <= 0:
            return 0.0
        return self.gas_cost_usd / self.notional_usd

    @property
    def total_cost_pct(self) -> float:
        """Total round-trip cost as a fraction of notional."""
        return self.cex_taker_fee + self.dex_pool_fee + self.gas_pct + self.slippage_estimate

    def __repr__(self) -> str:
        return (
            f"FeeModel(cex={self.cex_taker_fee*100:.3f}%, "
            f"dex={self.dex_pool_fee*100:.3f}%, "
            f"gas=${self.gas_cost_usd:.4f}, "
            f"slip={self.slippage_estimate*100:.3f}%, "
            f"total={self.total_cost_pct*100:.3f}%)"
        )


class Opportunity:
    """A detected arb opportunity, pre-fee and post-fee."""
    __slots__ = (
        "pair", "direction", "cex_price", "dex_price_usd",
        "raw_spread_pct", "net_profit_pct", "fee_model",
        "chain", "pool_address", "detected_at_ns",
        "cex_event", "dex_event", "vol_state", "is_viable",
        "notional_usd",
    )

    def __init__(
        self,
        pair: str,
        direction: Direction,
        cex_price: float,
        dex_price_usd: float,
        raw_spread_pct: float,
        net_profit_pct: float,
        fee_model: FeeModel,
        chain: Chain,
        pool_address: str,
        detected_at_ns: int,
        cex_event: PriceEvent,
        dex_event: PriceEvent,
        vol_state: "VolatilityState",
        is_viable: bool,
        notional_usd: float = 0.0,
    ):
        self.pair = pair
        self.direction = direction
        self.cex_price = cex_price
        self.dex_price_usd = dex_price_usd
        self.raw_spread_pct = raw_spread_pct
        self.net_profit_pct = net_profit_pct
        self.fee_model = fee_model
        self.chain = chain
        self.pool_address = pool_address
        self.detected_at_ns = detected_at_ns
        self.cex_event = cex_event
        self.dex_event = dex_event
        self.vol_state = vol_state
        self.is_viable = is_viable
        self.notional_usd = notional_usd

    def __repr__(self) -> str:
        arrow = "CEX→DEX" if self.direction == Direction.BUY_CEX_SELL_DEX else "DEX→CEX"
        return (
            f"Opportunity({self.pair} {arrow} chain={self.chain.value} "
            f"net={self.net_profit_pct*100:.3f}% viable={self.is_viable})"
        )


# ---------------------------------------------------------------------------
# Volatility state — dataclass (not hot-path, so no __slots__ needed)
# ---------------------------------------------------------------------------

@dataclass
class VolatilityState:
    """Rolling realized volatility tracker per pair."""
    pair: str
    window_seconds: int = 300        # 5-minute rolling window
    min_profit_base: float = 0.005   # 0.5% base threshold
    vol_multiplier: float = 2.0      # Scale factor when vol is elevated
    realized_vol_pct: float = field(default=0.0, init=False)
    _prices: list = field(default_factory=list, repr=False, init=False)
    _timestamps: list = field(default_factory=list, repr=False, init=False)

    def update(self, mid_price: float, ts_ns: int) -> None:
        """Add a new price sample and recompute realized vol."""
        cutoff_ns = ts_ns - self.window_seconds * 1_000_000_000
        # Trim stale samples
        while self._timestamps and self._timestamps[0] < cutoff_ns:
            self._timestamps.pop(0)
            self._prices.pop(0)

        self._prices.append(mid_price)
        self._timestamps.append(ts_ns)

        if len(self._prices) < 2:
            self.realized_vol_pct = 0.0
            return

        # Log returns
        import math
        returns = [
            math.log(self._prices[i] / self._prices[i - 1])
            for i in range(1, len(self._prices))
        ]
        n = len(returns)
        mean = sum(returns) / n
        variance = sum((r - mean) ** 2 for r in returns) / n
        self.realized_vol_pct = math.sqrt(variance) * 100.0

    @property
    def dynamic_min_profit(self) -> float:
        """Threshold scales up with volatility to avoid chasing noise."""
        if self.realized_vol_pct <= 0:
            return self.min_profit_base
        # Scale: if vol > 1%, multiply threshold
        vol_factor = max(1.0, self.realized_vol_pct / 1.0)
        return min(self.min_profit_base * vol_factor, 0.02)  # cap at 2%

    def __repr__(self) -> str:
        return (
            f"VolatilityState(pair={self.pair!r}, "
            f"vol={self.realized_vol_pct:.4f}%, "
            f"dyn_min={self.dynamic_min_profit*100:.3f}%)"
        )


# ---------------------------------------------------------------------------
# Trade record — persisted to SQLite
# ---------------------------------------------------------------------------

@dataclass
class TradeRecord:
    """Full record of a trade execution (paper, backtest, or live)."""
    mode: TradeMode
    pair: str
    direction: str          # Direction.name
    chain: str              # Chain.value
    pool_address: str
    entry_time_ns: int
    cex_fill_price: float
    dex_fill_price: float
    notional_usd: float
    gross_pnl_usd: float
    fees_usd: float
    gas_usd: float
    net_pnl_usd: float
    net_pnl_pct: float
    min_profit_threshold_used: float  # The config threshold at signal time
    raw_spread_at_entry: float
    realized_vol_at_entry: float
    slippage_usd: float
    exit_time_ns: Optional[int] = None
    status: TradeStatus = TradeStatus.FILLED
    notes: str = ""
    id: Optional[int] = None  # SQLite rowid, set after insert


# ---------------------------------------------------------------------------
# Gas state — shared across scanner (updated by gas trackers)
# ---------------------------------------------------------------------------

@dataclass
class GasState:
    """Latest gas cost estimate for a chain. Updated by gas/*.py trackers."""
    chain: Chain
    gas_usd_per_swap: float = 0.0   # USD cost for a typical swap tx
    gas_gwei: float = 0.0
    block_number: int = 0
    updated_at_ns: int = field(default_factory=time.time_ns)

    def is_stale(self, max_age_seconds: float = 60.0) -> bool:
        age = (time.time_ns() - self.updated_at_ns) / 1e9
        return age > max_age_seconds
