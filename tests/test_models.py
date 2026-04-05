"""Tests for core data models."""
import time
import pytest
from core.models import (
    Chain, Direction, FeeModel, Opportunity, PriceEvent,
    TradeMode, TradeRecord, TradeStatus, VolatilityState,
)


def test_price_event_defaults():
    evt = PriceEvent(source="binance", pair="SOL/USDT", bid=90.0, ask=90.1)
    assert evt.mid == pytest.approx(90.05)
    assert evt.timestamp_ns > 0
    assert evt.chain is None


def test_fee_model_total_cost():
    fm = FeeModel(
        cex_taker_fee=0.001,
        dex_pool_fee=0.0005,
        gas_cost_usd=0.20,
        notional_usd=500.0,
        slippage_estimate=0.001,
    )
    # 0.1% + 0.05% + 0.04% gas + 0.1% slip = 0.29%
    assert abs(fm.total_cost_pct - (0.001 + 0.0005 + 0.0004 + 0.001)) < 1e-9


def test_volatility_state_update():
    vs = VolatilityState(pair="SOL/USDT", window_seconds=60)
    ts = time.time_ns()
    prices = [100.0, 101.0, 99.5, 102.0, 98.0]
    for i, p in enumerate(prices):
        vs.update(p, ts + i * 1_000_000_000)
    assert vs.realized_vol_pct > 0


def test_volatility_dynamic_min_profit_increases_with_vol():
    vs = VolatilityState(pair="X/USDT", window_seconds=60, min_profit_base=0.005)
    ts = time.time_ns()
    # Simulate high volatility
    prices = [100, 120, 80, 130, 70, 140]
    for i, p in enumerate(prices):
        vs.update(float(p), ts + i * 500_000_000)
    assert vs.dynamic_min_profit >= vs.min_profit_base


def test_volatility_dynamic_min_profit_stable_market():
    vs = VolatilityState(pair="X/USDT", window_seconds=60, min_profit_base=0.005)
    ts = time.time_ns()
    # Near-flat prices
    for i in range(10):
        vs.update(100.0 + i * 0.001, ts + i * 1_000_000_000)
    assert vs.dynamic_min_profit == pytest.approx(vs.min_profit_base, rel=0.01)
