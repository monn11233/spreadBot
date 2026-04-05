"""Tests for the opportunity scanner."""
import asyncio
import time
import pytest
from unittest.mock import MagicMock

from core.events import PriceEventBus
from core.models import Chain, Direction, GasState, PriceEvent
from scanner.fee_model import FeeCalculator
from scanner.opportunity_scanner import OpportunityScanner
from scanner.volatility_tracker import VolatilityTracker


@pytest.fixture
def bus():
    return PriceEventBus()


@pytest.fixture
def fee_calc():
    calc = FeeCalculator(cex_taker_fee=0.001)
    # Register cheap gas for tests
    calc.register_gas_state(Chain.ARBITRUM, GasState(chain=Chain.ARBITRUM, gas_usd_per_swap=0.01))
    calc.register_gas_state(Chain.ETHEREUM, GasState(chain=Chain.ETHEREUM, gas_usd_per_swap=5.0))
    return calc


@pytest.fixture
def vol_tracker():
    return VolatilityTracker(min_profit_base=0.001)  # Low threshold for tests


@pytest.mark.asyncio
async def test_scanner_detects_opportunity(bus, fee_calc, vol_tracker):
    """Scanner should detect a viable opportunity when spread > fees."""
    opportunities = []

    scanner = OpportunityScanner(
        bus=bus,
        fee_calculator=fee_calc,
        vol_tracker=vol_tracker,
        min_profit_pct=0.001,  # 0.1% — easily achievable
        max_position_usd=100.0,
        min_pool_volume_usd=0.0,
        on_opportunity=opportunities.append,
    )

    # Push a Binance event
    cex_event = PriceEvent(
        source="binance", pair="SOL/USDT", bid=100.0, ask=100.1,
    )
    # Push a DEX event with higher bid (profitable to sell on DEX)
    dex_event = PriceEvent(
        source="uniswap_arb", pair="SOL/USDT",
        bid=101.0, ask=101.2,  # DEX bid > CEX ask → profit
        chain=Chain.ARBITRUM, pool_address="0xtest",
    )

    bus.put_nowait(cex_event)
    bus.put_nowait(dex_event)

    task = asyncio.create_task(scanner.run())
    await asyncio.sleep(0.1)
    await scanner.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    viable = [o for o in opportunities if o.is_viable]
    assert len(viable) > 0
    assert viable[0].direction == Direction.BUY_CEX_SELL_DEX
    assert viable[0].net_profit_pct > 0


@pytest.mark.asyncio
async def test_scanner_rejects_negative_spread(bus, fee_calc, vol_tracker):
    """Scanner should not emit viable opportunities when spread < fees."""
    opportunities = []

    scanner = OpportunityScanner(
        bus=bus,
        fee_calculator=fee_calc,
        vol_tracker=vol_tracker,
        min_profit_pct=0.005,
        max_position_usd=100.0,
        min_pool_volume_usd=0.0,
        on_opportunity=opportunities.append,
    )

    # Binance and DEX prices nearly identical — not profitable after fees
    bus.put_nowait(PriceEvent(source="binance", pair="SOL/USDT", bid=100.0, ask=100.1))
    bus.put_nowait(PriceEvent(
        source="uniswap_arb", pair="SOL/USDT",
        bid=100.05, ask=100.15,
        chain=Chain.ARBITRUM, pool_address="0xtest",
    ))

    task = asyncio.create_task(scanner.run())
    await asyncio.sleep(0.1)
    await scanner.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    viable = [o for o in opportunities if o.is_viable]
    assert len(viable) == 0
