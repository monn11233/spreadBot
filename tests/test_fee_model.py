"""Tests for the fee model."""
import pytest
from core.models import Chain, GasState, PriceEvent
from scanner.fee_model import FeeCalculator


def make_dex_event(chain: Chain = Chain.ARBITRUM, pool_address: str = "0xABC") -> PriceEvent:
    return PriceEvent(
        source="uniswap_arb",
        pair="SOL/USDT",
        bid=95.0,
        ask=95.1,
        chain=chain,
        pool_address=pool_address,
    )


def test_fee_model_total_cost():
    calc = FeeCalculator(cex_taker_fee=0.001)
    gas_state = GasState(chain=Chain.ARBITRUM, gas_usd_per_swap=0.10)
    calc.register_gas_state(Chain.ARBITRUM, gas_state)
    calc.register_pool_fee("0xABC", 500)  # 0.05%

    event = make_dex_event()
    fee_model = calc.compute(event, notional_usd=500.0)

    # 0.1% CEX + 0.05% DEX + 0.10/500 gas = 0.15% + 0.02% = 0.17%
    assert abs(fee_model.cex_taker_fee - 0.001) < 1e-9
    assert abs(fee_model.dex_pool_fee - 0.0005) < 1e-9
    assert abs(fee_model.gas_pct - 0.0002) < 1e-6
    assert fee_model.total_cost_pct < 0.005  # less than 0.5%


def test_fee_model_fallback_gas():
    calc = FeeCalculator()
    event = make_dex_event(chain=Chain.SOLANA, pool_address="mint123")
    fee_model = calc.compute(event, notional_usd=100.0)
    # Solana fallback gas = $0.001
    assert fee_model.gas_cost_usd == 0.001


def test_fee_model_total_cost_pct_positive():
    calc = FeeCalculator()
    event = make_dex_event()
    fee_model = calc.compute(event, notional_usd=200.0)
    assert fee_model.total_cost_pct > 0


def test_slippage_estimate_large_trade():
    calc = FeeCalculator()
    # Large trade relative to pool TVL → high slippage
    slippage = calc.estimate_slippage(notional_usd=50_000, pool_tvl_usd=100_000, pool_fee=0.003)
    assert slippage > 0.005  # > 0.5%


def test_slippage_estimate_small_trade():
    calc = FeeCalculator()
    slippage = calc.estimate_slippage(notional_usd=100, pool_tvl_usd=10_000_000, pool_fee=0.003)
    assert slippage < 0.0001  # < 0.01%
