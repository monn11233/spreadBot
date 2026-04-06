"""
spreadBot — CEX-DEX Arbitrage Bot
Entry point: dispatches to collect / paper / backtest modes.

Usage:
    python main.py --mode collect          # Collect tick data to SQLite
    python main.py --mode paper            # Paper trade with live prices
    python main.py --mode backtest         # Run param sweep on stored ticks
    python main.py --mode backtest --start 2025-01-01 --end 2025-02-01
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import List

# Try uvloop for lower latency (Linux/Mac only)
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("uvloop enabled")
except ImportError:
    pass  # Standard asyncio on Windows — fine for dev

import structlog

from config import settings
from core.events import PriceEventBus
from core.models import Chain, GasState, TradeMode
from data.db import Database
from data.pair_registry import PairRegistry
from data.tick_store import TickStore
from data.trade_store import TradeStore
from feeds.base_feed import BaseFeed
from feeds.binance_ws import BinanceFeed
from feeds.dex_price_poller import DexPricePoller
from feeds.jupiter_ws import JupiterFeed
from feeds.pancakeswap_v3_ws import BSC_POOLS, PancakeSwapV3Feed
from feeds.uniswap_v3_ws import ARB_MAINNET_POOLS, ETH_MAINNET_POOLS, UniswapV3Feed
from gas.arb_gas import ArbGasTracker
from gas.bsc_gas import BscGasTracker
from gas.eth_gas import EthGasTracker
from gas.solana_priority_fee import SolanaPriorityFeeTracker
from scanner.fee_model import FeeCalculator
from scanner.opportunity_scanner import OpportunityScanner
from scanner.volatility_tracker import VolatilityTracker


def configure_logging():
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    if settings.log_json:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.stdlib.add_log_level,
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.BoundLogger,
            logger_factory=structlog.PrintLoggerFactory(),
        )
    else:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%H:%M:%S",
        )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


async def run_collect_or_paper(mode: TradeMode) -> None:
    """Shared setup for collect and paper modes — both need live feeds."""
    db = Database(settings.db_path)
    await db.connect()

    pair_registry = PairRegistry(db, top_n=settings.top_n_pairs)
    pairs = await pair_registry.refresh()
    print(f"Tracking {len(pairs)} pairs: {pairs[:5]}{'...' if len(pairs) > 5 else ''}")

    bus = PriceEventBus()
    tick_store = TickStore(db)
    trade_store = TradeStore(db)

    # Fee calculator + gas states
    fee_calc = FeeCalculator(cex_taker_fee=settings.binance_taker_fee)
    eth_gas = EthGasTracker(settings.eth_ws_url) if settings.eth_ws_url else None
    arb_gas = ArbGasTracker(settings.arb_ws_url) if settings.arb_ws_url else None
    bsc_gas = BscGasTracker(settings.bsc_ws_url) if settings.bsc_ws_url else None
    sol_gas = SolanaPriorityFeeTracker(settings.solana_ws_url) if settings.solana_ws_url else None

    if eth_gas: fee_calc.register_gas_state(Chain.ETHEREUM, eth_gas.state)
    if arb_gas: fee_calc.register_gas_state(Chain.ARBITRUM, arb_gas.state)
    if bsc_gas: fee_calc.register_gas_state(Chain.BSC, bsc_gas.state)
    if sol_gas: fee_calc.register_gas_state(Chain.SOLANA, sol_gas.state)

    # Register known pool fees
    for pool in ETH_MAINNET_POOLS + ARB_MAINNET_POOLS + BSC_POOLS:
        fee_calc.register_pool_fee(pool.address, pool.fee_tier)

    # Build feeds
    binance_symbols = pair_registry.binance_symbols()
    feeds: List[BaseFeed] = [BinanceFeed(bus, binance_symbols)]

    if settings.eth_ws_url:
        feeds.append(UniswapV3Feed(bus, settings.eth_ws_url, ETH_MAINNET_POOLS, Chain.ETHEREUM))
    if settings.arb_ws_url:
        feeds.append(UniswapV3Feed(bus, settings.arb_ws_url, ARB_MAINNET_POOLS, Chain.ARBITRUM))
    if settings.bsc_ws_url:
        feeds.append(PancakeSwapV3Feed(bus, settings.bsc_ws_url, BSC_POOLS))

    sol_pairs = [p for p in pairs if p in ("SOL/USDT", "JUP/USDT", "BONK/USDT")]
    feeds.append(JupiterFeed(bus, sol_pairs, ws_url=settings.solana_ws_url))

    # DEX price pollers — poll slot0() every 3s to get dense price series
    # Runs alongside the Swap event WebSocket feeds (complementary)
    def _ws_to_http(url: str) -> str:
        http = url.replace("wss://", "https://").replace("ws://", "http://")
        return http[:-3] if http.endswith("/ws") else http

    pollers = []
    if settings.eth_ws_url:
        pollers.append(DexPricePoller(bus, _ws_to_http(settings.eth_ws_url), ETH_MAINNET_POOLS, Chain.ETHEREUM, interval=3.0))
    if settings.arb_ws_url:
        pollers.append(DexPricePoller(bus, _ws_to_http(settings.arb_ws_url), ARB_MAINNET_POOLS, Chain.ARBITRUM, interval=3.0))

    # Volatility tracker and scanner
    vol_tracker = VolatilityTracker(min_profit_base=settings.min_profit_pct)
    tuner = None  # set in paper mode

    # Tick persistence coroutine
    async def persist_ticks():
        """Background task: drain bus events to SQLite without blocking scanner."""
        while True:
            await asyncio.sleep(1.0)
            await tick_store.flush()

    if mode == TradeMode.PAPER:
        from execution.paper_trader import PaperTrader
        from scanner.adaptive_tuner import AdaptiveTuner
        from ui.dashboard import DashboardServer

        # Adaptive self-learning tuner
        tuner = AdaptiveTuner(db, base_threshold=settings.min_profit_pct)
        await tuner.load()

        paper_trader = PaperTrader(
            trade_store=trade_store,
            mode=TradeMode.PAPER,
            min_profit_threshold=settings.min_profit_pct,
        )

        # Web dashboard (serves on settings.dashboard_host:settings.port)
        gas_trackers = {
            "eth": eth_gas,
            "arb": arb_gas,
            "bsc": bsc_gas,
            "sol": sol_gas,
        }

        # Build scanner first with a placeholder callback, then build dashboard,
        # then patch the real callback so both objects reference each other cleanly.
        rpc_urls = {
            k: v for k, v in {
                Chain.ETHEREUM: settings.eth_ws_url,
                Chain.ARBITRUM: settings.arb_ws_url,
                Chain.BSC: settings.bsc_ws_url,
            }.items() if v
        }
        scanner = OpportunityScanner(
            bus=bus,
            fee_calculator=fee_calc,
            vol_tracker=vol_tracker,
            min_profit_pct=settings.min_profit_pct,
            max_position_usd=settings.max_position_usd,
            min_pool_volume_usd=settings.min_pool_volume_usd,
            on_opportunity=None,  # patched below
            adaptive_tuner=tuner,
            rpc_urls=rpc_urls,
        )

        web_dashboard = DashboardServer(
            scanner=scanner,
            paper_trader=paper_trader,
            feeds=feeds,
            gas_trackers=gas_trackers,
            tuner=tuner,
            host=settings.dashboard_host,
            port=settings.port,
        )

        async def on_opp(opp):
            record = await paper_trader.on_opportunity(opp)
            if record is not None:
                await tuner.record_outcome(record)
                web_dashboard.on_trade(record)
            web_dashboard.on_opportunity(opp)

        scanner._on_opportunity = lambda opp: asyncio.ensure_future(on_opp(opp))

        print(f"[dashboard] http://{settings.dashboard_host}:{settings.port}/")

        gas_tasks = [
            asyncio.create_task(g.start())
            for g in [eth_gas, arb_gas, bsc_gas, sol_gas] if g
        ]
        poller_tasks = [asyncio.create_task(p.start()) for p in pollers]
        tasks = [
            asyncio.create_task(feed.start()) for feed in feeds
        ] + gas_tasks + poller_tasks + [
            asyncio.create_task(scanner.run()),
            asyncio.create_task(web_dashboard.run()),
            asyncio.create_task(tuner.run_persist_loop()),
            asyncio.create_task(persist_ticks()),
        ]
    else:
        # Collect mode: just write ticks, no scanner
        async def collect_from_bus():
            while True:
                event = await bus.get()
                await tick_store.write(event)
                bus.task_done()

        scanner = None
        gas_tasks = [
            asyncio.create_task(g.start())
            for g in [eth_gas, arb_gas, bsc_gas, sol_gas] if g
        ]
        poller_tasks = [asyncio.create_task(p.start()) for p in pollers]
        tasks = [
            asyncio.create_task(feed.start()) for feed in feeds
        ] + gas_tasks + poller_tasks + [
            asyncio.create_task(collect_from_bus()),
        ]
        print(f"[collect] Writing ticks to {settings.db_path} -- press Ctrl+C to stop")

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutting down...")
    finally:
        await tick_store.flush()
        if scanner:
            await scanner.stop()
            await scanner._slippage_model.close()
        if tuner:
            await tuner.stop()
        for feed in feeds:
            await feed.stop()
        await db.close()


async def run_backtest(start_str: str, end_str: str) -> None:
    from backtest.engine import BacktestEngine
    from backtest.param_sweep import ParamSweep
    from backtest.reporter import BacktestReporter

    db = Database(settings.db_path)
    await db.connect()

    tick_store = TickStore(db)
    trade_store = TradeStore(db)
    fee_calc = FeeCalculator(cex_taker_fee=settings.binance_taker_fee)

    # Use conservative gas fallbacks for backtest (gas state not live)
    for pool in ETH_MAINNET_POOLS + ARB_MAINNET_POOLS + BSC_POOLS:
        fee_calc.register_pool_fee(pool.address, pool.fee_tier)

    start_dt = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end_str).replace(tzinfo=timezone.utc)

    thresholds = settings.sweep_threshold_list
    print(f"\nRunning param sweep: {start_str} → {end_str}")
    print(f"Thresholds: {[f'{t*100:.2f}%' for t in thresholds]}\n")

    sweep = ParamSweep(
        tick_store=tick_store,
        trade_store=trade_store,
        fee_calculator=fee_calc,
        thresholds=thresholds,
        max_position_usd=settings.max_position_usd,
    )
    results = await sweep.run(start_dt, end_dt)

    reporter = BacktestReporter()
    reporter.print_results(results)

    await db.close()


def main():
    parser = argparse.ArgumentParser(description="spreadBot CEX-DEX Arbitrage")
    parser.add_argument(
        "--mode",
        choices=["collect", "paper", "backtest", "live"],
        default=settings.bot_mode,
        help="Bot operating mode",
    )
    parser.add_argument("--start", default=settings.backtest_start, help="Backtest start YYYY-MM-DD")
    parser.add_argument("--end", default=settings.backtest_end, help="Backtest end YYYY-MM-DD")
    args = parser.parse_args()

    configure_logging()

    print(f"""
========================================
  spreadBot v1.0  [{args.mode}]
  CEX-DEX Arbitrage (Binance + 4 DEXs)
========================================
Mode: {args.mode}  |  Min profit: {settings.min_profit_pct*100:.2f}%  |  Max position: ${settings.max_position_usd:.0f}
DB: {settings.db_path}
""")

    if args.mode in ("collect", "paper"):
        asyncio.run(run_collect_or_paper(TradeMode(args.mode)))
    elif args.mode == "backtest":
        asyncio.run(run_backtest(args.start, args.end))
    elif args.mode == "live":
        print("Live trading not yet implemented. Run in paper mode first.")
        sys.exit(1)


if __name__ == "__main__":
    main()
