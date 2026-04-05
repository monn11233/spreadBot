"""
Live metrics dashboard using Rich.
Displays real-time P&L, feed status, scanner stats, and recent trades.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import TYPE_CHECKING, Deque, List, Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from core.models import TradeRecord

if TYPE_CHECKING:
    from execution.paper_trader import PaperTrader
    from feeds.base_feed import BaseFeed
    from scanner.opportunity_scanner import OpportunityScanner

logger = logging.getLogger(__name__)

console = Console()


class MetricsDashboard:
    """
    Rich-based live console dashboard.
    Updates every second with latest stats from all components.
    """

    def __init__(
        self,
        scanner: "OpportunityScanner",
        paper_trader: "PaperTrader",
        feeds: List["BaseFeed"],
        refresh_interval: float = 1.0,
    ):
        self._scanner = scanner
        self._trader = paper_trader
        self._feeds = feeds
        self._refresh = refresh_interval
        self._recent_trades: Deque[TradeRecord] = deque(maxlen=20)
        self._start_time = time.time()
        self._running = False

    def add_trade(self, record: TradeRecord) -> None:
        self._recent_trades.appendleft(record)

    async def run(self) -> None:
        self._running = True
        with Live(self._render(), refresh_per_second=1, console=console) as live:
            while self._running:
                live.update(self._render())
                await asyncio.sleep(self._refresh)

    async def stop(self) -> None:
        self._running = False

    def _render(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(self._header_panel(), size=3),
            Layout(name="body"),
            Layout(self._trades_table(), size=min(len(self._recent_trades) + 3, 15)),
        )
        layout["body"].split_row(
            Layout(self._stats_panel()),
            Layout(self._feeds_panel()),
        )
        return layout

    def _header_panel(self) -> Panel:
        uptime = int(time.time() - self._start_time)
        h, m, s = uptime // 3600, (uptime % 3600) // 60, uptime % 60
        stats = self._trader.stats
        pnl = stats["total_pnl_usd"]
        pnl_color = "green" if pnl >= 0 else "red"
        text = Text()
        text.append("spreadBot ", style="bold cyan")
        text.append("CEX-DEX Arbitrage  ", style="dim")
        text.append(f"Uptime: {h:02d}:{m:02d}:{s:02d}  ", style="dim")
        text.append(f"P&L: ", style="bold")
        text.append(f"${pnl:+.4f}", style=f"bold {pnl_color}")
        return Panel(text, style="cyan")

    def _stats_panel(self) -> Panel:
        scanner_stats = self._scanner.stats
        trader_stats = self._trader.stats
        bus_stats = scanner_stats.get("bus_stats", {})

        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column(style="dim")
        table.add_column(style="bold")

        table.add_row("Events processed", f"{scanner_stats['events_processed']:,}")
        table.add_row("Opportunities", f"{scanner_stats['opportunities_found']:,}")
        table.add_row("Viable", f"{scanner_stats['opportunities_viable']:,}")
        table.add_row("Viable rate", f"{scanner_stats['viable_rate']*100:.2f}%")
        table.add_row("", "")
        table.add_row("Trades", f"{trader_stats['total_trades']:,}")
        table.add_row("Win rate", f"{trader_stats['win_rate']*100:.1f}%")
        table.add_row("Total P&L", f"${trader_stats['total_pnl_usd']:+.4f}")
        table.add_row("ROI", f"{trader_stats['roi_pct']:+.4f}%")
        table.add_row("", "")
        table.add_row("Queue size", f"{bus_stats.get('queued', 0):,}")
        table.add_row("Dropped msgs", f"{bus_stats.get('dropped', 0):,}")

        return Panel(table, title="[bold]Scanner Stats[/bold]", border_style="blue")

    def _feeds_panel(self) -> Panel:
        table = Table(show_header=True, box=None)
        table.add_column("Feed", style="cyan")
        table.add_column("Connected")
        table.add_column("Messages", justify="right")
        table.add_column("P50 µs", justify="right")

        for feed in self._feeds:
            stats = feed.stats
            connected = "[green]YES[/green]" if stats["connected"] else "[red]NO[/red]"
            p50 = f"{stats.get('p50_us', 0):.0f}" if stats.get("p50_us") else "-"
            table.add_row(
                stats["name"],
                connected,
                f"{stats['messages']:,}",
                p50,
            )

        return Panel(table, title="[bold]Feeds[/bold]", border_style="green")

    def _trades_table(self) -> Panel:
        table = Table(show_header=True, box=None)
        table.add_column("Pair", style="cyan")
        table.add_column("Dir")
        table.add_column("Chain")
        table.add_column("Raw %", justify="right")
        table.add_column("Net %", justify="right")
        table.add_column("Gas $", justify="right")
        table.add_column("P&L $", justify="right")

        for record in list(self._recent_trades)[:15]:
            pnl_str = f"${record.net_pnl_usd:+.4f}"
            pnl_color = "green" if record.net_pnl_usd > 0 else "red"
            table.add_row(
                record.pair,
                record.direction.split("_")[1],  # "CEX" or "DEX"
                record.chain.upper(),
                f"{record.raw_spread_at_entry*100:.3f}%",
                f"{record.net_pnl_pct*100:.3f}%",
                f"${record.gas_usd:.3f}",
                f"[{pnl_color}]{pnl_str}[/{pnl_color}]",
            )

        return Panel(table, title="[bold]Recent Trades (paper)[/bold]", border_style="yellow")
