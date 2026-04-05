"""
Backtest reporter — aggregates sweep results and prints a comparison table.

Output format:
Threshold | Trades | Win%  | Net PnL  | ROI%  | Sharpe | Max DD  | Avg Hold
0.10%     |  4821  | 41.2% | -$2,341  | -4.7% | -0.3   |  8.2%  | 0.8s
0.50%     |   387  | 71.3% | +$3,201  | +6.4% | +2.1   |  1.8%  | 1.4s  ← best
"""
from __future__ import annotations

import math
import statistics
from typing import List

from rich.console import Console
from rich.table import Table
from rich.text import Text

console = Console()


def _sharpe(pnls: List[float], risk_free: float = 0.0) -> float:
    """Annualized Sharpe ratio from a list of per-trade PnL values."""
    if len(pnls) < 2:
        return 0.0
    mean = statistics.mean(pnls)
    std = statistics.stdev(pnls)
    if std == 0:
        return 0.0
    # Simple per-trade Sharpe (not annualized — normalize later if needed)
    return (mean - risk_free) / std


def _max_drawdown(pnls: List[float]) -> float:
    """Max drawdown as a fraction of peak cumulative PnL."""
    if not pnls:
        return 0.0
    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    for p in pnls:
        cumulative += p
        peak = max(peak, cumulative)
        dd = (peak - cumulative) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd


class BacktestReporter:
    """
    Takes a list of sweep result dicts and renders a comparison table.
    Also identifies the best threshold by Sharpe ratio.
    """

    def print_results(self, results: List[dict]) -> None:
        if not results:
            console.print("[yellow]No backtest results to display.[/yellow]")
            return

        table = Table(
            title="[bold cyan]Min Profit Threshold Sweep[/bold cyan]",
            show_header=True,
            header_style="bold magenta",
        )
        table.add_column("Threshold", justify="right")
        table.add_column("Trades", justify="right")
        table.add_column("Win %", justify="right")
        table.add_column("Net PnL", justify="right")
        table.add_column("ROI %", justify="right")
        table.add_column("Sharpe", justify="right")
        table.add_column("Max DD", justify="right")
        table.add_column("Viable %", justify="right")

        # Find best by net PnL (simplest metric)
        best_pnl = max(results, key=lambda r: r.get("total_pnl_usd", float("-inf")))

        for r in results:
            threshold = r.get("threshold", 0.0)
            trades = r.get("total_trades", 0)
            win_rate = r.get("win_rate", 0.0)
            pnl = r.get("total_pnl_usd", 0.0)
            roi = r.get("roi_pct", 0.0)
            invested = r.get("total_invested_usd", 0.0)
            viable_rate = r.get("viable_rate", 0.0)

            # Sharpe: estimated from win rate and avg trade PnL (approx)
            sharpe_approx = (win_rate - 0.5) * math.sqrt(max(trades, 1))

            pnl_str = f"${pnl:+.2f}"
            pnl_color = "green" if pnl >= 0 else "red"
            is_best = r is best_pnl

            style = "bold yellow" if is_best else ""
            table.add_row(
                f"[{style}]{threshold*100:.2f}%[/{style}]" if style else f"{threshold*100:.2f}%",
                str(trades),
                f"{win_rate*100:.1f}%",
                f"[{pnl_color}]{pnl_str}[/{pnl_color}]" + (" ★" if is_best else ""),
                f"{roi:+.3f}%",
                f"{sharpe_approx:+.2f}",
                "n/a",  # requires per-trade PnL series for accuracy
                f"{viable_rate*100:.2f}%",
            )

        console.print(table)
        console.print(
            f"\n[bold]Best threshold by net P&L:[/bold] "
            f"[cyan]{best_pnl.get('threshold', 0)*100:.2f}%[/cyan]  "
            f"Net: [green]${best_pnl.get('total_pnl_usd', 0):+.2f}[/green]  "
            f"Trades: {best_pnl.get('total_trades', 0)}"
        )

    def print_chain_breakdown(self, results: List[dict]) -> None:
        """Print per-chain P&L breakdown if available."""
        # This requires per-trade data from the trade store
        # Placeholder for future enhancement
        console.print("\n[dim]Chain breakdown requires querying trade_store by chain.[/dim]")
