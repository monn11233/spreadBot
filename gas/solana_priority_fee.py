"""
Solana priority fee tracker.
Polls getRecentPrioritizationFees RPC every 10 seconds.
Solana fees are tiny: ~$0.001 per tx.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from core.models import Chain, GasState

logger = logging.getLogger(__name__)

_BASE_FEE_LAMPORTS = 5000  # Fixed base fee per signature


class SolanaPriorityFeeTracker:
    def __init__(self, rpc_url: str, sol_price_getter=None):
        self._rpc_url = rpc_url.replace("wss://", "https://").replace("ws://", "http://")
        self._sol_price_getter = sol_price_getter or (lambda: 150.0)
        self.state = GasState(chain=Chain.SOLANA, gas_gwei=0.0, gas_usd_per_swap=0.001)
        self._running = False

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._poll()
            except Exception as exc:
                logger.debug("[sol_gas] Poll error: %s", exc)
            await asyncio.sleep(10)

    async def stop(self) -> None:
        self._running = False

    async def _poll(self) -> None:
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getRecentPrioritizationFees",
                "params": [],
            }
            async with session.post(
                self._rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()

        fees = data.get("result", [])
        if not fees:
            return

        # Median priority fee in microlamports
        priority_fees = [f.get("prioritizationFee", 0) for f in fees]
        priority_fees.sort()
        median_microlamports = priority_fees[len(priority_fees) // 2]

        sol_price = self._sol_price_getter()
        # Total fee = base + priority (per compute unit, ~200k CU for a swap)
        compute_units = 200_000
        total_lamports = _BASE_FEE_LAMPORTS + (median_microlamports * compute_units // 1_000_000)
        gas_usd = (total_lamports / 1e9) * sol_price

        self.state.gas_usd_per_swap = gas_usd
        self.state.updated_at_ns = time.time_ns()

        logger.debug("[sol_gas] priority=%d µlamports gas=$%.5f", median_microlamports, gas_usd)
