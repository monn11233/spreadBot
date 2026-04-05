"""
BSC gas tracker.
BSC uses a fixed gasPrice (typically 1–3 gwei), not EIP-1559.
Polls via eth_gasPrice RPC every 30 seconds.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import websockets

from core.models import Chain, GasState

logger = logging.getLogger(__name__)

_TYPICAL_SWAP_GAS = 150_000  # gas units for PancakeSwap v3 swap


class BscGasTracker:
    def __init__(self, ws_url: str, bnb_price_getter=None):
        self._ws_url = ws_url
        self._bnb_price_getter = bnb_price_getter or (lambda: 600.0)
        self.state = GasState(chain=Chain.BSC, gas_gwei=1.0, gas_usd_per_swap=0.09)
        self._running = False

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._run()
            except Exception as exc:
                logger.warning("[bsc_gas] Error: %s — retrying in 10s", exc)
                await asyncio.sleep(10)

    async def stop(self) -> None:
        self._running = False

    async def _run(self) -> None:
        """Poll gasPrice every 30s via WebSocket JSON-RPC."""
        async with websockets.connect(
            self._ws_url,
            ping_interval=20,
            compression=None,
            open_timeout=10,
        ) as ws:
            logger.info("[bsc_gas] Connected — polling gasPrice")
            while self._running:
                try:
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_gasPrice",
                        "params": [],
                    }))
                    resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                    gas_price_hex = resp.get("result", "0x3B9ACA00")  # 1 gwei default
                    gas_price_gwei = int(gas_price_hex, 16) / 1e9

                    bnb_price = self._bnb_price_getter()
                    gas_usd = gas_price_gwei * 1e-9 * _TYPICAL_SWAP_GAS * bnb_price

                    self.state.gas_gwei = gas_price_gwei
                    self.state.gas_usd_per_swap = gas_usd
                    self.state.updated_at_ns = time.time_ns()

                    logger.debug("[bsc_gas] gasPrice=%.1f gwei gas=$%.4f", gas_price_gwei, gas_usd)
                except Exception as exc:
                    logger.debug("[bsc_gas] Poll error: %s", exc)

                await asyncio.sleep(30)
