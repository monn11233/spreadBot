"""
Ethereum mainnet gas tracker.
Subscribes to newHeads via WebSocket and caches baseFeePerGas per block.
The cached value is read by the fee model — no RPC call in the hot path.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import websockets

from core.models import Chain, GasState

logger = logging.getLogger(__name__)

_TYPICAL_SWAP_GAS = 150_000  # gas units for a Uniswap v3 exactInputSingle


class EthGasTracker:
    """
    Maintains a live GasState for Ethereum mainnet.
    Updates on every new block head.
    """

    def __init__(self, ws_url: str, eth_price_getter=None):
        """
        Args:
            ws_url: Ethereum WebSocket RPC (Alchemy/Infura).
            eth_price_getter: Callable() -> float returning current ETH/USD price.
                              If None, uses a hardcoded fallback.
        """
        self._ws_url = ws_url
        self._eth_price_getter = eth_price_getter or (lambda: 3000.0)
        self.state = GasState(chain=Chain.ETHEREUM)
        self._running = False

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._run()
            except Exception as exc:
                logger.warning("[eth_gas] Error: %s — retrying in 5s", exc)
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False

    async def _run(self) -> None:
        async with websockets.connect(
            self._ws_url,
            ping_interval=20,
            compression=None,
            open_timeout=10,
        ) as ws:
            # Subscribe to new block heads
            await ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"],
            }))
            logger.info("[eth_gas] Subscribed to newHeads")

            async for message in ws:
                if not self._running:
                    break
                try:
                    data = json.loads(message)
                    params = data.get("params")
                    if not params:
                        continue
                    result = params.get("result", {})
                    if not isinstance(result, dict):
                        continue

                    base_fee_hex = result.get("baseFeePerGas", "0x0")
                    block_hex = result.get("number", "0x0")

                    base_fee_wei = int(base_fee_hex, 16)
                    block_num = int(block_hex, 16)

                    # EIP-1559: typical priority fee ~1.5 gwei
                    priority_gwei = 1.5
                    base_fee_gwei = base_fee_wei / 1e9
                    total_gwei = base_fee_gwei + priority_gwei

                    eth_price = self._eth_price_getter()
                    gas_eth = total_gwei * 1e-9 * _TYPICAL_SWAP_GAS
                    gas_usd = gas_eth * eth_price

                    self.state.gas_gwei = total_gwei
                    self.state.gas_usd_per_swap = gas_usd
                    self.state.block_number = block_num
                    self.state.updated_at_ns = time.time_ns()

                    logger.debug(
                        "[eth_gas] block=%d base=%.1f gwei gas=$%.2f",
                        block_num, base_fee_gwei, gas_usd,
                    )
                except Exception as exc:
                    logger.debug("[eth_gas] Parse error: %s", exc)
