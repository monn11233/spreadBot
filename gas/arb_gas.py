"""
Arbitrum gas tracker.
Arbitrum One uses a two-component fee:
  1. L2 execution gas (typically 0.01–0.1 gwei)
  2. L1 calldata surcharge (estimated from Ethereum baseFee + calldata bytes)

Gas is 10–50x cheaper than ETH mainnet.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import websockets

from core.models import Chain, GasState

logger = logging.getLogger(__name__)

_TYPICAL_SWAP_GAS_L2 = 800_000   # Arbitrum gas units (different from L1 gas units)
_CALLDATA_BYTES = 200             # Approximate calldata for a swap tx
_L1_GAS_PER_BYTE = 16            # EIP-2028: non-zero bytes cost 16 L1 gas


class ArbGasTracker:
    def __init__(self, ws_url: str, eth_price_getter=None):
        self._ws_url = ws_url
        self._eth_price_getter = eth_price_getter or (lambda: 3000.0)
        self.state = GasState(chain=Chain.ARBITRUM)
        self._running = False

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._run()
            except Exception as exc:
                logger.warning("[arb_gas] Error: %s — retrying in 5s", exc)
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
            await ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"],
            }))
            logger.info("[arb_gas] Subscribed to newHeads")

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

                    base_fee_hex = result.get("baseFeePerGas", "0x5F5E100")  # ~0.1 gwei default
                    block_hex = result.get("number", "0x0")
                    l2_base_fee_gwei = int(base_fee_hex, 16) / 1e9
                    block_num = int(block_hex, 16)

                    eth_price = self._eth_price_getter()

                    # L2 cost
                    l2_cost_eth = l2_base_fee_gwei * 1e-9 * _TYPICAL_SWAP_GAS_L2

                    # L1 surcharge (estimated — actual is calculated by the Arbitrum node)
                    # Simplification: assume L1 baseFee ~20 gwei when not fetched separately
                    l1_base_fee_gwei = 20.0
                    l1_calldata_gas = _CALLDATA_BYTES * _L1_GAS_PER_BYTE
                    l1_cost_eth = l1_base_fee_gwei * 1e-9 * l1_calldata_gas

                    total_gas_usd = (l2_cost_eth + l1_cost_eth) * eth_price

                    self.state.gas_gwei = l2_base_fee_gwei
                    self.state.gas_usd_per_swap = total_gas_usd
                    self.state.block_number = block_num
                    self.state.updated_at_ns = time.time_ns()

                    logger.debug(
                        "[arb_gas] block=%d l2=%.4f gwei total_gas=$%.4f",
                        block_num, l2_base_fee_gwei, total_gas_usd,
                    )
                except Exception as exc:
                    logger.debug("[arb_gas] Parse error: %s", exc)
