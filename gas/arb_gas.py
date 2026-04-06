"""
Arbitrum gas tracker.
Polls eth_getBlockByNumber("latest") via HTTP every 5s (Arbitrum ~0.25s blocks,
but gas rarely changes that fast — 5s is sufficient).
Uses HTTP instead of WebSocket to avoid consuming an Alchemy WS connection slot.

Arbitrum One uses a two-component fee:
  1. L2 execution gas (typically 0.01–0.1 gwei)
  2. L1 calldata surcharge (estimated from Ethereum baseFee + calldata bytes)
Gas is 10–50x cheaper than ETH mainnet.
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from core.models import Chain, GasState

logger = logging.getLogger(__name__)

_TYPICAL_SWAP_GAS_L2 = 800_000   # Arbitrum gas units
_CALLDATA_BYTES = 200             # Approximate calldata for a swap tx
_L1_GAS_PER_BYTE = 16            # EIP-2028: non-zero bytes cost 16 L1 gas
_POLL_INTERVAL = 5.0             # seconds


class ArbGasTracker:
    def __init__(self, ws_url: str, eth_price_getter=None):
        http = ws_url.replace("wss://", "https://").replace("ws://", "http://")
        # Ankr WS URLs end in /ws — strip it to get the HTTP RPC endpoint
        self._http_url = http[:-3] if http.endswith("/ws") else http
        self._eth_price_getter = eth_price_getter or (lambda: 3000.0)
        self.state = GasState(chain=Chain.ARBITRUM)
        self._running = False

    async def start(self) -> None:
        self._running = True
        logger.info("[arb_gas] Starting HTTP poller (every %.0fs)", _POLL_INTERVAL)
        async with aiohttp.ClientSession() as session:
            while self._running:
                t0 = time.monotonic()
                try:
                    await self._poll(session)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("[arb_gas] Poll error: %s", exc)
                elapsed = time.monotonic() - t0
                await asyncio.sleep(max(0.0, _POLL_INTERVAL - elapsed))

    async def stop(self) -> None:
        self._running = False

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": ["latest", False],
        }
        async with session.post(
            self._http_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            data = await resp.json(content_type=None)

        result = data.get("result") or {}
        base_fee_hex = result.get("baseFeePerGas", "0x5F5E100")  # ~0.1 gwei default
        block_hex = result.get("number", "0x0")

        l2_base_fee_gwei = int(base_fee_hex, 16) / 1e9
        block_num = int(block_hex, 16)

        eth_price = self._eth_price_getter()

        # L2 cost
        l2_cost_eth = l2_base_fee_gwei * 1e-9 * _TYPICAL_SWAP_GAS_L2

        # L1 calldata surcharge (simplified: assume L1 baseFee ~20 gwei)
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
