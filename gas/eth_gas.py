"""
Ethereum mainnet gas tracker.
Polls eth_getBlockByNumber("latest") via HTTP every 15s and caches baseFeePerGas.
Uses HTTP instead of WebSocket to avoid consuming an Alchemy WS connection slot.
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from core.models import Chain, GasState

logger = logging.getLogger(__name__)

_TYPICAL_SWAP_GAS = 150_000  # gas units for a Uniswap v3 exactInputSingle
_POLL_INTERVAL = 15.0        # seconds — Ethereum ~12s block time


class EthGasTracker:
    """
    Maintains a live GasState for Ethereum mainnet.
    Polls the latest block header via HTTP RPC every 15 seconds.
    """

    def __init__(self, ws_url: str, eth_price_getter=None):
        """
        Args:
            ws_url: Ethereum WebSocket or HTTP RPC URL (wss:// is converted to https://).
            eth_price_getter: Callable() -> float returning current ETH/USD price.
                              If None, uses a hardcoded fallback.
        """
        http = ws_url.replace("wss://", "https://").replace("ws://", "http://")
        # Ankr WS URLs end in /ws — strip it to get the HTTP RPC endpoint
        self._http_url = http[:-3] if http.endswith("/ws") else http
        self._eth_price_getter = eth_price_getter or (lambda: 3000.0)
        self.state = GasState(chain=Chain.ETHEREUM)
        self._running = False

    async def start(self) -> None:
        self._running = True
        logger.info("[eth_gas] Starting HTTP poller (every %.0fs)", _POLL_INTERVAL)
        async with aiohttp.ClientSession() as session:
            while self._running:
                t0 = time.monotonic()
                try:
                    await self._poll(session)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("[eth_gas] Poll error: %s", exc)
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
        base_fee_hex = result.get("baseFeePerGas", "0x0")
        block_hex = result.get("number", "0x0")

        base_fee_gwei = int(base_fee_hex, 16) / 1e9
        block_num = int(block_hex, 16)

        # EIP-1559: typical priority fee ~1.5 gwei
        total_gwei = base_fee_gwei + 1.5

        eth_price = self._eth_price_getter()
        gas_eth = total_gwei * 1e-9 * _TYPICAL_SWAP_GAS
        gas_usd = gas_eth * eth_price

        self.state.gas_gwei = total_gwei
        self.state.gas_usd_per_swap = gas_usd
        self.state.block_number = block_num
        self.state.updated_at_ns = time.time_ns()

        logger.debug(
            "[eth_gas] block=%d base=%.2f gwei gas=$%.3f",
            block_num, base_fee_gwei, gas_usd,
        )
