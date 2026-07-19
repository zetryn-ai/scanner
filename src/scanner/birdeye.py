import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

import httpx

from scanner.config import (
    BIRDEYE_POLL_INTERVAL_SECONDS,
    ROUTER_BIRDEYE_PROXY_URL,
    load_router_api_key,
)
from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_BIRDEYE, ScannerEvent
from scanner.publisher import Publisher

logger = logging.getLogger("scanner.birdeye")

_UNSET = object()


def parse_birdeye_token(item: object) -> ScannerEvent | None:
    """Parse one item from Birdeye's new_listing response into a
    ScannerEvent.

    Unlike GeckoTerminal, Birdeye's response has the mint address directly
    on each item (`address`) — no JSON:API relationship resolution needed.

    Returns None (never raises) if `address` is missing, not a string, or
    empty — the caller logs and skips rather than crashing the polling
    loop.
    """
    if not isinstance(item, dict):
        return None

    mint = item.get("address")
    if not isinstance(mint, str) or not mint:
        return None

    return ScannerEvent(
        event_type=EVENT_TYPE_NEW_TOKEN,
        source=SOURCE_BIRDEYE,
        mint=mint,
        raw=item,
        received_at=datetime.now(timezone.utc).isoformat(),
    )


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def _default_http_get_fn(url: str, headers: dict, params: dict) -> httpx.Response:
    async with httpx.AsyncClient() as client:
        return await client.get(url, headers=headers, params=params)


async def run_birdeye_scanner(
    publisher: Publisher,
    *,
    api_key: str | None | object = _UNSET,
    http_get_fn: Callable[[str, dict, dict], Awaitable[object]] | None = None,
    max_iterations: int | None = None,
    _sleep_fn: Callable[[float], Awaitable[None]] = _default_sleep,
) -> None:
    """Poll Birdeye's Solana new_listing endpoint through Zetryn Router's
    proxy every BIRDEYE_POLL_INTERVAL_SECONDS, publishing a ScannerEvent
    per item. Router injects Birdeye's actual API key server-side; this
    function only needs a Router consumer key, never Birdeye's key
    directly.

    If no Router API key is available (either the `api_key` param is
    None, or it is left at its default and load_router_api_key() returns
    None), this logs one INFO line and returns immediately — no HTTP call
    is ever attempted and the polling loop never runs.

    A failed poll cycle (HTTP error, malformed JSON, etc.) is logged and
    skipped — the loop always waits for the next interval and keeps
    running, it never raises out of this function on a bad response.

    max_iterations bounds the number of poll cycles for testing; it is
    None (unbounded) in production.
    """
    resolved_key = load_router_api_key() if api_key is _UNSET else api_key
    if not resolved_key:
        logger.info("Birdeye disabled: no Router API key")
        return

    http_get_fn = http_get_fn or _default_http_get_fn
    headers = {"Authorization": f"Bearer {resolved_key}", "x-chain": "solana"}
    iterations = 0

    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        try:
            response = await http_get_fn(ROUTER_BIRDEYE_PROXY_URL, headers, {"chain": "solana", "limit": 5})
            response.raise_for_status()
            payload = response.json()
            items = payload.get("data", {}).get("items", [])
            for item in items:
                event = parse_birdeye_token(item)
                if event is None:
                    continue
                await publisher.publish(event)
        except Exception as exc:
            logger.warning("birdeye poll failed (%s), will retry next interval", exc)

        await _sleep_fn(BIRDEYE_POLL_INTERVAL_SECONDS)
