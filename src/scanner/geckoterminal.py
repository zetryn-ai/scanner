import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

import httpx

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_GECKOTERMINAL, ScannerEvent
from scanner.publisher import Publisher

logger = logging.getLogger("scanner.geckoterminal")

NEW_POOLS_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"
POLL_INTERVAL_SECONDS = 30.0


def parse_geckoterminal_pool(pool: object, included_by_id: dict) -> ScannerEvent | None:
    """Parse one pool object from GeckoTerminal's new_pools response into a
    ScannerEvent.

    `pool` is one item from the response's top-level `data` array (JSON:API
    format). `included_by_id` is a lookup built from the response's
    top-level `included` array, keyed by each included item's `id`
    (required to resolve `base_token`'s bare mint address — the
    relationship itself only carries a prefixed id like
    "solana_<mint>", not the resolved token attributes).

    Returns None (never raises) if any expected field is missing —
    the caller logs and skips rather than crashing the polling loop.
    """
    if not isinstance(pool, dict):
        return None

    relationships = pool.get("relationships")
    if not isinstance(relationships, dict):
        return None

    base_token = relationships.get("base_token")
    if not isinstance(base_token, dict):
        return None

    base_token_data = base_token.get("data")
    if not isinstance(base_token_data, dict):
        return None

    base_token_id = base_token_data.get("id")
    if not isinstance(base_token_id, str):
        return None

    included_token = included_by_id.get(base_token_id)
    if not isinstance(included_token, dict):
        return None

    token_attributes = included_token.get("attributes")
    if not isinstance(token_attributes, dict):
        return None

    mint = token_attributes.get("address")
    if not isinstance(mint, str) or not mint:
        return None

    return ScannerEvent(
        event_type=EVENT_TYPE_NEW_TOKEN,
        source=SOURCE_GECKOTERMINAL,
        mint=mint,
        raw=pool,
        received_at=datetime.now(timezone.utc).isoformat(),
    )


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def _default_http_get_fn(url: str, params: dict) -> httpx.Response:
    async with httpx.AsyncClient() as client:
        return await client.get(url, params=params)


async def run_geckoterminal_scanner(
    publisher: Publisher,
    *,
    http_get_fn: Callable[[str, dict], Awaitable[object]] | None = None,
    max_iterations: int | None = None,
    _sleep_fn: Callable[[float], Awaitable[None]] = _default_sleep,
) -> None:
    """Poll GeckoTerminal's Solana new_pools endpoint every
    POLL_INTERVAL_SECONDS, publishing a ScannerEvent per pool.

    A failed poll cycle (HTTP error, malformed JSON, etc.) is logged and
    skipped — the loop always waits for the next interval and keeps
    running, it never raises out of this function on a bad response.

    max_iterations bounds the number of poll cycles for testing; it is
    None (unbounded) in production.
    """
    http_get_fn = http_get_fn or _default_http_get_fn
    iterations = 0

    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        try:
            response = await http_get_fn(NEW_POOLS_URL, {"include": "base_token,quote_token"})
            response.raise_for_status()
            payload = response.json()
            included_by_id = {item["id"]: item for item in payload.get("included", [])}
            for pool in payload.get("data", []):
                event = parse_geckoterminal_pool(pool, included_by_id)
                if event is None:
                    continue
                await publisher.publish(event)
        except Exception as exc:
            logger.warning("geckoterminal poll failed (%s), will retry next interval", exc)

        await _sleep_fn(POLL_INTERVAL_SECONDS)
