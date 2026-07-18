import asyncio
import json
import logging
import random
from typing import Awaitable, Callable

import websockets

from scanner.config import (
    PUMPPORTAL_WS_URL,
    RECONNECT_BASE_DELAY,
    RECONNECT_JITTER_FRACTION,
    RECONNECT_MAX_DELAY,
)
from scanner.events import parse_pumpportal_message
from scanner.publisher import Publisher

logger = logging.getLogger("scanner.pumpportal")

_SUBSCRIBE_NEW_TOKEN = json.dumps({"method": "subscribeNewToken"})
_SUBSCRIBE_MIGRATION = json.dumps({"method": "subscribeMigration"})


def compute_backoff_delay(attempt: int) -> float:
    """Exponential backoff, doubling from RECONNECT_BASE_DELAY, capped at
    RECONNECT_MAX_DELAY. Jitter is applied by the caller (kept out of this
    function so the growth/cap behavior stays deterministic and testable).
    """
    delay = RECONNECT_BASE_DELAY * (2**attempt)
    return min(delay, RECONNECT_MAX_DELAY)


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def run_pumpportal_scanner(
    publisher: Publisher,
    *,
    connect_fn: Callable[[str], object] | None = None,
    max_iterations: int | None = None,
    _sleep_fn: Callable[[float], Awaitable[None]] = _default_sleep,
) -> None:
    """Connect to PumpPortal, subscribe to new-token and migration events,
    publish parsed events, and reconnect indefinitely on any disconnect.

    max_iterations bounds the number of connect-attempts for testing; it is
    None (unbounded) in production.
    """
    connect_fn = connect_fn or websockets.connect
    attempt = 0
    iterations = 0

    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        try:
            async with connect_fn(PUMPPORTAL_WS_URL) as ws:
                logger.info("connected to pumpportal, subscribing")
                await ws.send(_SUBSCRIBE_NEW_TOKEN)
                await ws.send(_SUBSCRIBE_MIGRATION)
                attempt = 0  # reset backoff after a successful connection
                async for raw_message in ws:
                    try:
                        payload = json.loads(raw_message)
                    except (TypeError, ValueError):
                        logger.warning("received non-JSON message, skipping: %r", raw_message)
                        continue

                    event = parse_pumpportal_message(payload)
                    if event is None:
                        continue

                    await publisher.publish(event)
        except Exception as exc:
            delay = compute_backoff_delay(attempt)
            jitter = delay * RECONNECT_JITTER_FRACTION * random.random()
            logger.warning(
                "pumpportal connection lost (%s), reconnecting in %.1fs (attempt %d)",
                exc,
                delay + jitter,
                attempt,
            )
            attempt += 1
            await _sleep_fn(delay + jitter)
