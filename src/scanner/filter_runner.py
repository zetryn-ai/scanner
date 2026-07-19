import json
import logging

from scanner.events import ScannerEvent
from scanner.strategies import Strategy, event_matches_strategy, strategy_stream_name

logger = logging.getLogger("scanner.filter_runner")


def _fields_to_event(fields: dict) -> ScannerEvent | None:
    """Rebuild a ScannerEvent from Redis stream fields (reverse of
    publisher._event_to_fields). Returns None (never raises) on malformed
    data — the caller logs, acks, and moves on."""
    try:
        decoded = {
            (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in fields.items()
        }
        return ScannerEvent(
            event_type=decoded["event_type"],
            source=decoded["source"],
            mint=decoded["mint"],
            raw=json.loads(decoded["raw"]),
            received_at=decoded["received_at"],
        )
    except (KeyError, ValueError, TypeError):
        return None


async def _ensure_group(redis_client, stream: str, group: str) -> None:
    try:
        await redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        # BUSYGROUP: group already exists — fine on restart.
        if "BUSYGROUP" not in str(exc):
            raise


async def run_filter(
    redis_client,
    strategies: list[Strategy],
    *,
    source_streams: tuple[str, ...] = ("scanner:events:new_token", "scanner:events:migration"),
    group: str = "scanner-filter",
    consumer: str = "filter-1",
    block_ms: int = 5000,
    max_batches: int | None = None,
) -> None:
    """Consume raw scanner events via a consumer group, forward matches to
    per-strategy streams, and ack every entry.

    max_batches bounds the loop for testing; None runs forever.
    """
    for stream in source_streams:
        await _ensure_group(redis_client, stream, group)

    if not strategies:
        logger.info("no strategies configured; filter will read and ack but forward nothing")

    batches = 0
    while max_batches is None or batches < max_batches:
        batches += 1
        streams_arg = {stream: ">" for stream in source_streams}
        response = await redis_client.xreadgroup(group, consumer, streams_arg, count=100, block=block_ms)
        if not response:
            continue

        for stream_name_raw, entries in response:
            stream_name = stream_name_raw.decode() if isinstance(stream_name_raw, bytes) else stream_name_raw
            for entry_id, fields in entries:
                try:
                    event = _fields_to_event(fields)
                    if event is None:
                        logger.warning("skipping malformed entry %s on %s", entry_id, stream_name)
                    else:
                        for strategy in strategies:
                            if event_matches_strategy(event, strategy):
                                await redis_client.xadd(strategy_stream_name(strategy), fields)
                except Exception as exc:
                    logger.warning("error processing entry %s (%s), skipping", entry_id, exc)
                finally:
                    await redis_client.xack(stream_name, group, entry_id)
