import json

import fakeredis.aioredis
import pytest

from scanner.strategies import Strategy
from scanner.filter_runner import run_filter


def event_fields(source="birdeye", event_type="new_token", mint="MINTaaa"):
    # Matches publisher.py's _event_to_fields shape.
    return {
        "event_type": event_type,
        "source": source,
        "mint": mint,
        "received_at": "2026-07-19T00:00:00+00:00",
        "raw": json.dumps({"address": mint}),
    }


@pytest.fixture
async def fake_redis():
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


async def test_matching_events_land_in_strategy_streams(fake_redis):
    await fake_redis.xadd("scanner:events:new_token", event_fields(source="birdeye"))
    await fake_redis.xadd("scanner:events:new_token", event_fields(source="pumpportal"))

    strategies = [
        Strategy(name="birdeye-only", source_allowlist=["birdeye"]),
        Strategy(name="everything"),
    ]

    await run_filter(fake_redis, strategies, max_batches=1)

    # birdeye-only got just the birdeye event
    assert await fake_redis.xlen("scanner:strategy:birdeye-only") == 1
    # everything got both
    assert await fake_redis.xlen("scanner:strategy:everything") == 2


async def test_non_matching_events_not_forwarded_but_acked(fake_redis):
    await fake_redis.xadd("scanner:events:new_token", event_fields(source="pumpportal"))

    strategies = [Strategy(name="birdeye-only", source_allowlist=["birdeye"])]

    await run_filter(fake_redis, strategies, max_batches=1)

    # no strategy stream created for a non-match (or length 0 if created)
    assert await fake_redis.xlen("scanner:strategy:birdeye-only") == 0
    # entry was acked: pending count is zero
    pending = await fake_redis.xpending("scanner:events:new_token", "scanner-filter")
    assert pending["pending"] == 0


async def test_malformed_entry_is_skipped_and_acked(fake_redis):
    # raw is not valid JSON -> deserialization fails -> skip + ack, no crash
    bad = {
        "event_type": "new_token",
        "source": "birdeye",
        "mint": "MINTbad",
        "received_at": "2026-07-19T00:00:00+00:00",
        "raw": "not-json{{{",
    }
    await fake_redis.xadd("scanner:events:new_token", bad)

    strategies = [Strategy(name="everything")]

    await run_filter(fake_redis, strategies, max_batches=1)

    pending = await fake_redis.xpending("scanner:events:new_token", "scanner-filter")
    assert pending["pending"] == 0


async def test_forwarded_event_preserves_fields(fake_redis):
    await fake_redis.xadd("scanner:events:new_token", event_fields(source="birdeye", mint="MINTkeep"))
    await run_filter(fake_redis, [Strategy(name="everything")], max_batches=1)

    entries = await fake_redis.xrange("scanner:strategy:everything")
    assert len(entries) == 1
    _id, fields = entries[0]
    assert fields[b"mint"] == b"MINTkeep"
    assert fields[b"source"] == b"birdeye"


class _TimeoutOnceRedis:
    """Wraps a fakeredis client so the first xreadgroup raises a
    TimeoutError (as a real blocking read does when the stream is idle),
    then delegates normally. Proves run_filter treats a blocking-read
    timeout as an empty batch instead of crashing."""

    def __init__(self, inner):
        self._inner = inner
        self._raised = False

    async def xreadgroup(self, *args, **kwargs):
        if not self._raised:
            self._raised = True
            import redis.exceptions

            raise redis.exceptions.TimeoutError("simulated blocking read timeout")
        return await self._inner.xreadgroup(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


async def test_blocking_read_timeout_is_not_fatal(fake_redis):
    await fake_redis.xadd("scanner:events:new_token", event_fields(source="birdeye"))
    wrapped = _TimeoutOnceRedis(fake_redis)

    # First batch times out (no crash), second batch reads the event.
    await run_filter(wrapped, [Strategy(name="everything")], max_batches=2)

    assert await fake_redis.xlen("scanner:strategy:everything") == 1
