from collections import deque

import fakeredis.aioredis
import pytest

from scanner.events import ScannerEvent
from scanner.publisher import Publisher


def make_event(mint: str = "mint1", event_type: str = "new_token") -> ScannerEvent:
    return ScannerEvent(
        event_type=event_type,
        source="pumpportal",
        mint=mint,
        raw={"txType": "create", "mint": mint},
        received_at="2026-07-18T00:00:00+00:00",
    )


def make_publisher(redis_client, buffer_size: int = 500) -> Publisher:
    """Build a Publisher wired to a fake (or absent) Redis without hitting
    the real from_url connection in __init__."""
    publisher = Publisher.__new__(Publisher)
    publisher._redis = redis_client
    publisher._buffer = deque(maxlen=buffer_size)
    return publisher


@pytest.fixture
async def fake_redis_client():
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


async def test_publish_writes_to_the_correct_stream(fake_redis_client):
    publisher = make_publisher(fake_redis_client)

    await publisher.publish(make_event(event_type="new_token"))
    await publisher.publish(make_event(event_type="migration"))

    new_token_len = await fake_redis_client.xlen("scanner:events:new_token")
    migration_len = await fake_redis_client.xlen("scanner:events:migration")
    assert new_token_len == 1
    assert migration_len == 1


async def test_publish_stores_event_fields(fake_redis_client):
    publisher = make_publisher(fake_redis_client)

    await publisher.publish(make_event(mint="ABC123"))

    entries = await fake_redis_client.xrange("scanner:events:new_token")
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert fields[b"mint"] == b"ABC123"
    assert fields[b"event_type"] == b"new_token"


async def test_buffer_overflow_drops_oldest():
    publisher = make_publisher(None, buffer_size=3)  # Redis unreachable

    for i in range(5):
        await publisher.publish(make_event(mint=f"mint{i}"))

    assert len(publisher._buffer) == 3
    remaining_mints = [e.mint for e in publisher._buffer]
    assert remaining_mints == ["mint2", "mint3", "mint4"]


async def test_buffered_events_flush_once_redis_is_available(fake_redis_client):
    publisher = make_publisher(None)

    await publisher.publish(make_event(mint="buffered-1"))
    assert len(publisher._buffer) == 1

    # Redis becomes available again
    publisher._redis = fake_redis_client
    await publisher.publish(make_event(mint="live-1"))

    stream_len = await fake_redis_client.xlen("scanner:events:new_token")
    assert stream_len == 2
    assert len(publisher._buffer) == 0
