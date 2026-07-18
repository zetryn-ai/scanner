import json
import logging
from collections import deque

import redis.asyncio as redis

from scanner.events import ScannerEvent

logger = logging.getLogger("scanner.publisher")


def _stream_name(event_type: str) -> str:
    return f"scanner:events:{event_type}"


def _event_to_fields(event: ScannerEvent) -> dict[str, str]:
    return {
        "event_type": event.event_type,
        "source": event.source,
        "mint": event.mint,
        "received_at": event.received_at,
        "raw": json.dumps(event.raw),
    }


class Publisher:
    def __init__(self, redis_url: str, buffer_size: int = 500) -> None:
        self._redis: redis.Redis | None = redis.from_url(redis_url)
        self._buffer: deque[ScannerEvent] = deque(maxlen=buffer_size)

    async def _try_flush_buffer(self) -> None:
        if self._redis is None:
            return
        while self._buffer:
            event = self._buffer[0]
            try:
                await self._redis.xadd(_stream_name(event.event_type), _event_to_fields(event))
            except Exception:
                logger.warning("redis still unavailable, keeping %d buffered events", len(self._buffer))
                return
            self._buffer.popleft()

    async def publish(self, event: ScannerEvent) -> None:
        await self._try_flush_buffer()

        if self._redis is not None:
            try:
                await self._redis.xadd(_stream_name(event.event_type), _event_to_fields(event))
                return
            except Exception as exc:
                logger.warning("redis publish failed (%s), buffering event mint=%s", exc, event.mint)

        if len(self._buffer) == self._buffer.maxlen:
            dropped = self._buffer[0]
            logger.warning("publisher buffer full, dropping oldest event mint=%s", dropped.mint)
        self._buffer.append(event)

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
