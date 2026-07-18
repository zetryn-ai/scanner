# Scanner Phase 1 — PumpPortal Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Python process that connects to PumpPortal's public WebSocket, normalizes `new_token` and `migration` events into a validated schema, and publishes them to Redis Streams — with reconnect handling that treats disconnects as routine, not exceptional.

**Architecture:** Single `asyncio` process, no framework, no Docker. Three isolated modules (`events.py` for schema/parsing, `pumpportal.py` for the WebSocket connection + reconnect loop, `publisher.py` for Redis Streams with a bounded retry buffer) wired together in `main.py`. Deployed via PM2 on the same VPS as the `router` project, following its exact operational pattern (systemd-managed PM2 daemon, `.env` for secrets, `ecosystem.config.js`).

**Tech Stack:** Python 3.12+ (VPS has 3.12.3), `uv` for dependency/venv management (VPS lacks a working `pip3` CLI — `uv` is a single static binary, no separate bootstrap needed), `websockets` for the WS client, `redis` (async client, `redis.asyncio`), `pydantic` v2 for event validation, `pytest` + `pytest-asyncio` + `fakeredis` for testing.

## Global Constraints

- Python 3.12+ only (matches the VPS's installed interpreter; do not require 3.13 features).
- The process must never exit on a WebSocket connection error — only on a manual stop or an explicitly logged fatal condition (Redis unreachable after repeated retries).
- Redis Streams (`XADD`), not Pub/Sub — Pub/Sub drops events for consumers not actively listening, which breaks the "no consumer exists yet" and "consumer may restart independently" requirements from the spec.
- Two streams: `scanner:events:new_token` and `scanner:events:migration` — never a single combined stream.
- Redis bound to `127.0.0.1` only, no password (same trust boundary as Router's SQLite file — localhost-only, no network exposure). Verify this explicitly after installing Redis on the VPS.
- Exponential backoff for reconnects: 1s, 2s, 4s, 8s, 16s, capped at 30s, plus up to 20% random jitter.
- Publisher retry buffer: bounded ring buffer of the last 500 events; on overflow, drop the oldest and log a warning with the drop count — never block or crash the WebSocket read loop.
- Malformed/incomplete WebSocket payloads must never raise out of the parse function — return `None`, the caller logs a warning and skips.
- `ai-agent` and `bot` repositories are frozen reference-only — do not import from them, do not modify them.
- Definition of done: the process runs on the VPS, survives reconnects, and `redis-cli XLEN scanner:events:new_token` shows a monotonically increasing count over a 10+ minute window with zero crashes.

---

## File Structure

```
scanner/
├── pyproject.toml
├── .env.example
├── .gitignore
├── src/scanner/
│   ├── __init__.py
│   ├── config.py
│   ├── events.py
│   ├── pumpportal.py
│   ├── publisher.py
│   └── main.py
├── tests/
│   ├── __init__.py
│   ├── test_events.py
│   ├── test_publisher.py
│   └── test_pumpportal.py
├── ecosystem.config.js
└── README.md
```

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `src/scanner/__init__.py`
- Create: `tests/__init__.py`

**Interfaces:**
- Produces: an installable Python package `scanner` under `src/`, a working `pytest` runner, and `uv` as the dependency manager.

- [ ] **Step 1: Verify uv is available locally**

Run: `uv --version`
Expected: a version string (e.g. `uv 0.x.x`). If missing, this plan assumes `uv` is already installed (confirmed present in this environment at `~/.local/bin/uv`).

- [ ] **Step 2: Create the package directory and pyproject.toml**

Run:
```bash
cd /mnt/data/Project/zetryn/scanner
mkdir -p src/scanner tests
```

Create `pyproject.toml`:
```toml
[project]
name = "zetryn-scanner"
version = "0.1.0"
description = "Real-time Solana token/migration event scanner — publishes to Redis Streams"
requires-python = ">=3.12"
dependencies = [
    "websockets>=13.0",
    "redis>=5.0",
    "pydantic>=2.0",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "fakeredis>=2.23",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/scanner"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 3: Create package init and test init**

Create `src/scanner/__init__.py`:
```python
"""Zetryn Scanner — real-time Solana event ingestion into Redis Streams."""
```

Create `tests/__init__.py`:
```python
```

- [ ] **Step 4: Create .env.example and .gitignore**

Create `.env.example`:
```
PUMPPORTAL_API_KEY=
REDIS_URL=redis://127.0.0.1:6379
```

Create `.gitignore`:
```
.venv/
__pycache__/
*.pyc
.env
.pytest_cache/
*.egg-info/
```

- [ ] **Step 5: Install dependencies and verify the environment**

Run:
```bash
cd /mnt/data/Project/zetryn/scanner
uv sync
```
Expected: creates `.venv/` and `uv.lock`, installs all deps with no errors.

Run: `uv run pytest --collect-only`
Expected: `collected 0 items` (no test files yet, but pytest runs without import errors).

- [ ] **Step 6: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git init
git add pyproject.toml .gitignore .env.example src/scanner/__init__.py tests/__init__.py uv.lock
git commit -m "chore: scaffold scanner package with uv, pytest, pydantic, websockets, redis

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Event schema + parsing

**Files:**
- Create: `src/scanner/events.py`
- Test: `tests/test_events.py`

**Interfaces:**
- Produces:
  - `class ScannerEvent(BaseModel)` with fields `event_type: Literal["new_token", "migration"]`, `source: Literal["pumpportal"]`, `mint: str`, `raw: dict`, `received_at: str`.
  - `def parse_pumpportal_message(payload: dict) -> ScannerEvent | None` — returns `None` (never raises) if the payload cannot be classified as `new_token` or `migration`, or is missing a `mint` field.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_events.py`:
```python
from datetime import datetime, timezone

from scanner.events import ScannerEvent, parse_pumpportal_message


def test_scanner_event_requires_all_fields():
    event = ScannerEvent(
        event_type="new_token",
        source="pumpportal",
        mint="ABC123mintaddress",
        raw={"foo": "bar"},
        received_at="2026-07-18T00:00:00+00:00",
    )
    assert event.event_type == "new_token"
    assert event.mint == "ABC123mintaddress"


def test_parse_new_token_payload():
    # Shape based on PumpPortal's documented subscribeNewToken payload.
    payload = {
        "txType": "create",
        "mint": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "name": "Example Token",
        "symbol": "EXPL",
    }
    event = parse_pumpportal_message(payload)
    assert event is not None
    assert event.event_type == "new_token"
    assert event.source == "pumpportal"
    assert event.mint == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    assert event.raw == payload
    # received_at must be a parseable ISO8601 UTC timestamp
    parsed = datetime.fromisoformat(event.received_at)
    assert parsed.tzinfo is not None


def test_parse_migration_payload():
    payload = {
        "txType": "migrate",
        "mint": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "pool": "raydium",
    }
    event = parse_pumpportal_message(payload)
    assert event is not None
    assert event.event_type == "migration"
    assert event.mint == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"


def test_parse_unknown_tx_type_returns_none():
    payload = {"txType": "trade", "mint": "someMint"}
    assert parse_pumpportal_message(payload) is None


def test_parse_missing_mint_returns_none():
    payload = {"txType": "create", "name": "No Mint Field"}
    assert parse_pumpportal_message(payload) is None


def test_parse_non_dict_returns_none():
    assert parse_pumpportal_message("not a dict") is None
    assert parse_pumpportal_message(None) is None
    assert parse_pumpportal_message([1, 2, 3]) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_events.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.events'` (or similar import error).

- [ ] **Step 3: Implement events.py**

Create `src/scanner/events.py`:
```python
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel


class ScannerEvent(BaseModel):
    event_type: Literal["new_token", "migration"]
    source: Literal["pumpportal"]
    mint: str
    raw: dict
    received_at: str


# PumpPortal's txType field values that map to each event_type we care about.
# Any other txType (e.g. "buy", "sell", "trade") is not relevant to this
# phase and is intentionally ignored, not an error.
_TX_TYPE_TO_EVENT_TYPE: dict[str, Literal["new_token", "migration"]] = {
    "create": "new_token",
    "migrate": "migration",
}


def parse_pumpportal_message(payload: object) -> ScannerEvent | None:
    """Parse a raw PumpPortal WebSocket message into a ScannerEvent.

    Returns None (never raises) for any payload that isn't a dict, doesn't
    have a recognized txType, or is missing a mint address — the caller is
    expected to log and skip rather than crash the read loop on bad data.
    """
    if not isinstance(payload, dict):
        return None

    tx_type = payload.get("txType")
    event_type = _TX_TYPE_TO_EVENT_TYPE.get(tx_type)
    if event_type is None:
        return None

    mint = payload.get("mint")
    if not isinstance(mint, str) or not mint:
        return None

    return ScannerEvent(
        event_type=event_type,
        source="pumpportal",
        mint=mint,
        raw=payload,
        received_at=datetime.now(timezone.utc).isoformat(),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_events.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/events.py tests/test_events.py
git commit -m "feat: add ScannerEvent schema and PumpPortal payload parsing

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Redis publisher with bounded retry buffer

**Files:**
- Create: `src/scanner/publisher.py`
- Test: `tests/test_publisher.py`

**Interfaces:**
- Consumes: `ScannerEvent` (Task 2).
- Produces:
  - `class Publisher` with `async def __init__(self, redis_url: str, buffer_size: int = 500)`.
  - `async def publish(self, event: ScannerEvent) -> None` — attempts `XADD scanner:events:<event_type>`; on failure, buffers the event (dropping the oldest if `buffer_size` is exceeded) and re-attempts buffered events on the next call.
  - `async def close(self) -> None`.
  - Stream naming: `scanner:events:new_token`, `scanner:events:migration` (derived from `event.event_type`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_publisher.py`:
```python
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


@pytest.fixture
async def fake_redis_client():
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


async def test_publish_writes_to_the_correct_stream(fake_redis_client):
    publisher = Publisher.__new__(Publisher)
    publisher._redis = fake_redis_client
    publisher._buffer = []
    publisher._buffer_size = 500

    await publisher.publish(make_event(event_type="new_token"))
    await publisher.publish(make_event(event_type="migration"))

    new_token_len = await fake_redis_client.xlen("scanner:events:new_token")
    migration_len = await fake_redis_client.xlen("scanner:events:migration")
    assert new_token_len == 1
    assert migration_len == 1


async def test_publish_stores_event_fields(fake_redis_client):
    publisher = Publisher.__new__(Publisher)
    publisher._redis = fake_redis_client
    publisher._buffer = []
    publisher._buffer_size = 500

    await publisher.publish(make_event(mint="ABC123"))

    entries = await fake_redis_client.xrange("scanner:events:new_token")
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert fields[b"mint"] == b"ABC123"
    assert fields[b"event_type"] == b"new_token"


async def test_buffer_overflow_drops_oldest():
    publisher = Publisher.__new__(Publisher)
    publisher._redis = None  # simulate Redis being unreachable
    publisher._buffer = []
    publisher._buffer_size = 3

    for i in range(5):
        await publisher.publish(make_event(mint=f"mint{i}"))

    assert len(publisher._buffer) == 3
    remaining_mints = [e.mint for e in publisher._buffer]
    assert remaining_mints == ["mint2", "mint3", "mint4"]


async def test_buffered_events_flush_once_redis_is_available(fake_redis_client):
    publisher = Publisher.__new__(Publisher)
    publisher._redis = None
    publisher._buffer = []
    publisher._buffer_size = 500

    await publisher.publish(make_event(mint="buffered-1"))
    assert len(publisher._buffer) == 1

    # Redis becomes available again
    publisher._redis = fake_redis_client
    await publisher.publish(make_event(mint="live-1"))

    stream_len = await fake_redis_client.xlen("scanner:events:new_token")
    assert stream_len == 2
    assert publisher._buffer == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_publisher.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.publisher'`.

- [ ] **Step 3: Implement publisher.py**

Create `src/scanner/publisher.py`:
```python
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
        "raw": str(event.raw),
    }


class Publisher:
    def __init__(self, redis_url: str, buffer_size: int = 500) -> None:
        self._redis: redis.Redis | None = redis.from_url(redis_url)
        self._buffer: deque[ScannerEvent] = deque(maxlen=buffer_size)
        self._buffer_size = buffer_size

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_publisher.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/publisher.py tests/test_publisher.py
git commit -m "feat: add Redis Streams publisher with bounded retry buffer

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Config loading

**Files:**
- Create: `src/scanner/config.py`

**Interfaces:**
- Produces:
  - `PUMPPORTAL_WS_URL: str` constant.
  - `RECONNECT_BASE_DELAY: float = 1.0`, `RECONNECT_MAX_DELAY: float = 30.0`, `RECONNECT_JITTER_FRACTION: float = 0.2` constants.
  - `PUBLISHER_BUFFER_SIZE: int = 500` constant.
  - `def load_redis_url() -> str` — reads `REDIS_URL` env var, defaults to `redis://127.0.0.1:6379`.
  - `def load_pumpportal_api_key() -> str | None` — reads `PUMPPORTAL_API_KEY` env var, returns `None` if unset (unused in phase 1's keyless subscriptions, but loaded for forward compatibility).

No test file for this task — it's pure env/constant wiring with no branching logic worth a unit test on its own; its behavior is exercised indirectly by Task 5's integration.

- [ ] **Step 1: Implement config.py**

Create `src/scanner/config.py`:
```python
import os

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"

# Exponential backoff for WebSocket reconnects: 1s, 2s, 4s, 8s, 16s, capped at 30s,
# plus up to 20% random jitter. Disconnects are routine (per PumpPortal's own FAQ),
# not exceptional — this loop is expected to run indefinitely.
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_JITTER_FRACTION = 0.2

PUBLISHER_BUFFER_SIZE = 500


def load_redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")


def load_pumpportal_api_key() -> str | None:
    return os.environ.get("PUMPPORTAL_API_KEY") or None
```

- [ ] **Step 2: Verify it imports cleanly**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run python -c "from scanner import config; print(config.PUMPPORTAL_WS_URL, config.load_redis_url())"`
Expected: `wss://pumpportal.fun/api/data redis://127.0.0.1:6379`

- [ ] **Step 3: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/config.py
git commit -m "feat: add config module (WS URL, backoff constants, env loading)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: PumpPortal connection + reconnect loop

**Files:**
- Create: `src/scanner/pumpportal.py`
- Test: `tests/test_pumpportal.py`

**Interfaces:**
- Consumes: `parse_pumpportal_message` (Task 2), `Publisher.publish` (Task 3), backoff constants from `config.py` (Task 4).
- Produces:
  - `def compute_backoff_delay(attempt: int) -> float` — pure function, `attempt` is 0-indexed retry count, returns the delay in seconds (deterministic base, jitter applied by caller so it stays testable).
  - `async def run_pumpportal_scanner(publisher: Publisher, *, connect_fn=None, max_iterations: int | None = None) -> None` — the main reconnect loop. `connect_fn` is an injectable WebSocket-connect callable (defaults to `websockets.connect`) so tests can substitute a fake. `max_iterations` bounds the loop for testing (defaults to `None` = run forever); production call sites omit it.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_pumpportal.py`:
```python
import asyncio
import json

import pytest

from scanner.publisher import Publisher
from scanner.pumpportal import compute_backoff_delay, run_pumpportal_scanner


def test_backoff_delay_grows_and_caps():
    assert compute_backoff_delay(0) == 1.0
    assert compute_backoff_delay(1) == 2.0
    assert compute_backoff_delay(2) == 4.0
    assert compute_backoff_delay(3) == 8.0
    assert compute_backoff_delay(4) == 16.0
    assert compute_backoff_delay(5) == 30.0  # capped
    assert compute_backoff_delay(20) == 30.0  # stays capped


class _FakeWebSocket:
    """Yields a fixed list of messages, then raises ConnectionClosed-like error."""

    def __init__(self, messages: list[str], fail_after: bool = True):
        self._messages = list(messages)
        self._fail_after = fail_after
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._messages:
            return self._messages.pop(0)
        if self._fail_after:
            raise ConnectionResetError("simulated disconnect")
        raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _RecordingPublisher(Publisher):
    def __init__(self):
        self.published = []

    async def publish(self, event):
        self.published.append(event)


async def test_run_pumpportal_scanner_publishes_parsed_events_and_reconnects():
    new_token_payload = json.dumps({"txType": "create", "mint": "mintA"})
    connections = [
        _FakeWebSocket([new_token_payload], fail_after=True),
        _FakeWebSocket([json.dumps({"txType": "migrate", "mint": "mintB"})], fail_after=True),
    ]

    def fake_connect_fn(_url):
        return connections.pop(0)

    publisher = _RecordingPublisher()

    # sleeps happen between reconnects; patch out the real delay for test speed
    async def no_op_sleep(_seconds):
        return None

    await run_pumpportal_scanner(
        publisher,
        connect_fn=fake_connect_fn,
        max_iterations=2,
        _sleep_fn=no_op_sleep,
    )

    assert len(publisher.published) == 2
    assert publisher.published[0].mint == "mintA"
    assert publisher.published[0].event_type == "new_token"
    assert publisher.published[1].mint == "mintB"
    assert publisher.published[1].event_type == "migration"


async def test_run_pumpportal_scanner_skips_unparseable_messages():
    connections = [
        _FakeWebSocket(
            [json.dumps({"txType": "trade", "mint": "irrelevant"}), json.dumps({"txType": "create", "mint": "mintC"})],
            fail_after=True,
        ),
    ]

    def fake_connect_fn(_url):
        return connections.pop(0)

    publisher = _RecordingPublisher()

    async def no_op_sleep(_seconds):
        return None

    await run_pumpportal_scanner(
        publisher,
        connect_fn=fake_connect_fn,
        max_iterations=1,
        _sleep_fn=no_op_sleep,
    )

    assert len(publisher.published) == 1
    assert publisher.published[0].mint == "mintC"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_pumpportal.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.pumpportal'`.

- [ ] **Step 3: Implement pumpportal.py**

Create `src/scanner/pumpportal.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_pumpportal.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the full test suite**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all tests across all files PASS (13 tests: 6 events + 4 publisher + 3 pumpportal).

- [ ] **Step 6: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/pumpportal.py tests/test_pumpportal.py
git commit -m "feat: add PumpPortal WebSocket connection with reconnect loop

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Entrypoint + structured logging

**Files:**
- Create: `src/scanner/main.py`

**Interfaces:**
- Consumes: `run_pumpportal_scanner` (Task 5), `Publisher` (Task 3), `load_redis_url` (Task 4).
- Produces: a runnable module (`python -m scanner.main`) that wires everything together with JSON-lines logging to stdout.

- [ ] **Step 1: Implement main.py**

Create `src/scanner/main.py`:
```python
import asyncio
import json
import logging
import sys
import time

from scanner.config import load_redis_url
from scanner.publisher import Publisher
from scanner.pumpportal import run_pumpportal_scanner


class _JsonLinesFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
        }
        return json.dumps(payload)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonLinesFormatter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


async def _main() -> None:
    _configure_logging()
    logger = logging.getLogger("scanner.main")

    redis_url = load_redis_url()
    logger.info(f"starting scanner, redis_url={redis_url}")

    publisher = Publisher(redis_url)
    try:
        await run_pumpportal_scanner(publisher)
    finally:
        await publisher.close()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it imports and constructs cleanly (no network call yet)**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run python -c "from scanner.main import main; print('import ok')"`
Expected: `import ok`

- [ ] **Step 3: Run the full test suite once more to confirm nothing broke**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all 13 tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/main.py
git commit -m "feat: add scanner entrypoint with JSON-lines logging

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: VPS Redis install + PM2 deployment

**Files:**
- Create: `ecosystem.config.js`
- Create: `README.md`

**Interfaces:**
- Consumes: `main.py` (Task 6).
- Produces: Redis running on the VPS bound to loopback, and the scanner process running under PM2 alongside the `router` app.

- [ ] **Step 1: Install Redis on the VPS**

Run on the VPS:
```bash
apt-get update && apt-get install -y redis-server
```
Expected: package installs successfully.

- [ ] **Step 2: Configure Redis to bind to loopback only, with no password (matches Router's SQLite trust model)**

Edit `/etc/redis/redis.conf` on the VPS — confirm/set:
```
bind 127.0.0.1 -::1
protected-mode yes
port 6379
```
(This is Redis's default `bind` value on Ubuntu — verify it explicitly rather than assuming.)

Run: `systemctl restart redis-server && systemctl enable redis-server`
Expected: service starts and is enabled for boot.

- [ ] **Step 3: Verify Redis is loopback-only**

Run on the VPS: `redis-cli -h 127.0.0.1 ping`
Expected: `PONG`

Run: `ss -ltnp | grep 6379`
Expected: shows `127.0.0.1:6379` (and/or `::1:6379`), NOT `0.0.0.0:6379`.

- [ ] **Step 4: Copy the scanner project to the VPS and install dependencies**

From the local machine, push the scanner code to the VPS the same way `router` was deployed (git repo + clone, or direct copy — follow whatever transport was already established for this VPS in this session). On the VPS:
```bash
cd /opt/zetryn-scanner   # or wherever the project lands
curl -LsSf https://astral.sh/uv/install.sh | sh   # if uv isn't already on the VPS
export PATH="$HOME/.local/bin:$PATH"
uv sync
```
Expected: `.venv/` created, all dependencies installed.

- [ ] **Step 5: Create the .env file on the VPS**

```bash
cd /opt/zetryn-scanner
cat > .env <<'EOF'
REDIS_URL=redis://127.0.0.1:6379
EOF
chmod 600 .env
```
(`PUMPPORTAL_API_KEY` is left unset — phase 1 only uses the free, keyless `subscribeNewToken`/`subscribeMigration` subscriptions.)

- [ ] **Step 6: Create ecosystem.config.js**

Create `ecosystem.config.js`:
```javascript
module.exports = {
  apps: [
    {
      name: 'zetryn-scanner',
      script: '.venv/bin/python',
      args: '-m scanner.main',
      cwd: __dirname,
      env: {
        REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
      },
      instances: 1,
      autorestart: true,
    },
  ],
}
```

- [ ] **Step 7: Start via PM2 and verify**

On the VPS:
```bash
cd /opt/zetryn-scanner
set -a && . ./.env && set +a
pm2 start ecosystem.config.js
pm2 save
```
Expected: `pm2 list` shows `zetryn-scanner` as `online`.

- [ ] **Step 8: Create README.md**

Create `README.md`:
```markdown
# Zetryn Scanner

Real-time Solana token-launch and migration event scanner. Connects to
PumpPortal's public WebSocket, normalizes events, and publishes them to
Redis Streams for any consumer (bot, AI agent, ad-hoc scripts) to read.

Phase 1 covers PumpPortal only — see
`docs/superpowers/specs/2026-07-18-scanner-phase1-pumpportal-design.md`
for the full design and what's deliberately out of scope.

## Setup

1. `uv sync`
2. Copy `.env.example` to `.env` — `REDIS_URL` defaults to
   `redis://127.0.0.1:6379`; `PUMPPORTAL_API_KEY` is unused in phase 1
   (the free `subscribeNewToken`/`subscribeMigration` subscriptions need
   no key).
3. Ensure Redis is running and reachable at `REDIS_URL`.
4. `uv run python -m scanner.main`

## Running with PM2

```bash
pm2 start ecosystem.config.js
pm2 save
```

## Reading events

Two Redis Streams:
- `scanner:events:new_token`
- `scanner:events:migration`

```bash
redis-cli XLEN scanner:events:new_token
redis-cli XRANGE scanner:events:new_token - + COUNT 5
```

## Tests

```bash
uv run pytest -v
```
```

- [ ] **Step 9: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add ecosystem.config.js README.md
git commit -m "feat: add PM2 deployment config and README

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Manual integration verification (Definition of Done)

Not a code task — the final verification against the spec's Definition of Done.

- [ ] **Step 1: Confirm the process is connected**

On the VPS: `pm2 logs zetryn-scanner --lines 20 --nostream`
Expected: JSON-lines log entries, most recent showing `"starting scanner, redis_url=..."` with no repeated reconnect warnings (or, if PumpPortal disconnected, warnings followed by a successful reconnect — this is expected behavior per the spec, not a failure).

- [ ] **Step 2: Observe events accumulating over a 10+ minute window**

On the VPS, run twice with a gap:
```bash
redis-cli XLEN scanner:events:new_token
# wait 10+ minutes
redis-cli XLEN scanner:events:new_token
```
Expected: the second count is strictly greater than the first (monotonically increasing, per the spec's Definition of Done). Also check:
```bash
redis-cli XLEN scanner:events:migration
```
Expected: a non-negative count (migrations are rarer than new tokens, so 0 across a short window is acceptable, but the stream must exist and be readable).

- [ ] **Step 3: Inspect a sample event**

Run: `redis-cli XRANGE scanner:events:new_token - + COUNT 1`
Expected: a single entry with fields `event_type`, `source`, `mint`, `received_at`, `raw` — confirming the schema from Task 2 round-trips correctly through the publisher into Redis.

- [ ] **Step 4: Confirm zero crashes**

Run: `pm2 describe zetryn-scanner | grep -E "restarts|status"`
Expected: `status: online`, `restarts: 0` (or a low, explained number if PM2 itself was restarted manually during setup — not from the process crashing).

No commit for this task — it is a verification checkpoint only.
