# Scanner Source #2 — GeckoTerminal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second, independent scanner source — GeckoTerminal's Solana `new_pools` REST endpoint — running as a second `asyncio` task alongside the existing PumpPortal WebSocket task, publishing into the same Redis Streams.

**Architecture:** A new `run_geckoterminal_scanner()` coroutine polls `GET https://api.geckoterminal.com/api/v2/networks/solana/new_pools?include=base_token,quote_token` every 30 seconds, resolves each pool's base-token mint via the JSON:API `included[]` array, and publishes a `ScannerEvent` per pool. `main.py` runs it and `run_pumpportal_scanner()` concurrently via `asyncio.gather`. `events.py`'s `event_type`/`source` fields are loosened from closed `Literal` types to plain `str` backed by named constants, so future event types and sources don't require schema changes.

**Tech Stack:** Python 3.12+, `httpx` (new dependency, async HTTP client) for polling, existing `pydantic`/`redis`/`pytest`/`pytest-asyncio`/`fakeredis` stack from Phase 1.

## Global Constraints

- GeckoTerminal endpoint is keyless/public; rate limit is 30 requests/minute — polling every 30 seconds uses 2 of 30 slots/minute, well within budget. Do not poll faster than 30s without re-verifying the rate limit.
- The `?include=base_token,quote_token` query parameter is mandatory — without it, the `included[]` array is empty and mint resolution is impossible.
- Mint address must be read from `included[].attributes.address` (the bare address), never by stripping the `"solana_"` prefix from a relationship id manually.
- Every pool from `new_pools` maps to `event_type="new_token"` (unified with PumpPortal's stream, not a separate `new_pool` stream) and `source="geckoterminal"`.
- A failed poll cycle (timeout, HTTP error, malformed JSON) must be logged and skipped — no retry/backoff for REST polling failures; the loop just waits for the next 30-second interval.
- Both scanner tasks (PumpPortal, GeckoTerminal) run in the same process; a crash in one task's internals must never take down the other. Each task's own per-cycle `try/except Exception` is the mechanism — not `asyncio.gather`'s `return_exceptions`.
- `ai-agent` and `bot` repositories remain frozen reference-only — do not import from them, do not modify them.
- Definition of done: process runs continuously (local and/or VPS), `redis-cli XLEN scanner:events:new_token` shows entries with both `source: "pumpportal"` and `source: "geckoterminal"` over a 10+ minute window, zero crashes.

---

## File Structure (additions/changes)

```
scanner/
├── pyproject.toml                 # MODIFIED: add httpx dependency
├── src/scanner/
│   ├── events.py                  # MODIFIED: event_type/source Literal -> str + constants
│   ├── geckoterminal.py           # NEW: polling loop + parser
│   └── main.py                    # MODIFIED: run both scanner tasks via asyncio.gather
├── tests/
│   ├── test_events.py             # MODIFIED: assert constants match original literal values
│   └── test_geckoterminal.py      # NEW
```

---

### Task 1: Open up the event schema (str + named constants)

**Files:**
- Modify: `src/scanner/events.py`
- Modify: `tests/test_events.py`

**Interfaces:**
- Produces: `EVENT_TYPE_NEW_TOKEN = "new_token"`, `EVENT_TYPE_MIGRATION = "migration"`, `SOURCE_PUMPPORTAL = "pumpportal"`, `SOURCE_GECKOTERMINAL = "geckoterminal"` (module-level constants in `scanner.events`). `ScannerEvent.event_type: str` and `ScannerEvent.source: str` (no longer `Literal`). `parse_pumpportal_message` behavior unchanged (same return values), internals now reference the new constants instead of literal strings.

- [ ] **Step 1: Write the failing test for the new constants**

Add to `tests/test_events.py` (append at the end of the file):

```python
from scanner.events import (
    EVENT_TYPE_MIGRATION,
    EVENT_TYPE_NEW_TOKEN,
    SOURCE_GECKOTERMINAL,
    SOURCE_PUMPPORTAL,
)


def test_event_type_constants_match_expected_values():
    assert EVENT_TYPE_NEW_TOKEN == "new_token"
    assert EVENT_TYPE_MIGRATION == "migration"
    assert SOURCE_PUMPPORTAL == "pumpportal"
    assert SOURCE_GECKOTERMINAL == "geckoterminal"


def test_scanner_event_accepts_any_string_event_type_and_source():
    # event_type/source are now open str fields, not closed Literals — a
    # future source (e.g. a third scanner) must be representable without
    # a schema change.
    event = ScannerEvent(
        event_type="price_update",
        source="some_future_source",
        mint="ABC123mintaddress",
        raw={},
        received_at="2026-07-18T00:00:00+00:00",
    )
    assert event.event_type == "price_update"
    assert event.source == "some_future_source"
```

Update the import line at the top of `tests/test_events.py`:

```python
from datetime import datetime

from scanner.events import ScannerEvent, parse_pumpportal_message
```

(This import line already exists — no change needed there; the new constants import above is a separate, additional import statement appended later in the file alongside the new tests.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_events.py -v`
Expected: FAIL — `ImportError: cannot import name 'EVENT_TYPE_NEW_TOKEN' from 'scanner.events'`

- [ ] **Step 3: Update events.py**

Replace the full contents of `src/scanner/events.py`:

```python
from datetime import datetime, timezone

from pydantic import BaseModel

EVENT_TYPE_NEW_TOKEN = "new_token"
EVENT_TYPE_MIGRATION = "migration"

SOURCE_PUMPPORTAL = "pumpportal"
SOURCE_GECKOTERMINAL = "geckoterminal"


class ScannerEvent(BaseModel):
    event_type: str
    source: str
    mint: str
    raw: dict
    received_at: str


# PumpPortal's txType field values that map to each event_type we care about.
# Any other txType (e.g. "buy", "sell", "trade") is not relevant to this
# phase and is intentionally ignored, not an error.
_TX_TYPE_TO_EVENT_TYPE: dict[str, str] = {
    "create": EVENT_TYPE_NEW_TOKEN,
    "migrate": EVENT_TYPE_MIGRATION,
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
        source=SOURCE_PUMPPORTAL,
        mint=mint,
        raw=payload,
        received_at=datetime.now(timezone.utc).isoformat(),
    )
```

- [ ] **Step 4: Run the full test suite to verify everything passes**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: PASS (16 tests: the original 14 plus the 2 new ones added in Step 1).

- [ ] **Step 5: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/events.py tests/test_events.py
git commit -m "refactor: open up ScannerEvent.event_type/source to str + named constants"
```

---

### Task 2: GeckoTerminal parser (JSON:API pool -> ScannerEvent)

**Files:**
- Create: `src/scanner/geckoterminal.py` (parser + constants only in this task; polling loop added in Task 3)
- Test: `tests/test_geckoterminal.py`

**Interfaces:**
- Consumes: `ScannerEvent`, `EVENT_TYPE_NEW_TOKEN`, `SOURCE_GECKOTERMINAL` (Task 1).
- Produces: `NEW_POOLS_URL: str` constant. `def parse_geckoterminal_pool(pool: dict, included_by_id: dict) -> ScannerEvent | None` — resolves the pool's `base_token` mint via `included_by_id`, returns `None` (never raises) on any missing/malformed field.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_geckoterminal.py`:

```python
from datetime import datetime

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_GECKOTERMINAL
from scanner.geckoterminal import parse_geckoterminal_pool

# Shape verified via a live fetch to
# https://api.geckoterminal.com/api/v2/networks/solana/new_pools?include=base_token,quote_token
# (July 2026).
SAMPLE_POOL = {
    "id": "solana_8R6B7bC57N3SpZFSt9FGgGq9ZnweAW8w1aauUkDSQZoG",
    "type": "pool",
    "attributes": {
        "address": "8R6B7bC57N3SpZFSt9FGgGq9ZnweAW8w1aauUkDSQZoG",
        "name": "MOG / SOL",
        "pool_created_at": "2026-07-18T13:56:53Z",
        "reserve_in_usd": "1660.49958411643",
    },
    "relationships": {
        "base_token": {
            "data": {"id": "solana_6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump", "type": "token"}
        },
        "quote_token": {
            "data": {"id": "solana_So11111111111111111111111111111111111111112", "type": "token"}
        },
        "dex": {"data": {"id": "pump-fun", "type": "dex"}},
    },
}

SAMPLE_INCLUDED_BY_ID = {
    "solana_6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump": {
        "id": "solana_6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump",
        "type": "token",
        "attributes": {
            "address": "6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump",
            "name": "Mog Coin",
            "symbol": "MOG",
        },
    },
    "solana_So11111111111111111111111111111111111111112": {
        "id": "solana_So11111111111111111111111111111111111111112",
        "type": "token",
        "attributes": {
            "address": "So11111111111111111111111111111111111111112",
            "name": "Wrapped SOL",
            "symbol": "SOL",
        },
    },
}


def test_parse_valid_pool_resolves_mint_from_included():
    event = parse_geckoterminal_pool(SAMPLE_POOL, SAMPLE_INCLUDED_BY_ID)
    assert event is not None
    assert event.event_type == EVENT_TYPE_NEW_TOKEN
    assert event.source == SOURCE_GECKOTERMINAL
    assert event.mint == "6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump"
    assert event.raw == SAMPLE_POOL
    parsed = datetime.fromisoformat(event.received_at)
    assert parsed.tzinfo is not None


def test_parse_missing_base_token_relationship_returns_none():
    pool = {"id": "solana_x", "attributes": {}, "relationships": {}}
    assert parse_geckoterminal_pool(pool, SAMPLE_INCLUDED_BY_ID) is None


def test_parse_base_token_not_in_included_returns_none():
    pool = {
        "id": "solana_x",
        "attributes": {},
        "relationships": {
            "base_token": {"data": {"id": "solana_not_in_included", "type": "token"}}
        },
    }
    assert parse_geckoterminal_pool(pool, SAMPLE_INCLUDED_BY_ID) is None


def test_parse_included_item_missing_address_returns_none():
    pool = {
        "id": "solana_x",
        "attributes": {},
        "relationships": {
            "base_token": {"data": {"id": "solana_no_address", "type": "token"}}
        },
    }
    included = {"solana_no_address": {"id": "solana_no_address", "type": "token", "attributes": {}}}
    assert parse_geckoterminal_pool(pool, included) is None


def test_parse_non_dict_pool_returns_none():
    assert parse_geckoterminal_pool("not a dict", SAMPLE_INCLUDED_BY_ID) is None
    assert parse_geckoterminal_pool(None, SAMPLE_INCLUDED_BY_ID) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_geckoterminal.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.geckoterminal'`

- [ ] **Step 3: Implement the parser**

Create `src/scanner/geckoterminal.py`:

```python
from datetime import datetime, timezone

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_GECKOTERMINAL, ScannerEvent

NEW_POOLS_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_geckoterminal.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/geckoterminal.py tests/test_geckoterminal.py
git commit -m "feat: add GeckoTerminal new_pools parser"
```

---

### Task 3: GeckoTerminal polling loop

**Files:**
- Modify: `src/scanner/geckoterminal.py`
- Modify: `tests/test_geckoterminal.py`
- Modify: `pyproject.toml` (add `httpx` dependency)

**Interfaces:**
- Consumes: `parse_geckoterminal_pool`, `NEW_POOLS_URL` (this task's own Task 2 additions); `Publisher.publish` (existing, from `scanner.publisher`).
- Produces: `async def run_geckoterminal_scanner(publisher: Publisher, *, http_get_fn=None, max_iterations: int | None = None, _sleep_fn=_default_sleep) -> None`. `http_get_fn` is an injectable async callable `(url: str, params: dict) -> httpx.Response`-like object with `.raise_for_status()` and `.json()`, defaulting to a real `httpx.AsyncClient` call in production. `max_iterations` bounds the loop for testing (`None` = run forever in production), mirroring `run_pumpportal_scanner`'s existing pattern.

- [ ] **Step 1: Add httpx dependency**

Edit `pyproject.toml`, update the `dependencies` list:

```toml
dependencies = [
    "websockets>=13.0",
    "redis>=5.0",
    "pydantic>=2.0",
    "httpx>=0.27",
]
```

Run: `cd /mnt/data/Project/zetryn/scanner && uv sync`
Expected: installs `httpx` and its transitive deps with no errors.

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_geckoterminal.py`:

```python
from scanner.publisher import Publisher


class _RecordingPublisher(Publisher):
    def __init__(self):
        self.published = []

    async def publish(self, event):
        self.published.append(event)


class _FakeResponse:
    def __init__(self, payload: dict, status_ok: bool = True):
        self._payload = payload
        self._status_ok = status_ok

    def raise_for_status(self) -> None:
        if not self._status_ok:
            raise RuntimeError("simulated HTTP error")

    def json(self) -> dict:
        return self._payload


async def _no_op_sleep(_seconds: float) -> None:
    return None


async def test_run_geckoterminal_scanner_publishes_parsed_pools():
    payload = {"data": [SAMPLE_POOL], "included": list(SAMPLE_INCLUDED_BY_ID.values())}

    async def fake_http_get_fn(_url, _params):
        return _FakeResponse(payload)

    publisher = _RecordingPublisher()

    from scanner.geckoterminal import run_geckoterminal_scanner

    await run_geckoterminal_scanner(
        publisher,
        http_get_fn=fake_http_get_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert len(publisher.published) == 1
    assert publisher.published[0].mint == "6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump"


async def test_run_geckoterminal_scanner_skips_unparseable_pools_without_crashing():
    payload = {
        "data": [{"id": "solana_broken", "attributes": {}, "relationships": {}}],
        "included": [],
    }

    async def fake_http_get_fn(_url, _params):
        return _FakeResponse(payload)

    publisher = _RecordingPublisher()

    from scanner.geckoterminal import run_geckoterminal_scanner

    await run_geckoterminal_scanner(
        publisher,
        http_get_fn=fake_http_get_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert publisher.published == []


async def test_run_geckoterminal_scanner_continues_after_http_error():
    call_count = 0

    async def fake_http_get_fn(_url, _params):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _FakeResponse({}, status_ok=False)
        return _FakeResponse({"data": [SAMPLE_POOL], "included": list(SAMPLE_INCLUDED_BY_ID.values())})

    publisher = _RecordingPublisher()

    from scanner.geckoterminal import run_geckoterminal_scanner

    await run_geckoterminal_scanner(
        publisher,
        http_get_fn=fake_http_get_fn,
        max_iterations=2,
        _sleep_fn=_no_op_sleep,
    )

    assert call_count == 2
    assert len(publisher.published) == 1
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_geckoterminal.py -v`
Expected: FAIL — `ImportError: cannot import name 'run_geckoterminal_scanner' from 'scanner.geckoterminal'`

- [ ] **Step 4: Implement the polling loop**

Append to `src/scanner/geckoterminal.py` (add these imports at the top of the file, and the new code at the end):

Update the top of `src/scanner/geckoterminal.py` to:

```python
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
```

(The existing `parse_geckoterminal_pool` function from Task 2 stays unchanged below these imports — only the import block and the module-level constants above it change, plus the new code appended below.)

Append this to the end of `src/scanner/geckoterminal.py`:

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_geckoterminal.py -v`
Expected: PASS (8 tests: the original 5 from Task 2 plus the 3 new ones).

- [ ] **Step 6: Run the full test suite**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all 24 tests PASS — breakdown: `test_events.py` 8 (original 6 + 2 added in Task 1), `test_publisher.py` 4 (unchanged), `test_pumpportal.py` 4 (unchanged), `test_geckoterminal.py` 8 (5 from Task 2 + 3 added in this task).

- [ ] **Step 7: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/geckoterminal.py tests/test_geckoterminal.py pyproject.toml uv.lock
git commit -m "feat: add GeckoTerminal polling loop with per-cycle error handling"
```

---

### Task 4: Wire both scanners into main.py

**Files:**
- Modify: `src/scanner/main.py`

**Interfaces:**
- Consumes: `run_pumpportal_scanner` (existing), `run_geckoterminal_scanner` (Task 3).
- Produces: `_main()` now runs both scanner coroutines concurrently.

- [ ] **Step 1: Update main.py**

Replace the full contents of `src/scanner/main.py`:

```python
import asyncio
import json
import logging
import sys
import time

from scanner.config import PUBLISHER_BUFFER_SIZE, load_redis_url
from scanner.geckoterminal import run_geckoterminal_scanner
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
    logger.info("starting scanner, redis_url=%s", redis_url)

    publisher = Publisher(redis_url, buffer_size=PUBLISHER_BUFFER_SIZE)
    try:
        await asyncio.gather(
            run_pumpportal_scanner(publisher),
            run_geckoterminal_scanner(publisher),
        )
    finally:
        await publisher.close()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it imports cleanly**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run python -c "from scanner.main import main; print('import ok')"`
Expected: `import ok`

- [ ] **Step 3: Run the full test suite once more to confirm nothing broke**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all 24 tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/main.py
git commit -m "feat: run PumpPortal and GeckoTerminal scanners concurrently"
```

---

### Task 5: Deploy and verify (Definition of Done)

**Files:** none (deployment + verification only).

- [ ] **Step 1: Sync dependencies and run the suite locally in the conda env**

Run:
```bash
conda activate zetryn-scanner
cd /mnt/data/Project/zetryn/scanner
pip install -e ".[dev]"
pytest -v
```
Expected: all 24 tests PASS (this also picks up the new `httpx` dependency via the `[project.optional-dependencies].dev`/main `dependencies` list already updated in Task 3).

- [ ] **Step 2: Run the scanner locally in the background**

Run:
```bash
cd /mnt/data/Project/zetryn/scanner
REDIS_URL=redis://127.0.0.1:6379 nohup python -m scanner.main > /tmp/scanner-local-source2.log 2>&1 &
```
Expected: process starts; check `redis-cli xlen scanner:events:new_token` before starting as a baseline.

- [ ] **Step 3: Observe for 10+ minutes**

Run (after waiting 10+ minutes):
```bash
redis-cli xlen scanner:events:new_token
redis-cli xrevrange scanner:events:new_token + - COUNT 20 | grep -A1 '"source"' | grep -E 'pumpportal|geckoterminal'
```
Expected: `xlen` count higher than the baseline from Step 2, and both `pumpportal` and `geckoterminal` values appear among recent entries — confirming both sources are contributing to the same stream.

- [ ] **Step 4: Confirm zero crashes**

Run: `tail -30 /tmp/scanner-local-source2.log`
Expected: only `INFO` connect/subscribe logs and, if any, `WARNING` poll-failure logs (which are expected and non-fatal) — no unhandled tracebacks.

- [ ] **Step 5: Deploy to VPS**

Package and copy the updated project to the VPS the same way Phase 1 was deployed (tarball + scp to `/opt/zetryn-scanner`, or `git pull` if the VPS clone is git-based), then on the VPS:
```bash
cd /opt/zetryn-scanner
export PATH="$HOME/.local/bin:$PATH"
uv sync
pm2 restart zetryn-scanner
```
Expected: `pm2 describe zetryn-scanner` shows `status: online` after the restart.

- [ ] **Step 6: Verify on VPS over 10+ minutes**

Same checks as Step 3, run against the VPS via SSH/exec: `redis-cli xlen scanner:events:new_token` before and after a 10+ minute wait, confirming growth and both `source` values present, plus `pm2 describe zetryn-scanner | grep restarts` showing no unexpected restarts.

No commit for this task — it is a deployment and verification checkpoint only.
