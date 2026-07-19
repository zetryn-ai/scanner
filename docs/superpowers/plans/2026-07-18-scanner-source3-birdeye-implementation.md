# Scanner Source #3 — Birdeye Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third, independent scanner source — Birdeye's Solana `new_listing` REST endpoint, the first key-required source — running as a third `asyncio` task alongside PumpPortal and GeckoTerminal, publishing into the same Redis Streams.

**Architecture:** A new `run_birdeye_scanner()` coroutine checks for `BIRDEYE_API_KEY` at startup (skipping entirely if unset), then polls `GET https://public-api.birdeye.so/defi/v2/tokens/new_listing?chain=solana&limit=5` every 30 seconds with `X-API-KEY`/`x-chain` headers, parsing each item directly (no JSON:API resolution needed — simpler than GeckoTerminal since the mint address is a top-level field). `main.py` runs it as a third member of the existing `asyncio.gather` call.

**Tech Stack:** Python 3.12+, `httpx` (already a dependency from source #2), existing `pydantic`/`redis`/`pytest`/`pytest-asyncio` stack.

## Global Constraints

- Birdeye endpoint requires `X-API-KEY` and `x-chain: solana` headers; read the key from the `BIRDEYE_API_KEY` environment variable (Router integration is explicitly out of scope — deferred to a future spec).
- If `BIRDEYE_API_KEY` is unset/empty, `run_birdeye_scanner()` must log one INFO line and return immediately — it must never enter the polling loop or attempt any HTTP call in that case.
- Response shape is simpler than GeckoTerminal's: the mint address is directly on each item as `address` — no `included[]` resolution needed.
- Verified live rate limit for the user's own key: `x-ratelimit-limit: 100` per ~60-second window. Polling every 30 seconds uses roughly 2 of 100 slots — do not poll faster without re-verifying.
- No fallback endpoint if `new_listing` fails — the old `tokenlist` fallback from the `bot/` reference no longer exists (`{"success": false, "message": "Not found"}` when tested live). A failed poll cycle is logged and skipped, same as GeckoTerminal's error handling — no backoff, no secondary endpoint.
- Every item maps to `event_type=EVENT_TYPE_NEW_TOKEN`, `source=SOURCE_BIRDEYE` (new constant `"birdeye"`) — not to be confused with Birdeye's own per-item `source` field (e.g. `"pump_amm"`), which is preserved untouched inside `raw`.
- `ai-agent` and `bot` repositories remain frozen reference-only — do not import from them, do not modify them.
- Definition of done: process runs continuously (local and/or VPS) with `BIRDEYE_API_KEY` set, `redis-cli XLEN scanner:events:new_token` shows entries with `source: "birdeye"` interleaved with the other two sources over a 10+ minute window, zero crashes.

---

## File Structure (additions/changes)

```
scanner/
├── .env.example                 # MODIFIED: add BIRDEYE_API_KEY=
├── src/scanner/
│   ├── config.py                # MODIFIED: add load_birdeye_api_key(), BIRDEYE_NEW_LISTING_URL, BIRDEYE_POLL_INTERVAL_SECONDS
│   ├── events.py                # MODIFIED: add SOURCE_BIRDEYE constant
│   ├── birdeye.py               # NEW: parser + polling loop
│   └── main.py                  # MODIFIED: add run_birdeye_scanner to asyncio.gather
├── tests/
│   └── test_birdeye.py          # NEW
```

---

### Task 1: Config and event-source constant

**Files:**
- Modify: `src/scanner/config.py`
- Modify: `src/scanner/events.py`
- Modify: `.env.example`

**Interfaces:**
- Produces: `SOURCE_BIRDEYE = "birdeye"` in `scanner.events`. `load_birdeye_api_key() -> str | None`, `BIRDEYE_NEW_LISTING_URL: str`, `BIRDEYE_POLL_INTERVAL_SECONDS: float` in `scanner.config`.

- [ ] **Step 1: Add SOURCE_BIRDEYE to events.py**

In `src/scanner/events.py`, change:
```python
SOURCE_PUMPPORTAL = "pumpportal"
SOURCE_GECKOTERMINAL = "geckoterminal"
```
to:
```python
SOURCE_PUMPPORTAL = "pumpportal"
SOURCE_GECKOTERMINAL = "geckoterminal"
SOURCE_BIRDEYE = "birdeye"
```

- [ ] **Step 2: Add Birdeye config to config.py**

In `src/scanner/config.py`, append at the end of the file:
```python

BIRDEYE_NEW_LISTING_URL = "https://public-api.birdeye.so/defi/v2/tokens/new_listing"
BIRDEYE_POLL_INTERVAL_SECONDS = 30.0


def load_birdeye_api_key() -> str | None:
    return os.environ.get("BIRDEYE_API_KEY") or None
```

- [ ] **Step 3: Update .env.example**

Change `.env.example` from:
```
PUMPPORTAL_API_KEY=
REDIS_URL=redis://127.0.0.1:6379
```
to:
```
PUMPPORTAL_API_KEY=
BIRDEYE_API_KEY=
REDIS_URL=redis://127.0.0.1:6379
```

- [ ] **Step 4: Verify everything still imports and existing tests pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all 24 existing tests PASS (no test changes in this task, just new constants/functions with no consumers yet).

- [ ] **Step 5: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/config.py src/scanner/events.py .env.example
git commit -m "feat: add Birdeye config (API key loader, endpoint URL, poll interval)"
```

---

### Task 2: Birdeye parser

**Files:**
- Create: `src/scanner/birdeye.py` (parser only in this task; polling loop added in Task 3)
- Test: `tests/test_birdeye.py`

**Interfaces:**
- Consumes: `ScannerEvent`, `EVENT_TYPE_NEW_TOKEN`, `SOURCE_BIRDEYE` (Task 1).
- Produces: `def parse_birdeye_token(item: object) -> ScannerEvent | None` — reads `mint` directly from `item["address"]`, returns `None` (never raises) on any missing/malformed field.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_birdeye.py`:

```python
from datetime import datetime

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_BIRDEYE
from scanner.birdeye import parse_birdeye_token

# Shape verified via a live curl call using the user's own free-tier key to
# https://public-api.birdeye.so/defi/v2/tokens/new_listing?chain=solana&limit=5
# (July 2026).
SAMPLE_ITEM = {
    "address": "2yGGhJ9AyQsgHTwWHCpcqMhMMmv5QE7JT3D5jAvR4dAj",
    "symbol": "Pedro",
    "name": "Pedro Pedro Pedro",
    "decimals": 6,
    "source": "pump_amm",
    "liquidityAddedAt": "2026-07-18T15:38:04",
    "logoURI": "https://ipfs.io/ipfs/Qmd4vS5KChLix3JAg2UPAmLNGZBHAVJVDRbkk43gdfLp1a",
    "liquidity": 17215.636907934913,
}


def test_parse_valid_item_reads_mint_directly():
    event = parse_birdeye_token(SAMPLE_ITEM)
    assert event is not None
    assert event.event_type == EVENT_TYPE_NEW_TOKEN
    assert event.source == SOURCE_BIRDEYE
    assert event.mint == "2yGGhJ9AyQsgHTwWHCpcqMhMMmv5QE7JT3D5jAvR4dAj"
    assert event.raw == SAMPLE_ITEM
    parsed = datetime.fromisoformat(event.received_at)
    assert parsed.tzinfo is not None


def test_parse_missing_address_returns_none():
    item = {"symbol": "NoAddress", "name": "No Address Field"}
    assert parse_birdeye_token(item) is None


def test_parse_non_string_address_returns_none():
    item = {"address": 12345, "symbol": "BadAddress"}
    assert parse_birdeye_token(item) is None


def test_parse_empty_address_returns_none():
    item = {"address": "", "symbol": "EmptyAddress"}
    assert parse_birdeye_token(item) is None


def test_parse_non_dict_item_returns_none():
    assert parse_birdeye_token("not a dict") is None
    assert parse_birdeye_token(None) is None
    assert parse_birdeye_token([1, 2, 3]) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_birdeye.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.birdeye'`

- [ ] **Step 3: Implement the parser**

Create `src/scanner/birdeye.py`:

```python
from datetime import datetime, timezone

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_BIRDEYE, ScannerEvent


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_birdeye.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/birdeye.py tests/test_birdeye.py
git commit -m "feat: add Birdeye new_listing parser"
```

---

### Task 3: Birdeye polling loop with missing-key skip

**Files:**
- Modify: `src/scanner/birdeye.py`
- Modify: `tests/test_birdeye.py`

**Interfaces:**
- Consumes: `parse_birdeye_token` (Task 2); `load_birdeye_api_key`, `BIRDEYE_NEW_LISTING_URL`, `BIRDEYE_POLL_INTERVAL_SECONDS` (Task 1); `Publisher.publish` (existing, from `scanner.publisher`).
- Produces: `async def run_birdeye_scanner(publisher: Publisher, *, api_key: str | None | object = _UNSET, http_get_fn=None, max_iterations: int | None = None, _sleep_fn=_default_sleep) -> None`. `api_key` defaults to a sentinel meaning "read from `load_birdeye_api_key()`"; tests pass an explicit `str` or `None` to control the missing-key path without touching environment variables. `http_get_fn` is an injectable async callable `(url: str, headers: dict, params: dict) -> httpx.Response`-like object with `.raise_for_status()` and `.json()`, defaulting to a real `httpx.AsyncClient` call in production. `max_iterations` bounds the loop for testing, same pattern as `run_geckoterminal_scanner`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_birdeye.py`:

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


async def test_run_birdeye_scanner_skips_entirely_without_api_key():
    call_count = 0

    async def fake_http_get_fn(_url, _headers, _params):
        nonlocal call_count
        call_count += 1
        return _FakeResponse({"success": True, "data": {"items": []}})

    publisher = _RecordingPublisher()

    from scanner.birdeye import run_birdeye_scanner

    await run_birdeye_scanner(
        publisher,
        api_key=None,
        http_get_fn=fake_http_get_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert call_count == 0
    assert publisher.published == []


async def test_run_birdeye_scanner_publishes_parsed_items():
    payload = {"success": True, "data": {"items": [SAMPLE_ITEM]}}

    async def fake_http_get_fn(_url, _headers, _params):
        return _FakeResponse(payload)

    publisher = _RecordingPublisher()

    from scanner.birdeye import run_birdeye_scanner

    await run_birdeye_scanner(
        publisher,
        api_key="fake-test-key",
        http_get_fn=fake_http_get_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert len(publisher.published) == 1
    assert publisher.published[0].mint == "2yGGhJ9AyQsgHTwWHCpcqMhMMmv5QE7JT3D5jAvR4dAj"


async def test_run_birdeye_scanner_continues_after_http_error():
    call_count = 0

    async def fake_http_get_fn(_url, _headers, _params):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _FakeResponse({}, status_ok=False)
        return _FakeResponse({"success": True, "data": {"items": [SAMPLE_ITEM]}})

    publisher = _RecordingPublisher()

    from scanner.birdeye import run_birdeye_scanner

    await run_birdeye_scanner(
        publisher,
        api_key="fake-test-key",
        http_get_fn=fake_http_get_fn,
        max_iterations=2,
        _sleep_fn=_no_op_sleep,
    )

    assert call_count == 2
    assert len(publisher.published) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_birdeye.py -v`
Expected: FAIL — `ImportError: cannot import name 'run_birdeye_scanner' from 'scanner.birdeye'`

- [ ] **Step 3: Implement the polling loop**

Update the top of `src/scanner/birdeye.py` to:

```python
import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

import httpx

from scanner.config import (
    BIRDEYE_NEW_LISTING_URL,
    BIRDEYE_POLL_INTERVAL_SECONDS,
    load_birdeye_api_key,
)
from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_BIRDEYE, ScannerEvent
from scanner.publisher import Publisher

logger = logging.getLogger("scanner.birdeye")

_UNSET = object()
```

(The existing `parse_birdeye_token` function from Task 2 stays unchanged below these imports.)

Append this to the end of `src/scanner/birdeye.py`:

```python
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
    """Poll Birdeye's Solana new_listing endpoint every
    BIRDEYE_POLL_INTERVAL_SECONDS, publishing a ScannerEvent per item.

    If no API key is available (either the `api_key` param is None, or it
    is left at its default and load_birdeye_api_key() returns None), this
    logs one INFO line and returns immediately — no HTTP call is ever
    attempted and the polling loop never runs.

    A failed poll cycle (HTTP error, malformed JSON, etc.) is logged and
    skipped — the loop always waits for the next interval and keeps
    running, it never raises out of this function on a bad response.

    max_iterations bounds the number of poll cycles for testing; it is
    None (unbounded) in production.
    """
    resolved_key = load_birdeye_api_key() if api_key is _UNSET else api_key
    if not resolved_key:
        logger.info("Birdeye disabled: no API key")
        return

    http_get_fn = http_get_fn or _default_http_get_fn
    headers = {"X-API-KEY": resolved_key, "x-chain": "solana"}
    iterations = 0

    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        try:
            response = await http_get_fn(BIRDEYE_NEW_LISTING_URL, headers, {"chain": "solana", "limit": 5})
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_birdeye.py -v`
Expected: PASS (8 tests: the original 5 from Task 2 plus the 3 new ones).

- [ ] **Step 5: Run the full test suite**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all 32 tests PASS — breakdown: `test_events.py` 8, `test_publisher.py` 4, `test_pumpportal.py` 4, `test_geckoterminal.py` 8, `test_birdeye.py` 8.

- [ ] **Step 6: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/birdeye.py tests/test_birdeye.py
git commit -m "feat: add Birdeye polling loop with missing-key skip and per-cycle error handling"
```

---

### Task 4: Wire Birdeye into main.py

**Files:**
- Modify: `src/scanner/main.py`

**Interfaces:**
- Consumes: `run_pumpportal_scanner`, `run_geckoterminal_scanner` (existing), `run_birdeye_scanner` (Task 3).
- Produces: `_main()` now runs all three scanner coroutines concurrently.

- [ ] **Step 1: Update main.py**

Replace the full contents of `src/scanner/main.py`:

```python
import asyncio
import json
import logging
import sys
import time

from scanner.birdeye import run_birdeye_scanner
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
            run_birdeye_scanner(publisher),
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
Expected: all 32 tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/main.py
git commit -m "feat: run PumpPortal, GeckoTerminal, and Birdeye scanners concurrently"
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
Expected: all 32 tests PASS.

- [ ] **Step 2: Add BIRDEYE_API_KEY to the local .env and run the scanner**

Add the user's own free-tier Birdeye key to a local `.env` file (not committed — `.env` is already gitignored):
```bash
cd /mnt/data/Project/zetryn/scanner
echo "BIRDEYE_API_KEY=<the user's actual key>" >> .env
```

Check the baseline: `redis-cli xlen scanner:events:new_token`

Run:
```bash
cd /mnt/data/Project/zetryn/scanner
set -a && . ./.env && set +a
nohup python -m scanner.main > /tmp/scanner-local-source3.log 2>&1 &
```

- [ ] **Step 3: Observe for 10+ minutes**

Run (after waiting 10+ minutes):
```bash
redis-cli xlen scanner:events:new_token
redis-cli xrevrange scanner:events:new_token + - COUNT 30 | grep -A1 '"source"' | grep -oE 'pumpportal|geckoterminal|birdeye' | sort | uniq -c
```
Expected: `xlen` count higher than the baseline from Step 2, and the `uniq -c` output shows all three source values (`pumpportal`, `geckoterminal`, `birdeye`) present among recent entries.

- [ ] **Step 4: Confirm zero crashes**

Run: `tail -30 /tmp/scanner-local-source3.log`
Expected: only `INFO` connect/poll logs and, if any, `WARNING` poll-failure logs (expected and non-fatal) — no unhandled tracebacks. Confirm no `"Birdeye disabled: no API key"` line appears (since the key was set in Step 2) — if it does appear, the `.env` wasn't loaded correctly and Step 2 needs to be redone before proceeding.

- [ ] **Step 5: Deploy to VPS**

Copy the updated project to the VPS the same way sources #1 and #2 were deployed (tarball + scp to `/opt/zetryn-scanner`), then on the VPS:
```bash
cd /opt/zetryn-scanner
export PATH="$HOME/.local/bin:$PATH"
uv sync
```

Add `BIRDEYE_API_KEY` to the VPS's existing `.env` file (append, do not overwrite `REDIS_URL`):
```bash
echo "BIRDEYE_API_KEY=<the user's actual key>" >> .env
```

Restart:
```bash
pm2 restart zetryn-scanner
```
Expected: `pm2 describe zetryn-scanner` shows `status: online` after the restart.

- [ ] **Step 6: Verify on VPS over 10+ minutes**

Same checks as Step 3, run against the VPS: `redis-cli xlen scanner:events:new_token` before and after a 10+ minute wait, confirming growth and all three `source` values present, plus `pm2 describe zetryn-scanner | grep restarts` showing no unexpected restarts beyond the one from this deploy.

No commit for this task — it is a deployment and verification checkpoint only.
