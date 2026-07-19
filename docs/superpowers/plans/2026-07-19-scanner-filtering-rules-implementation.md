# Scanner Filtering/Rules (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Scanner's first consumer — a separate process that reads raw events from the scanner streams via a Redis consumer group, applies named declarative filter rules ("strategies"), and writes matching events to per-strategy Redis streams.

**Architecture:** A new `zetryn-scanner-filter` PM2 process (`python -m scanner.filter_main`) reads `scanner:events:new_token`/`scanner:events:migration` with `XREADGROUP` (group `scanner-filter`), checks each event against strategies loaded from a JSON config, and `XADD`s matches to `scanner:strategy:<name>`, then `XACK`s. Strategies filter only the uniform top-level fields (`source`, `event_type`) in this phase — matching is pure per-event logic with no I/O or state.

**Tech Stack:** Python 3.12+, existing `redis`/`pydantic`/`pytest`/`pytest-asyncio`/`fakeredis` stack — no new dependencies.

## Global Constraints

- Filtering runs as a SEPARATE process/PM2 app (`zetryn-scanner-filter`), never merged into the producer process — producer/consumer isolation.
- Reads use `XREADGROUP` with consumer group `scanner-filter`, consumer name `filter-1`; the group is created with `XGROUP CREATE ... MKSTREAM`, ignoring the `BUSYGROUP` error on restart.
- Every consumed entry is `XACK`-ed after processing — including no-match events and events that errored (logged and skipped) — so one bad event never blocks the stream.
- Phase 1 filters ONLY top-level uniform fields `source` and `event_type`. No numeric/`raw`-field filters (those need per-source normalization — a later phase).
- Strategy `name` is restricted to `[a-z0-9_-]+` (safe as a Redis key segment); an invalid name is a config error logged and skipped at load time, never a crash.
- Output stream naming: `scanner:strategy:<name>`, derived from `strategy.name`.
- Config is read from `STRATEGIES_CONFIG_PATH` (default `./strategies.json`); a missing/empty/unparseable file yields `[]` strategies and the process idles (stays alive, reads+acks, forwards nothing) rather than crashing.
- Stream field format matches the producer's `publisher.py`: fields `event_type`, `source`, `mint`, `received_at`, `raw` (where `raw` is a JSON string). Deserialization reverses this via `json.loads` on `raw`.
- `ai-agent` and `bot` repositories remain frozen reference-only.
- Definition of done: producer + filter both running with a `strategies.json` defining ≥1 strategy, `redis-cli XLEN scanner:strategy:<name>` increases for matching events over 10+ minutes, raw stream keeps growing, consumer-group pending count stays low, neither process crashes.

---

## File Structure (additions/changes)

```
scanner/
├── strategies.example.json        # NEW: example rule config
├── .env.example                   # MODIFIED: add STRATEGIES_CONFIG_PATH
├── src/scanner/
│   ├── strategies.py              # NEW: Strategy model + event_matches_strategy
│   ├── strategy_config.py         # NEW: load_strategies(path)
│   ├── filter_runner.py           # NEW: run_filter consumer-group loop
│   └── filter_main.py             # NEW: entrypoint
├── tests/
│   ├── test_strategies.py         # NEW
│   ├── test_strategy_config.py    # NEW
│   └── test_filter_runner.py      # NEW
├── ecosystem.config.js            # MODIFIED: add zetryn-scanner-filter app
```

---

### Task 1: Strategy model + matching logic

**Files:**
- Create: `src/scanner/strategies.py`
- Test: `tests/test_strategies.py`

**Interfaces:**
- Consumes: `ScannerEvent` (from `scanner.events`).
- Produces: `class Strategy(BaseModel)` with fields `name: str`, `source_allowlist: list[str] | None = None`, `event_types: list[str] | None = None` (with `name` validated against `^[a-z0-9_-]+$`); `def event_matches_strategy(event: ScannerEvent, strategy: Strategy) -> bool`; `def strategy_stream_name(strategy: Strategy) -> str` returning `f"scanner:strategy:{strategy.name}"`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_strategies.py`:
```python
import pytest
from pydantic import ValidationError

from scanner.events import ScannerEvent
from scanner.strategies import Strategy, event_matches_strategy, strategy_stream_name


def make_event(source="pumpportal", event_type="new_token") -> ScannerEvent:
    return ScannerEvent(
        event_type=event_type,
        source=source,
        mint="MINTxxx",
        raw={},
        received_at="2026-07-19T00:00:00+00:00",
    )


def test_no_filters_matches_anything():
    s = Strategy(name="everything")
    assert event_matches_strategy(make_event(), s) is True
    assert event_matches_strategy(make_event(source="birdeye", event_type="migration"), s) is True


def test_source_allowlist_hit_and_miss():
    s = Strategy(name="birdeye-only", source_allowlist=["birdeye"])
    assert event_matches_strategy(make_event(source="birdeye"), s) is True
    assert event_matches_strategy(make_event(source="pumpportal"), s) is False


def test_event_types_hit_and_miss():
    s = Strategy(name="launches", event_types=["new_token"])
    assert event_matches_strategy(make_event(event_type="new_token"), s) is True
    assert event_matches_strategy(make_event(event_type="migration"), s) is False


def test_both_conditions_are_anded():
    s = Strategy(name="bd-launch", source_allowlist=["birdeye"], event_types=["new_token"])
    assert event_matches_strategy(make_event(source="birdeye", event_type="new_token"), s) is True
    # right source, wrong type
    assert event_matches_strategy(make_event(source="birdeye", event_type="migration"), s) is False
    # wrong source, right type
    assert event_matches_strategy(make_event(source="pumpportal", event_type="new_token"), s) is False


def test_stream_name_derivation():
    assert strategy_stream_name(Strategy(name="sniper")) == "scanner:strategy:sniper"


def test_valid_names_accepted():
    for name in ["sniper", "birdeye-only", "launch_2", "abc123"]:
        assert Strategy(name=name).name == name


def test_invalid_names_rejected():
    for name in ["Sniper", "has space", "bad/slash", "colon:name", ""]:
        with pytest.raises(ValidationError):
            Strategy(name=name)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_strategies.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.strategies'`

- [ ] **Step 3: Implement strategies.py**

Create `src/scanner/strategies.py`:
```python
import re

from pydantic import BaseModel, field_validator

from scanner.events import ScannerEvent

_NAME_RE = re.compile(r"^[a-z0-9_-]+$")


class Strategy(BaseModel):
    name: str
    source_allowlist: list[str] | None = None
    event_types: list[str] | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError(f"strategy name must match [a-z0-9_-]+, got {v!r}")
        return v


def event_matches_strategy(event: ScannerEvent, strategy: Strategy) -> bool:
    """Pure per-event pass/reject. A None field means 'don't filter on it'
    (match anything). Both conditions are AND-ed."""
    if strategy.source_allowlist is not None and event.source not in strategy.source_allowlist:
        return False
    if strategy.event_types is not None and event.event_type not in strategy.event_types:
        return False
    return True


def strategy_stream_name(strategy: Strategy) -> str:
    return f"scanner:strategy:{strategy.name}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_strategies.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/strategies.py tests/test_strategies.py
./scripts/commit-as.sh random "feat: add Strategy model and per-event matching logic"
```

---

### Task 2: Config loading

**Files:**
- Create: `src/scanner/strategy_config.py`
- Create: `strategies.example.json`
- Modify: `.env.example`
- Test: `tests/test_strategy_config.py`

**Interfaces:**
- Consumes: `Strategy` (Task 1).
- Produces: `def load_strategies(path: str) -> list[Strategy]` — returns `[]` for a missing/empty/unparseable file; skips malformed individual entries (logging them) while keeping valid ones.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_strategy_config.py`:
```python
import json

from scanner.strategy_config import load_strategies


def test_load_valid_config(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text(json.dumps([
        {"name": "birdeye-only", "source_allowlist": ["birdeye"]},
        {"name": "launches", "event_types": ["new_token"]},
        {"name": "everything"},
    ]))
    strategies = load_strategies(str(p))
    assert [s.name for s in strategies] == ["birdeye-only", "launches", "everything"]
    assert strategies[0].source_allowlist == ["birdeye"]


def test_missing_file_returns_empty(tmp_path):
    assert load_strategies(str(tmp_path / "does-not-exist.json")) == []


def test_empty_list_returns_empty(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text("[]")
    assert load_strategies(str(p)) == []


def test_malformed_entry_is_skipped_valid_ones_kept(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text(json.dumps([
        {"name": "good"},
        {"name": "Bad Name With Spaces"},   # invalid name -> skipped
        {"source_allowlist": ["birdeye"]},   # missing name -> skipped
        {"name": "also-good"},
    ]))
    strategies = load_strategies(str(p))
    assert [s.name for s in strategies] == ["good", "also-good"]


def test_completely_unparseable_file_returns_empty(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text("this is not json {{{")
    assert load_strategies(str(p)) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_strategy_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.strategy_config'`

- [ ] **Step 3: Implement strategy_config.py**

Create `src/scanner/strategy_config.py`:
```python
import json
import logging
import os

from pydantic import ValidationError

from scanner.strategies import Strategy

logger = logging.getLogger("scanner.strategy_config")


def load_strategies(path: str) -> list[Strategy]:
    """Load strategy definitions from a JSON file (a list of objects).

    Returns [] for a missing, empty, or unparseable file. Individual
    malformed entries are logged and skipped; valid entries still load.
    Never raises — a bad config idles the filter, it doesn't crash it.
    """
    if not os.path.exists(path):
        logger.info("strategies config not found at %s, no strategies configured", path)
        return []

    try:
        with open(path) as f:
            data = json.load(f)
    except (ValueError, OSError) as exc:
        logger.warning("failed to read strategies config %s (%s), treating as empty", path, exc)
        return []

    if not isinstance(data, list):
        logger.warning("strategies config %s is not a list, treating as empty", path)
        return []

    strategies: list[Strategy] = []
    for entry in data:
        try:
            strategies.append(Strategy(**entry))
        except (ValidationError, TypeError) as exc:
            logger.warning("skipping malformed strategy entry %r (%s)", entry, exc)
    return strategies
```

- [ ] **Step 4: Create strategies.example.json and update .env.example**

Create `strategies.example.json`:
```json
[
  {"name": "birdeye-only", "source_allowlist": ["birdeye"]},
  {"name": "launches", "event_types": ["new_token"]},
  {"name": "everything"}
]
```

Change `.env.example` from:
```
PUMPPORTAL_API_KEY=
ROUTER_API_KEY=
REDIS_URL=redis://127.0.0.1:6379
```
to:
```
PUMPPORTAL_API_KEY=
ROUTER_API_KEY=
REDIS_URL=redis://127.0.0.1:6379
STRATEGIES_CONFIG_PATH=./strategies.json
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_strategy_config.py -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/strategy_config.py tests/test_strategy_config.py strategies.example.json .env.example
./scripts/commit-as.sh random "feat: add strategy config loader with example config"
```

---

### Task 3: Filter runner (consumer-group loop)

**Files:**
- Create: `src/scanner/filter_runner.py`
- Test: `tests/test_filter_runner.py`

**Interfaces:**
- Consumes: `Strategy`, `event_matches_strategy`, `strategy_stream_name` (Task 1); `ScannerEvent` (from `scanner.events`).
- Produces: `async def run_filter(redis_client, strategies: list[Strategy], *, source_streams: tuple[str, ...] = ("scanner:events:new_token", "scanner:events:migration"), group: str = "scanner-filter", consumer: str = "filter-1", block_ms: int = 5000, max_batches: int | None = None) -> None`. `redis_client` is an async redis client (real `redis.asyncio` in production, `fakeredis.aioredis` in tests). `max_batches` bounds the loop for testing (None = forever).
- Also produces: `def _fields_to_event(fields: dict) -> ScannerEvent | None` — rebuild a ScannerEvent from stream fields (reverse of publisher's `_event_to_fields`), returning None on malformed data.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_filter_runner.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_filter_runner.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scanner.filter_runner'`

- [ ] **Step 3: Implement filter_runner.py**

Create `src/scanner/filter_runner.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest tests/test_filter_runner.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full test suite**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all tests pass (33 previous + 7 strategies + 5 config + 4 filter_runner = 49 total).

- [ ] **Step 6: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/filter_runner.py tests/test_filter_runner.py
./scripts/commit-as.sh random "feat: add consumer-group filter runner"
```

---

### Task 4: Filter entrypoint

**Files:**
- Create: `src/scanner/filter_main.py`

**Interfaces:**
- Consumes: `load_strategies` (Task 2), `run_filter` (Task 3), `load_redis_url` (from `scanner.config`).
- Produces: a runnable module (`python -m scanner.filter_main`) with the same JSON-lines logging as `main.py`.

- [ ] **Step 1: Implement filter_main.py**

Create `src/scanner/filter_main.py`:
```python
import asyncio
import json
import logging
import os
import sys
import time

import redis.asyncio as redis

from scanner.config import load_redis_url
from scanner.filter_runner import run_filter
from scanner.strategy_config import load_strategies


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
    logger = logging.getLogger("scanner.filter_main")

    redis_url = load_redis_url()
    config_path = os.environ.get("STRATEGIES_CONFIG_PATH", "./strategies.json")
    strategies = load_strategies(config_path)
    logger.info("starting filter, redis_url=%s, strategies=%d", redis_url, len(strategies))

    redis_client = redis.from_url(redis_url)
    try:
        await run_filter(redis_client, strategies)
    finally:
        await redis_client.aclose()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it imports cleanly**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run python -c "from scanner.filter_main import main; print('import ok')"`
Expected: `import ok`

- [ ] **Step 3: Run the full test suite to confirm nothing broke**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all 49 tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/filter_main.py
./scripts/commit-as.sh random "feat: add filter entrypoint with JSON-lines logging"
```

---

### Task 5: PM2 app + deploy + verify (Definition of Done)

**Files:**
- Modify: `ecosystem.config.js`

- [ ] **Step 1: Add the filter app to ecosystem.config.js**

Replace the full contents of `ecosystem.config.js`:
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
    {
      name: 'zetryn-scanner-filter',
      script: '.venv/bin/python',
      args: '-m scanner.filter_main',
      cwd: __dirname,
      env: {
        REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
        STRATEGIES_CONFIG_PATH: process.env.STRATEGIES_CONFIG_PATH || './strategies.json',
      },
      instances: 1,
      autorestart: true,
    },
  ],
}
```

- [ ] **Step 2: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add ecosystem.config.js
./scripts/commit-as.sh random "feat: add zetryn-scanner-filter PM2 app"
```

- [ ] **Step 3: Run the filter locally against a real strategies.json**

```bash
conda activate zetryn-scanner
cd /mnt/data/Project/zetryn/scanner
pip install -e ".[dev]"
cp strategies.example.json strategies.json
REDIS_URL=redis://127.0.0.1:6379 STRATEGIES_CONFIG_PATH=./strategies.json nohup python -m scanner.filter_main > /tmp/scanner-filter-local.log 2>&1 &
```
(The producer `python -m scanner.main` should also be running locally — start it the same way if it isn't, so there are raw events to filter.)

Check the log within ~15 seconds:
```bash
tail -10 /tmp/scanner-filter-local.log
```
Expected: an INFO line `starting filter, redis_url=..., strategies=3`, no tracebacks.

- [ ] **Step 4: Verify matches land in strategy streams**

Run (after a minute or two of the producer running):
```bash
redis-cli xlen scanner:strategy:everything
redis-cli xlen scanner:strategy:birdeye-only
redis-cli xlen scanner:strategy:launches
redis-cli xrevrange scanner:strategy:birdeye-only + - COUNT 2 | grep -A1 '"source"'
```
Expected: `everything` count > 0 and growing, `birdeye-only` entries all have `source: birdeye`, `launches` entries all `event_type: new_token`. Confirm the consumer-group pending count is low:
```bash
redis-cli xpending scanner:events:new_token scanner-filter
```
Expected: a small/zero pending count (events are being acked).

- [ ] **Step 5: Deploy to VPS**

Copy the updated project to the VPS (tarball + scp to `/opt/zetryn-scanner`, same pattern as prior deploys), then on the VPS:
```bash
cd /opt/zetryn-scanner
export PATH="$HOME/.local/bin:$PATH"
uv sync
cp strategies.example.json strategies.json   # or author a custom one
```

Start the new app (the existing `zetryn-scanner` app is untouched):
```bash
set -a && . ./.env && set +a
pm2 start ecosystem.config.js
pm2 save
```
(`pm2 start ecosystem.config.js` starts any not-yet-running apps in the file — it will bring up `zetryn-scanner-filter` without disturbing the already-online `zetryn-scanner`. Verify both are online: `pm2 list`.)

- [ ] **Step 6: Verify on VPS over 10+ minutes**

```bash
redis-cli xlen scanner:strategy:everything   # baseline
# wait 10+ minutes
redis-cli xlen scanner:strategy:everything   # should be higher
redis-cli xlen scanner:events:new_token      # raw stream still growing (producer unaffected)
redis-cli xpending scanner:events:new_token scanner-filter   # low pending
pm2 describe zetryn-scanner-filter | grep -E "status|restarts"
```
Expected: strategy stream growing, raw stream still growing, low pending count, `status: online`, no unexpected restarts.

No commit for this task beyond the ecosystem.config.js change already committed — Steps 3-6 are deployment and verification.
