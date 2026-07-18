# Scanner Module — Source #2: GeckoTerminal Design

**Status:** Approved
**Date:** 2026-07-18

## Background

Scanner Phase 1 (PumpPortal WS) is live and verified — both on the VPS
(PM2) and locally (conda env `zetryn-scanner`), producing real `new_token`
and `migration` events into Redis Streams. This spec covers adding the
**second** scanner source.

**Long-term vision** (recorded here so it isn't re-litigated): the user
wants every viable provider — free, freetier, and paid — to eventually
have a slot in Scanner, each contributing its own role, mirroring Router's
50-provider catalog with "Free" badges. That vision is intentionally
**not** pursued in one shot: each source gets its own spec → plan →
implementation cycle, one at a time, to avoid the "building everything at
once has failed 6 times" failure mode the user has explicitly flagged.
This spec covers exactly one source: **GeckoTerminal**.

**Why GeckoTerminal, not DexScreener**: the user's original request named
DexScreener, but research (docs.dexscreener.com/api/reference, verified
July 2026) found DexScreener has **no per-chain "new pairs" discovery
endpoint** — only lookup-by-known-address or cross-chain profile/boost
feeds not filterable to Solana and not a reliable signal of "newly
created." GeckoTerminal (apiguide.geckoterminal.com, independent CoinGecko
product) has the endpoint this phase actually needs:
`GET /networks/solana/new_pools`, keyless, 30 requests/minute. DexScreener
remains a candidate for a later *enrichment* role (pulling liquidity/volume
detail for an already-known mint), not discovery — out of scope here.

## Goal

Add a second, independent event source to Scanner: poll GeckoTerminal's
`new_pools` endpoint for Solana, parse newly created pools into the same
`ScannerEvent` schema Phase 1 already defined, and publish them into the
same Redis Streams PumpPortal already writes to — running as a second
`asyncio` task in the same process, with zero coordination between the two
sources beyond sharing a stream.

**Explicitly out of scope for this spec** (deferred):
- Any other scanner source (Helius, Birdeye WS, Shyft, DexScreener-as-
  enrichment, etc.)
- Cross-source deduplication or correlation (e.g. recognizing that a
  PumpPortal `migration` event and a GeckoTerminal `new_pools` event refer
  to the same mint) — this is explicitly deferred to the future
  Strategies/Combos-equivalent aggregation spec, not this one
- Router integration, Strategies page, Scanner dashboard/UI (separate,
  deliberately un-bundled specs per user decision on 2026-07-18)
- Any event types beyond `new_token` (GeckoTerminal's `new_pools` maps to
  this one type only in this phase)

## Schema Change: Open Event Types (Cross-Cutting)

Phase 1 defined `ScannerEvent.event_type: Literal["new_token", "migration"]`
and `.source: Literal["pumpportal"]` — closed enums. The user explicitly
corrected this direction: event types must be open-ended from the start,
not closed to whatever's implemented today ("saya ingin dari awal langsung
bisa semua" — referring to future event types like trades, price updates,
liquidity changes, not just today's two).

**Change**: loosen both fields to plain `str`, backed by a registry of
named constants in `events.py`:

```python
EVENT_TYPE_NEW_TOKEN = "new_token"
EVENT_TYPE_MIGRATION = "migration"

SOURCE_PUMPPORTAL = "pumpportal"
SOURCE_GECKOTERMINAL = "geckoterminal"
```

This is a low-risk change: `publisher.py`'s `_stream_name()` was already
written generically (`f"scanner:events:{event_type}"`, not hardcoded to
two values), so adding a new event type in the future requires zero
publisher changes — confirmed by reading the existing code. Existing
Phase 1 tests keep passing unchanged since the string values themselves
don't change, only the type annotation and the introduction of named
constants in place of literal strings inside `parse_pumpportal_message`.

## Architecture

One new `asyncio` task, `run_geckoterminal_scanner()`, runs **in the same
process** as `run_pumpportal_scanner()`, started via `asyncio.gather` in
`main.py`. No new PM2 app, no new deployment unit — this mirrors the
user's explicit choice to keep operational overhead low for sources that
are already independent at the publish level.

```
GeckoTerminal REST  <--poll 30s-->  geckoterminal scanner task  --\
                                                                    +--> Redis Stream
PumpPortal WS       <--stream-->    pumpportal scanner task    --/       scanner:events:new_token
                                                                          scanner:events:migration
```

Each task owns its own `Publisher` call — no shared mutable state between
the two tasks. If one task's coroutine raises an unhandled exception,
`asyncio.gather` (called without `return_exceptions=True`) would normally
cancel the sibling task too; this spec requires wrapping each task so a
crash in one does not silently kill the other silently (see Error Handling
below).

## Project Structure (additions)

```
scanner/
├── src/scanner/
│   ├── geckoterminal.py      # NEW: polling loop + parser
│   └── main.py                # MODIFIED: run both tasks via asyncio.gather
├── tests/
│   └── test_geckoterminal.py  # NEW
```

## GeckoTerminal API Details (verified via live fetch, July 2026)

- Endpoint: `GET https://api.geckoterminal.com/api/v2/networks/solana/new_pools?include=base_token,quote_token`
- Keyless, public. Rate limit: 30 requests/minute (confirmed via
  support.coingecko.com rate-limit article). Polling every 30 seconds uses
  2 of the 30 slots/minute — well within budget.
- Response is JSON:API format:
  - `data[]` — pool objects. Each has `id` (format `"solana_<pool_address>"`),
    `attributes.pool_created_at` (ISO 8601 UTC, e.g.
    `"2026-07-18T13:56:53Z"`), and `relationships.base_token.data.id` /
    `relationships.quote_token.data.id` (format `"solana_<mint>"`, NOT the
    resolved token details).
  - `included[]` — referenced token (and dex) objects, required via the
    `?include=base_token,quote_token` query param or this array is empty.
    Each token item has `id` matching a relationship id, and
    `attributes.address` (the bare mint address, no prefix).
- **Mint extraction**: resolve `pool["relationships"]["base_token"]["data"]["id"]`
  against a `{item["id"]: item for item in response["included"]}` lookup,
  then read `.attributes.address` from the matched item. Do NOT strip the
  `"solana_"` prefix manually from the relationship id — use the resolved
  `included[]` item's `attributes.address` instead, which is already bare.
- Base vs quote: GeckoTerminal's own ordering convention puts the newly
  created token in `base_token` and the pairing asset (typically SOL,
  `So11111111111111111111111111111111111111112`) in `quote_token`. This
  spec always reads `mint` from `base_token` — no detection logic for the
  reverse case, consistent with not solving problems that haven't been
  observed in real data yet.

## Event Mapping

Every pool returned by `new_pools` maps to `event_type=EVENT_TYPE_NEW_TOKEN`
(unified with PumpPortal's `new_token` stream — both answer "a new
token/pair worth looking at appeared," just via different detection
mechanisms; a future consumer distinguishes origin via the `source` field,
not via a separate stream). `source=SOURCE_GECKOTERMINAL`. `raw` stores the
full original pool object from `data[]` (not the resolved/flattened form)
— consistent with Phase 1's "raw preserves the full original payload"
principle. `received_at` is set the same way as Phase 1: the scanner's own
UTC timestamp at parse time, not `pool_created_at` (which is preserved
inside `raw` for any consumer that wants it).

## Polling Loop & Error Handling

```
loop:
  try:
    response = await http_client.get(NEW_POOLS_URL, params={"include": "base_token,quote_token"})
    response.raise_for_status()
    payload = response.json()
    included_by_id = {item["id"]: item for item in payload.get("included", [])}
    for pool in payload.get("data", []):
      event = parse_geckoterminal_pool(pool, included_by_id)
      if event is None:
        continue
      await publisher.publish(event)
  except Exception as exc:
    log a warning with the exception, do not retry immediately
  wait 30 seconds
  go back to the top of the loop
```

No backoff logic for REST polling failures (unlike PumpPortal's WS
reconnect backoff) — a failed poll is not fatal, it just tries again at
the next 30-second interval. This is a deliberate simplification the user
approved: consistent with "never crash, keep running," but without adding
backoff machinery that a fixed-interval poller doesn't need the way a
persistent WS connection does.

**Task isolation**: `main.py` wraps each scanner task so that an unhandled
exception in one does not cancel the other. Concretely, each task's
top-level coroutine already has its own `try/except Exception` around the
per-poll-cycle body (shown above) so exceptions from a single bad response
never escape the loop; `run_geckoterminal_scanner`, like
`run_pumpportal_scanner`, only exits on cancellation from the outside
(e.g. process shutdown), never on its own from a data or network error.
`main.py` calls both with `asyncio.gather(scanner_a(), scanner_b())`
(default `return_exceptions=False` is fine specifically because neither
coroutine is expected to ever raise past its own internal try/except —
this is a belt-and-suspenders design, not a substitute for the per-loop
error handling above).

## HTTP Client

New dependency: **httpx** (async-native, `requests`-like API). Added to
`pyproject.toml` dependencies alongside `websockets`, `redis`, `pydantic`.

## Testing

- **`test_events.py`** (existing, unchanged behavior): confirms
  `event_type`/`source` as `str` still validate the same known values;
  named constants (`EVENT_TYPE_NEW_TOKEN`, etc.) importable and equal to
  the original literal strings.
- **`test_geckoterminal.py`** (new):
  - Parses a realistic JSON:API fixture (shaped like the verified live
    example in this spec) into a `ScannerEvent` with the correct `mint`
    resolved from `included[]`.
    - Missing `included[]` entry for a relationship → parse function
    returns `None` (never raises).
  - Malformed/partial pool object (missing `relationships`, missing
    `attributes.pool_created_at`, etc.) → returns `None`, never raises.
  - Polling loop test: mocked HTTP client returns a fixed payload once,
    then raises (simulating a network error) — asserts the loop logs and
    continues rather than propagating the exception, using an injectable
    `max_iterations` and `_sleep_fn` the same way `test_pumpportal.py`
    already does for its reconnect loop.

**Manual integration test** (after unit tests are green): run the process
locally (conda env `zetryn-scanner`) or on the VPS for several minutes;
verify via `redis-cli XLEN scanner:events:new_token` that the count
increases faster than PumpPortal alone would produce, and spot-check a few
entries have `source: "geckoterminal"` via `redis-cli XRANGE`.

## Deployment

No new PM2 app. The existing `zetryn-scanner` PM2 process picks up the
new task automatically once `main.py` is updated to `asyncio.gather` both
scanners — a normal `pm2 restart zetryn-scanner` (or redeploy) is all
that's needed on the VPS.

## Definition of Done

The process runs continuously (VPS and/or local), both tasks stay alive
(GeckoTerminal task survives individual poll failures; PumpPortal task
keeps its existing reconnect behavior), and `redis-cli XLEN
scanner:events:new_token` shows entries with `source: "geckoterminal"`
interleaved with `source: "pumpportal"` entries over a 10+ minute
observation window, with zero process crashes.

## Out of Scope (Deferred)

- Cross-source deduplication/correlation (Strategies aggregation spec)
- Additional scanner sources (Helius, Birdeye WS, Shyft, DexScreener
  enrichment)
- Router integration, Strategies page, Scanner dashboard/UI
- Any event type beyond `new_token` for this source
- Rate-limit-aware adaptive polling (e.g. slowing down automatically on
  429) — a fixed 30s interval is far enough under the 30 req/min limit
  that this isn't needed yet
