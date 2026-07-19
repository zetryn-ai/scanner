# Scanner Module — Filtering/Rules (Phase 1) Design

**Status:** Approved
**Date:** 2026-07-19

## Background

Scanner has three live sources (PumpPortal WS, GeckoTerminal REST,
Birdeye REST via Router proxy), all *producers* writing raw events into
Redis Streams. Nothing reads those streams yet — Scanner has only ever
written, never consumed.

This spec introduces Scanner's **first consumer**: a Filtering/Rules
layer. The user asked for filtering/rules/enrich as a standalone feature
that the future Strategies/Combos aggregation will build on top of rather
than reimplement. This mirrors the OLD `bot/` module's proven
architecture (read-only reference, not modified), which splits `Scanner`
(raw event stream) from `TokenEnricher` (on-demand enrichment) as two
distinct Protocols — so making filter/enrich a first-class layer follows
a battle-tested pattern.

**Sequencing decision** (recorded so it isn't re-litigated): the broader
"filtering/rules/enrich" idea is split by complexity and dependency into
three separate specs, built in order — (1) **Filtering/Rules** (this
spec: pure per-event pass/reject logic, no I/O, no state), (2) **Enrich**
(augment events via external API calls — I/O-heavy, quota-sensitive;
deferred because it likely only enriches events that already passed
filtering, so filtering is its natural prerequisite; `bot/` proved a
prefilter cut 92% of events before enrichers ran), (3) **Strategies/
Combos** (dedup + cross-source correlation — most complex, needs state +
time windows, built last, on top of the first two). This spec is only #1.

## Goal

Read raw events from the scanner streams, apply named declarative rules
("strategies"), and write matching events to per-strategy Redis streams —
so a downstream consumer can subscribe to only the events a given
strategy cares about, instead of the full raw firehose.

**Explicitly out of scope for this phase** (deferred):
- Numeric filters (min liquidity, market cap, token age) — these live in
  each source's `raw` payload under *different* field names per source
  (PumpPortal `marketCapSol`, Birdeye `liquidity`, GeckoTerminal
  `reserve_in_usd`), so they need a per-source normalization layer that is
  its own phase. Phase 1 filters only the top-level fields that are
  uniform across every source (`source`, `event_type`).
- Enrichment (calling external APIs to add data to events)
- Cross-source deduplication/correlation (the Strategies/Combos phase)
- Any dashboard/UI for managing rules (that's the deferred #4 UI work;
  this phase reads rules from a config file, which a future UI can write)
- Editing rules at runtime without a restart (config is read at startup)

## Architecture

A **separate process** — `python -m scanner.filter_main`, its own PM2 app
`zetryn-scanner-filter`, distinct from the `zetryn-scanner` producer
process. Rationale: sources are producers (write to Redis), filtering is a
consumer (reads from Redis); Redis streams are already the clean contract
between them, so a process boundary makes the conceptual boundary an
operational one too. A bug in the filter (runaway loop, state buildup)
cannot then take down source ingestion — raw events keep flowing, the
filter restarts independently. This matches `bot/`'s per-component
`supervise()` isolation intent.

```
zetryn-scanner (producer)          zetryn-scanner-filter (consumer)
  sources --> XADD                   XREADGROUP scanner:events:new_token
  scanner:events:new_token   ------> for each event, for each strategy:
  scanner:events:migration             if event_matches_strategy: XADD
                                          scanner:strategy:<name>
                                        XACK
```

Communication is purely through Redis — the two processes share no
in-memory state.

## Reading: Consumer Groups

Reads use `XREADGROUP` with a consumer group named `scanner-filter`
(created via `XGROUP CREATE ... MKSTREAM` on each source stream, ignoring
the "BUSYGROUP already exists" error on restart). This gives position
tracking + acknowledgement: if the filter process restarts, it resumes
from the last un-acknowledged event rather than missing events that
arrived while it was down. Phase 1's original design deliberately
deferred consumer groups "until a real reader exists" — this filter is
that reader, so this is the right moment to introduce them.

Each event is `XACK`-ed once processed — including events that matched no
strategy (they were successfully processed, just not forwarded) and
events that errored during processing (logged and skipped, then acked, so
one bad event never blocks the stream). This is consistent with the
"never crash, keep running" philosophy established across all sources.

## Project Structure (additions)

```
scanner/
├── strategies.example.json        # NEW: example rule config
├── .env.example                   # MODIFIED: add STRATEGIES_CONFIG_PATH
├── src/scanner/
│   ├── strategies.py              # NEW: Strategy model + matching logic
│   ├── strategy_config.py         # NEW: load strategies from JSON file
│   ├── filter_runner.py           # NEW: consumer-group loop
│   └── filter_main.py             # NEW: entrypoint + logging wiring
├── tests/
│   ├── test_strategies.py         # NEW
│   ├── test_strategy_config.py    # NEW
│   └── test_filter_runner.py      # NEW
├── ecosystem.config.js            # MODIFIED: add zetryn-scanner-filter app
```

## Strategy Model & Matching

`strategies.py`:
```python
class Strategy(BaseModel):
    name: str                              # -> stream scanner:strategy:<name>
    source_allowlist: list[str] | None = None   # None = any source
    event_types: list[str] | None = None        # None = any event_type


def event_matches_strategy(event: ScannerEvent, strategy: Strategy) -> bool:
    if strategy.source_allowlist is not None and event.source not in strategy.source_allowlist:
        return False
    if strategy.event_types is not None and event.event_type not in strategy.event_types:
        return False
    return True
```
Pure logic — no I/O, no state, no external calls. `None` on a field means
"don't filter on this field" (match anything). Both conditions are AND-ed.
Fully unit-testable without mocks.

The output stream name is derived from `strategy.name` as
`scanner:strategy:<name>`. Strategy names are restricted to characters
safe for a Redis key segment (lowercase letters, digits, hyphen,
underscore) — validated by the Pydantic model; an invalid name is a
config error logged and skipped at load time, not a crash.

## Config Loading

`strategy_config.py` loads a JSON file (path from `STRATEGIES_CONFIG_PATH`
env var, default `./strategies.json`). Shape:
```json
[
  {"name": "birdeye-only", "source_allowlist": ["birdeye"]},
  {"name": "launches", "event_types": ["new_token"]},
  {"name": "everything"}
]
```
`load_strategies(path) -> list[Strategy]`:
- File missing or empty list → returns `[]`; the runner logs one INFO line
  ("no strategies configured") and idles (stays alive, reads+acks events
  but forwards nothing) — analogous to the "Birdeye disabled" pattern.
- Malformed individual entry (bad name, wrong types) → that entry is
  logged and skipped; valid entries still load. A completely unparseable
  file → logged, treated as empty (`[]`), process stays alive.

`strategies.example.json` ships the three example strategies above so a
user can copy it to `strategies.json` and edit.

## Filter Runner

`filter_runner.py` exposes
`async def run_filter(redis_client, strategies, *, source_streams=("scanner:events:new_token", "scanner:events:migration"), group="scanner-filter", consumer="filter-1", max_batches=None, _sleep_fn=...) -> None`:

```
ensure consumer group exists on each source stream (XGROUP CREATE MKSTREAM, ignore BUSYGROUP)
loop (bounded by max_batches for tests, unbounded in production):
  entries = XREADGROUP group consumer, BLOCK, from each source stream
  for each entry:
    try:
      event = deserialize the stream fields back into a ScannerEvent
      for strategy in strategies:
        if event_matches_strategy(event, strategy):
          XADD scanner:strategy:<strategy.name> with the same fields
    except Exception:
      log warning, skip
    finally:
      XACK the entry on its source stream
```

`max_batches` bounds the loop for testing (None = run forever in
production), mirroring the `max_iterations` pattern used in every source's
loop. `redis_client` is injected so tests use `fakeredis`.

## Testing

- `test_strategies.py` — `event_matches_strategy` truth table: allowlist
  hit/miss, event_types hit/miss, `None` fields match anything, both
  conditions AND-ed. Plus `Strategy` name validation (valid names accepted,
  invalid names rejected). No mocks needed — pure logic.
- `test_filter_runner.py` — using `fakeredis`: seed a source stream with a
  few events (mixed sources/types), run `run_filter` with `max_batches=1`
  and a couple of strategies, assert matching events landed in the correct
  `scanner:strategy:<name>` streams, non-matching events did not, and all
  consumed entries were acked (pending count returns to zero). Include a
  test that a malformed stream entry is skipped-and-acked without raising.
- `test_strategy_config.py` — loading valid config, missing file → `[]`,
  malformed entry skipped, fully-broken file → `[]`.

## Deployment

New PM2 app `zetryn-scanner-filter` added to `ecosystem.config.js`
alongside the existing producer app. Copy `strategies.example.json` to
`strategies.json` on each environment and edit as desired (or leave absent
to idle). Redis is the same localhost instance already running. Use the
`pm2 delete`/`pm2 start` env-reload pattern if adding new env vars (per the
operational gotcha from earlier deploys).

## Definition of Done

With the producer (`zetryn-scanner`) and filter (`zetryn-scanner-filter`)
both running and a `strategies.json` defining at least one strategy,
`redis-cli XLEN scanner:strategy:<name>` increases for events that match
that strategy over a 10+ minute window, the raw `scanner:events:new_token`
stream keeps growing normally (producer unaffected), the consumer group's
pending count stays low (events are being acked), and neither process
crashes.

## Out of Scope (Deferred)

- Numeric/`raw`-field filters needing per-source normalization
- Enrichment via external APIs
- Cross-source dedup/correlation (Strategies/Combos)
- Runtime rule editing / dashboard UI
- Multiple consumers sharing one group for horizontal scaling (single
  consumer `filter-1` is enough for this phase)
