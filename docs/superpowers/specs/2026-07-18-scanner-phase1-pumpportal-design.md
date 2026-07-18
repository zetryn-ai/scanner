# Scanner Module — Phase 1: PumpPortal Core Design

**Status:** Approved
**Date:** 2026-07-18

## Background

`ai-agent` (Solana trading framework, PyPI `zetryn-trading`) and `bot` (production
I/O bot) are being superseded by a v2 rebuild. Both repos are frozen as
**reference only** — nothing in this spec touches or depends on their code.
`bot`'s `docs/API-KEYS.md` documents a scanner that already runs in production
(PumpPortal WS, DexScreener, GeckoTerminal, Raydium polling, measured at ~23
decisions/min steady state) — useful as a data reference (e.g. dust-prefilter
cut PumpPortal volume 155→13 events/10min), not as code to reuse.

Separately, `router` (this session's other major deliverable) is a Next.js
HTTP proxy for REST-style API key rotation. Research (see design rationale
below) established that WebSocket/gRPC streaming — which is what every
real-time Solana scanner source requires — cannot be forwarded through an
HTTP request/response proxy like Router; the connection is long-lived and a
serverless-style API route terminates after one response. This is why
Scanner is architected as an independent long-running process, not a Router
feature.

## Goal

Prove out the smallest possible real-time scanner pipeline: connect to
PumpPortal's public WebSocket, normalize its `new_token` and `migration`
events into a consistent schema, and publish them to Redis Streams — so
that any future consumer (bot v2, an AI agent, ad-hoc scripts) can read a
live feed of Solana token launches without needing its own WebSocket client
or PumpPortal integration.

**Explicitly out of scope for this phase** (deferred to later specs):
- Any other scanner source (Helius Geyser, Birdeye WS, DexScreener REST, etc.)
- Any dashboard/UI
- Reading API keys from Router's database
- Any Combos/Strategies-style multi-source aggregation
- Consumer-side code (nothing reads from the Redis stream in this phase — only
  manual `redis-cli XLEN`/`XRANGE` verification)

## Why PumpPortal, and its known risk

PumpPortal is a third-party API, **not officially affiliated with Pump.fun**.
Its own FAQ discloses: no guarantee of first-snipe or fastest trade, WebSocket
delivery at "processed" commitment (not confirmed/finalized — reorg-exposed),
and explicitly "websockets can just disconnect... good idea to have
reconnection logic." A documented GitHub issue reports a pump-related
third-party API returning 502s (Jan 2026). `subscribeNewToken` and
`subscribeMigration` are free and keyless; `subscribeTokenTrade` is metered
and requires a funded API key (not used in this phase).

**Conclusion**: acceptable foundation for this validation phase — it is the
fastest way to prove the pipeline shape — but not a long-term production
commitment. A later phase should evaluate migrating to Yellowstone gRPC via
Shyft (~$199/mo, commercial SLA, published Pump.fun-specific tutorials) once
the pipeline and downstream consumers are proven out. This tradeoff is
recorded here so it isn't re-litigated from scratch later.

## Architecture

One long-running Python (`asyncio`) process, no framework, no Docker for this
phase — deployed via PM2 on the same VPS as Router, matching the pattern
that already works there. Redis runs on the same VPS, bound to `127.0.0.1`
only, installed via `apt`, no auth (localhost-only, same trust model as
Router's SQLite file).

```
PumpPortal WS  <-->  scanner process (reconnect loop, parse, validate)  -->  Redis Stream
                                                                              scanner:events:new_token
                                                                              scanner:events:migration
```

The process never exits on connection errors — only on a fatal, explicitly
logged condition (e.g. Redis unreachable after repeated retries) or manual
stop. This mirrors Router's principle of failing loud and staying alive
rather than crash-looping silently.

## Project Structure

```
scanner/
├── pyproject.toml              # deps: websockets, redis, pydantic, pytest, pytest-asyncio, fakeredis
├── .env.example                # PUMPPORTAL_API_KEY (optional, unused in phase 1), REDIS_URL
├── src/scanner/
│   ├── __init__.py
│   ├── config.py                # env loading, WS URL, backoff constants
│   ├── events.py                 # ScannerEvent Pydantic model + parse function
│   ├── pumpportal.py             # WS connection + reconnect loop
│   ├── publisher.py              # Redis Stream publish wrapper (with in-memory retry buffer)
│   └── main.py                   # wires pumpportal -> publisher, runs the asyncio loop
├── tests/
│   ├── test_events.py
│   ├── test_publisher.py
│   └── test_pumpportal.py
├── ecosystem.config.js           # PM2 config, same pattern as router/ecosystem.config.js
└── README.md
```

Each file has one responsibility so a second source (e.g. Helius Geyser)
can be added later as a new sibling module without touching `pumpportal.py`.

## Event Schema

```python
class ScannerEvent(BaseModel):
    event_type: Literal["new_token", "migration"]
    source: Literal["pumpportal"]   # ready for multi-provider later
    mint: str                        # token mint address
    raw: dict                        # untouched original payload
    received_at: str                 # ISO8601 UTC, when the scanner received it (not on-chain time)
```

`raw` is kept in full so fields not yet promoted to the top level (token
name, bonding-curve progress, etc.) remain available to consumers without a
schema migration — they can be added as first-class fields once a real
consumer needs them.

## Redis Layout

**Redis Streams** (not plain Pub/Sub) via `XADD scanner:events:<event_type> *
<fields>`. Streams persist events with an ordered ID; Pub/Sub drops messages
for any consumer not actively listening at publish time. Since Scanner will
often run before any consumer exists (this phase has none), and consumers
may restart independently, Streams' durability and `XREAD`-from-last-ID
semantics are required — Pub/Sub would silently lose events.

Two streams, split by event type so a future consumer can subscribe to only
what it needs:
- `scanner:events:new_token`
- `scanner:events:migration`

No consumer groups in this phase (no consumer exists yet) — just plain
`XADD`. Consumer groups (`XREADGROUP`) are a phase-2 concern once a real
reader exists and needs at-least-once delivery tracking.

## Reconnect & Error Handling

```
loop:
  try:
    connect to wss://pumpportal.fun/api/data
    send subscribeNewToken + subscribeMigration payloads
    while connected:
      receive message -> parse -> validate (Pydantic) -> publish to Redis Stream
      on parse/validation failure: log a warning with the raw payload, skip it, keep the loop running
  except (ConnectionClosed, TimeoutError, OSError, any other exception):
    log the disconnect reason
    wait with exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 30s, + up to 20% random jitter
    go back to the top of the loop (reconnect)
```

Redis publish failures are handled independently: `publisher.py` holds a
bounded in-memory ring buffer (last 500 events) so a transient Redis outage
does not lose recent events; publish is retried on each new event until
Redis responds. If the buffer fills (Redis down for a sustained period), the
oldest buffered event is dropped and a warning is logged with the drop
count — the process never crashes or blocks the WebSocket read loop because
of a Redis outage.

Structured JSON-lines logging to stdout (captured by PM2) records: connect,
disconnect (with reason), reconnect attempt (with delay), and a periodic
events-per-minute counter — so a future "why did this stop producing
events" investigation has a trail instead of silence.

## Testing

- **`test_events.py`** — unit tests parsing real PumpPortal example payloads
  (from official docs) into `ScannerEvent`; malformed/incomplete payloads
  must not raise — the parse function returns `None` and the caller skips
  and logs, never propagating an exception up into the WS read loop.
- **`test_publisher.py`** — publish behavior against `fakeredis` (in-memory
  mock), no real Redis needed for unit tests; covers the ring-buffer
  overflow/drop behavior.
- **`test_pumpportal.py`** — reconnect logic against a mocked WebSocket
  (simulated disconnects), asserting backoff delays and that the loop
  retries indefinitely rather than exiting.

**Manual integration test** (once, after unit tests are green): run the
process against the real PumpPortal endpoint for several minutes; verify
via `redis-cli XLEN scanner:events:new_token` that the count increases.

## Deployment

PM2 on the same VPS as Router (`46.250.236.190`), same operational pattern:
`ecosystem.config.js`, `pm2 start`, `pm2 save`, `pm2 startup` already
enabled from Router's setup covers auto-boot for both apps under one
systemd-managed PM2 daemon. Redis installed via `apt install redis-server`,
config bound to `127.0.0.1`, no password (localhost-only trust boundary,
consistent with Router's SQLite file having no network exposure).

## Definition of Done (Phase 1)

The process runs continuously on the VPS, stays connected to PumpPortal
(reconnecting through any drops), and `redis-cli XLEN scanner:events:new_token`
shows a monotonically increasing count over a 10+ minute observation window
with zero process crashes.

## Out of Scope (Deferred)

- Additional scanner sources (Helius Geyser/LaserStream, Birdeye WS,
  DexScreener/GeckoTerminal REST polling)
- Any dashboard or web UI for Scanner
- Reading provider API keys from Router's database (the "auto-activate if
  Router has a key" integration from the original request)
- Strategies/Combos-equivalent multi-source aggregation
- Any consumer code (bot v2, AI agent) reading from the Redis streams
- Redis consumer groups / at-least-once delivery tracking
- Migration to Yellowstone gRPC (Shyft/Helius) for lower latency
