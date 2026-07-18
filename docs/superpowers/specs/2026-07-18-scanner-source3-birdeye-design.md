# Scanner Module — Source #3: Birdeye Design

**Status:** Approved
**Date:** 2026-07-18

## Background

Scanner Phase 1 (PumpPortal WS) and Source #2 (GeckoTerminal REST polling)
are both live and verified, both keyless. This spec covers the third
source — the first one that requires an API key.

**Why this matters beyond adding a source**: the user's original 8-point
Scanner request included Router integration (auto-activate a scanner
source when its API key already exists in Router's database). That work
was deliberately deferred (2026-07-18) because Scanner had no real
key-required source to test the integration against — building it now
would only be testable with mock data. This spec's source becomes that
real test case for a future Router-integration spec.

**Why Birdeye, not Helius (reconsidered mid-brainstorm)**: the initial
plan for source #3 was Helius WebSocket (`logsSubscribe`/`programSubscribe`
on the standard, free-tier-available Solana WS surface — Helius's
*Enhanced* decoded WS needs a paid Developer tier). Research into the
old, frozen `bot/` scanner module (read-only reference, not modified)
found this would be unprecedented: `bot/`'s Helius integration
(`zetryn_bot/scanners/enrichers/helius.py`) uses Helius **only** as an
on-demand enricher (`getTokenAccounts` + `getAsset` for holder/metadata of
already-found tokens) — never as a detector. No code anywhere in that
repo (or, to the user's knowledge, in any real Zetryn deployment) parses
raw Helius WS transaction logs for Pump.fun/Raydium program activity into
decoded events. Building that now would be net-new, unproven work — the
opposite of the "small, independently-testable unit" approach that has
worked twice in a row for Scanner. Birdeye, by contrast, is REST polling
(`bot/scanners/birdeye.py` already did `new_listing` polling in
production) — the same shape as GeckoTerminal, a pattern Scanner already
has proven working code for. Helius-as-detector is shelved indefinitely;
Helius-as-enricher is a separate, later concern outside Scanner's scope
entirely (Scanner publishes events, it doesn't enrich them).

## Goal

Add a third, independent event source: poll Birdeye's Solana
`new_listing` endpoint for newly listed tokens, parse them into the
existing `ScannerEvent` schema, and publish into the same Redis Streams
the other two sources already write to — running as a third `asyncio`
task in the same process. This is also the first source requiring an API
key, read from an environment variable for this phase (Router integration
is explicitly deferred, see above).

**Explicitly out of scope for this spec** (deferred):
- Router integration (reading the key from Router's database instead of
  an env var) — a separate future spec, once this source proves the
  key-required pattern works
- Cross-source deduplication/correlation (same deferral as source #2 —
  belongs to the future Strategies/Combos-equivalent aggregation spec)
- Helius in any form (detector or enricher)
- Any other scanner source
- Any event types beyond `new_token`

## Verified API Details (live-tested with the user's own free-tier key, July 2026)

Contrary to older documentation/research (which cited "30,000 CU/month, 1
rps" and frequent 400s on `new_listing` under free tier), a live test
against the user's actual key returned:

- `GET https://public-api.birdeye.so/defi/v2/tokens/new_listing?chain=solana&limit=5`
  with headers `X-API-KEY: <key>` and `x-chain: solana` → **HTTP 200**,
  clean JSON, no tier-restriction error.
- Response headers confirm the *actual* current rate limit for this key:
  `x-ratelimit-limit: 100`, resetting roughly every ~60 seconds (`x-ratelimit-reset`
  is a unix timestamp observed ~60s ahead of the request). This is
  **numerically different from published docs** — trust the live header
  over the older docs, and design the polling interval against the header
  value, not the docs value.
- The old fallback endpoint the `bot/` reference used
  (`/defi/tokenlist?sort_by=recentListingTime`) returned `{"success":
  false, "message": "Not found"}` — it appears removed/renamed since
  `bot/` was last active. **No fallback endpoint is implemented in this
  spec** — if `new_listing` fails, the poll cycle is simply logged and
  skipped like any other failure, no secondary endpoint is attempted.
- Response shape (`data.items[]`), simpler than GeckoTerminal — the mint
  address is directly on the item, no JSON:API `included[]` resolution
  needed:
  ```json
  {
    "success": true,
    "data": {
      "items": [
        {
          "address": "2yGGhJ9AyQsgHTwWHCpcqMhMMmv5QE7JT3D5jAvR4dAj",
          "symbol": "Pedro",
          "name": "Pedro Pedro Pedro",
          "decimals": 6,
          "source": "pump_amm",
          "liquidityAddedAt": "2026-07-18T15:38:04",
          "logoURI": "https://...",
          "liquidity": 17215.636907934913
        }
      ]
    }
  }
  ```
  Note `source` here is Birdeye's own field naming the listing venue
  (`pump_amm`, `whirlpool`, `meteora_damm_v2`, `meteora_dynamic_bonding_curve`,
  etc.) — not to be confused with `ScannerEvent.source`, which this spec
  always sets to the literal string `"birdeye"` regardless of Birdeye's
  venue value (the venue detail is preserved inside `raw`, not lost).

## Architecture

A third `asyncio` task, `run_birdeye_scanner()`, added to the same
`asyncio.gather` call in `main.py` alongside the existing PumpPortal and
GeckoTerminal tasks. No new PM2 app — same reasoning as source #2 (these
sources are independent at the publish level, no coordination needed
beyond writing to the same stream).

```
Birdeye REST  <--poll 30s-->  birdeye scanner task   --\
GeckoTerminal REST <--poll 30s-->  geckoterminal task --+--> Redis Stream
PumpPortal WS <--stream--> pumpportal task            --/    scanner:events:new_token
                                                              scanner:events:migration
```

## Project Structure (additions)

```
scanner/
├── .env.example                # MODIFIED: add BIRDEYE_API_KEY=
├── src/scanner/
│   ├── config.py               # MODIFIED: add load_birdeye_api_key(), URL/interval constants
│   └── birdeye.py              # NEW: parser + polling loop
├── tests/
│   └── test_birdeye.py         # NEW
```

## Event Mapping

Every item from `new_listing` maps to `event_type=EVENT_TYPE_NEW_TOKEN`,
`source=SOURCE_BIRDEYE` (new constant, `"birdeye"`, added to `events.py`
alongside the existing `SOURCE_PUMPPORTAL`/`SOURCE_GECKOTERMINAL`). `mint`
comes directly from the item's `address` field. `raw` stores the full
original item (including Birdeye's own `source` field, e.g. `"pump_amm"`
— preserved as data, not interpreted). `received_at` is the scanner's own
UTC timestamp at parse time (consistent with sources #1 and #2), not
`liquidityAddedAt` (which stays inside `raw` for any consumer that wants
it).

## Missing-Key Behavior

`run_birdeye_scanner()` checks `load_birdeye_api_key()` once at the start
of the function, before entering the polling loop. If it returns `None`
(env var unset or empty): log one INFO line
(`"Birdeye disabled: no API key"`) and return immediately — no polling
loop iteration ever runs, no HTTP calls are attempted, no retry. This
mirrors the observed `bot/` pattern of skipping keyless-required sources
with a log line rather than crashing or looping on guaranteed-failure
requests. The other two scanner tasks in `asyncio.gather` are unaffected
either way — this task simply completes early.

## Polling Loop & Error Handling

```
if no BIRDEYE_API_KEY:
  log "Birdeye disabled: no API key"
  return

loop:
  try:
    response = await http_get_fn(NEW_LISTING_URL, headers={"X-API-KEY": key, "x-chain": "solana"}, params={"chain": "solana", "limit": 5})
    response.raise_for_status()
    payload = response.json()
    for item in payload.get("data", {}).get("items", []):
      event = parse_birdeye_token(item)
      if event is None:
        continue
      await publisher.publish(event)
  except Exception as exc:
    log a warning with the exception, do not retry immediately
  wait 30 seconds
  go back to the top of the loop
```

Same simplification as source #2: no backoff logic for REST polling
failures — a failed poll is not fatal, it just tries again at the next
30-second interval. 30 seconds against the observed 100-per-~60s limit
uses roughly 2 of 100 available requests per window, well within budget.

## Testing

- **`test_birdeye.py`** (new):
  - Parses a realistic item (shaped like the verified live example above)
    into a `ScannerEvent` with `mint` read directly from `address`.
  - Missing/non-string `address` → parse function returns `None`, never
    raises.
  - Non-dict item → returns `None`.
  - Polling loop test: mocked HTTP client returns a fixed payload once,
    then raises (simulating a network error) — asserts the loop logs and
    continues, using the same injectable `max_iterations`/`_sleep_fn`
    pattern already established in `test_geckoterminal.py`.
  - Missing-key test: with `BIRDEYE_API_KEY` unset (or an injectable key
    parameter set to `None`), asserts the function returns without ever
    calling the injected `http_get_fn` (mock call count stays 0).

**Manual integration test** (after unit tests are green): run the process
locally (conda env `zetryn-scanner`, with `BIRDEYE_API_KEY` set in
`.env`) or on the VPS for several minutes; verify via `redis-cli XLEN
scanner:events:new_token` that the count increases, and spot-check a few
entries have `source: "birdeye"` via `redis-cli XRANGE`.

## Deployment

No new PM2 app — same `zetryn-scanner` process picks up the third task
once `main.py` is updated. `BIRDEYE_API_KEY` must be added to the VPS's
`.env` file (the user's free-tier key) before restarting; without it, the
task simply logs its one disabled-message and the other two sources
continue unaffected, per the Missing-Key Behavior section above.

## Definition of Done

The process runs continuously (VPS and/or local) with `BIRDEYE_API_KEY`
set, all three tasks stay alive (Birdeye task survives individual poll
failures the same way GeckoTerminal's does), and `redis-cli XLEN
scanner:events:new_token` shows entries with `source: "birdeye"`
interleaved with `source: "pumpportal"` and `source: "geckoterminal"`
entries over a 10+ minute observation window, with zero process crashes.

## Out of Scope (Deferred)

- Router integration for reading the Birdeye key (future spec — this
  source's existence is what makes that spec testable for real)
- A fallback endpoint if `new_listing` fails (the old `tokenlist` fallback
  the `bot/` reference used no longer exists; no replacement fallback is
  in scope for this phase — a failed poll is simply skipped)
- Cross-source deduplication/correlation
- Helius (detector or enricher, in any form)
- Any other scanner source
- Any event type beyond `new_token` for this source
