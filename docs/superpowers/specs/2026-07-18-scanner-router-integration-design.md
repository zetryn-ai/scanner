# Scanner Module — Router Integration Design

**Status:** Approved
**Date:** 2026-07-18

## Background

This is the deferred item #2 from the original 8-point Scanner request:
integrate Scanner with Zetryn Router so a scanner source whose credential
already exists in Router doesn't need its own separately-managed key. It
was postponed on 2026-07-18 until Scanner had a real key-required source
to test the integration against (source #3, Birdeye, shipped the same
day) — this spec is that follow-through.

**Architecture-defining finding** (confirmed by reading Router's code,
not assumed): Router never exposes a raw provider API key through any
endpoint. `POST /api/credentials` deliberately masks `secretValue` to
`'••••••••'` in its response, with an explicit code comment "never
serialize secrets to the client"; no `GET` endpoint for an individual
credential exists at all. Router is a pure proxy — external consumers hit
`/proxy/[slug]/[...path]` with their own Router-issued
`Authorization: Bearer zr_...` consumer key, and Router's
`proxy-orchestrator.ts`/`rotation.ts` pick, decrypt, and inject the actual
provider credential server-side before forwarding to the real provider
(e.g. Birdeye). This rules out the literal original framing ("Scanner
reads Birdeye's key out of Router's database") without weakening Router's
security model. Instead: **Scanner calls providers through Router's
existing proxy surface**, using one Router consumer key — zero changes to
Router itself.

## Goal

Change Scanner's Birdeye source (shipped as source #3) to call Birdeye
through Router's proxy (`https://apirouter.lemacore.com/proxy/birdeye/...`)
instead of calling Birdeye directly, using a single Router consumer key
(`ROUTER_API_KEY`) instead of a Birdeye-specific key. This is the first
proof that a Scanner source can run entirely without its own
provider-specific credential, deferring all key management to Router.

**Explicitly out of scope for this spec**:
- Any change to Router itself (this is 100% a Scanner-side change)
- Auto-discovery of which Router providers exist/are active (Scanner
  still explicitly names `birdeye` in its proxy URL — no dynamic
  provider-list querying in this phase)
- Any other Scanner source moving to Router (PumpPortal and GeckoTerminal
  are keyless and stay as-is; future paid sources should follow this same
  pattern once they exist, but that's not built here)
- Retry/backoff specific to Router's own failure modes (e.g. Router
  reporting no active credential) beyond the existing generic
  log-and-skip-per-cycle behavior already in place

## Verified Router Proxy Mechanics

- Auth is conditional on Router's `require_api_key` setting; confirmed
  live on the VPS this is currently `'1'` (enabled) — so calls without a
  valid `Authorization: Bearer zr_...` header get a `401` from
  `proxy-orchestrator.ts`'s `requireApiKeyEnabled()` check.
- URL shape: `/proxy/<slug>/<provider's own path>?<query>` — confirmed via
  `rotation.ts`'s `resolveTarget`, which joins the incoming path onto the
  provider's `default_base_url` (`https://public-api.birdeye.so` for
  Birdeye) and copies the query string through unchanged. So Scanner's
  existing path (`defi/v2/tokens/new_listing`) and query
  (`chain=solana&limit=5`) carry over unmodified — only the base URL and
  auth header change.
- Router injects **only** the provider's registered credential (for
  Birdeye: header `X-API-KEY`, per its `default_inject_location`/
  `default_inject_key_name`). It does **not** inject `x-chain: solana` —
  that's not part of Birdeye's credential-injection config in Router, just
  a header Birdeye's API happens to also require. Scanner must keep
  sending `x-chain: solana` itself; the proxy route forwards non-stripped
  headers through untouched, so it reaches Birdeye correctly.
- Birdeye already has 2 active credentials configured in Router (verified
  live on the VPS DB, `credentials` table, provider_id for slug
  `birdeye`), so the proxy call has a real credential to rotate through
  from day one.

## Architecture

No new file — this modifies the existing `src/scanner/birdeye.py` and
`src/scanner/config.py` from source #3. `parse_birdeye_token` (the
parser) is unchanged: Router's proxy is byte-transparent for the response
body, so the JSON shape Scanner already parses is identical whether it
comes from Birdeye directly or via Router.

```
Scanner (birdeye.py)
   --Authorization: Bearer <ROUTER_API_KEY>, x-chain: solana-->
   https://apirouter.lemacore.com/proxy/birdeye/defi/v2/tokens/new_listing?chain=solana&limit=5
   --Router injects X-API-KEY internally, forwards-->
   https://public-api.birdeye.so/defi/v2/tokens/new_listing?chain=solana&limit=5
   <--same JSON response shape as before--
```

## Config Changes

`src/scanner/config.py`:
- Remove `load_birdeye_api_key()` and the direct `BIRDEYE_NEW_LISTING_URL`
  (`https://public-api.birdeye.so/...`) — no longer used.
- Add `load_router_api_key() -> str | None` reading `ROUTER_API_KEY`.
- Add `ROUTER_BIRDEYE_PROXY_URL = "https://apirouter.lemacore.com/proxy/birdeye/defi/v2/tokens/new_listing"`.
- `BIRDEYE_POLL_INTERVAL_SECONDS` stays unchanged (30s — Router's proxy
  doesn't change Birdeye's underlying rate limit, which the interval was
  already sized well under).

`.env.example`: replace `BIRDEYE_API_KEY=` with `ROUTER_API_KEY=`. The
name is deliberately generic (not `BIRDEYE_ROUTER_API_KEY`) because the
long-term Scanner roadmap is for every future paid/key-required source to
eventually go through Router the same way — one Router consumer key
should be reusable across all of them, the same way one `zr_...` key can
already call multiple providers through Router's proxy.

## Polling Loop Changes

`run_birdeye_scanner()`'s missing-key check now reads
`load_router_api_key()` instead of `load_birdeye_api_key()`; the log
message changes to `"Birdeye disabled: no Router API key"` (still logs
once and returns immediately, same behavior as before — just checking a
different, more general credential).

The HTTP call changes from:
```
headers = {"X-API-KEY": resolved_key, "x-chain": "solana"}
GET BIRDEYE_NEW_LISTING_URL with params {"chain": "solana", "limit": 5}
```
to:
```
headers = {"Authorization": f"Bearer {resolved_key}", "x-chain": "solana"}
GET ROUTER_BIRDEYE_PROXY_URL with params {"chain": "solana", "limit": 5}
```
Everything else in the polling loop (30s interval, per-cycle
try/except-log-skip on any failure, `max_iterations`/`_sleep_fn`
injection points for testing) is unchanged from source #3.

## Testing

Existing `test_birdeye.py` parser tests (`test_parse_valid_item_...`,
missing/malformed `address` tests) are unaffected — `parse_birdeye_token`
doesn't change. The polling-loop tests are updated:
- The existing `run_birdeye_scanner(..., api_key=...)` parameter keeps its
  name (it's already generic) but now represents a Router consumer key
  rather than a Birdeye key — no signature change needed, only what's
  passed in the tests and what the function does with it internally.
- Update the fake HTTP call assertions so the URL argument matches
  `ROUTER_BIRDEYE_PROXY_URL`, not the old direct Birdeye URL.
- Add a new test asserting the request headers sent include both
  `Authorization: Bearer <key>` and `x-chain: solana` together (this
  wasn't previously tested as a pair since source #3 used `X-API-KEY` as
  the credential header instead of `Authorization`).

**Manual integration test** (after unit tests are green): with
`ROUTER_API_KEY` set (local `.env` and VPS `.env`, replacing
`BIRDEYE_API_KEY`), run the process and verify via `redis-cli XLEN
scanner:events:new_token` that `source: "birdeye"` entries keep arriving,
confirming the proxy round-trip works end-to-end with a real Router
consumer key against the live Router deployment at
`apirouter.lemacore.com`.

## Deployment

No new PM2 app, no Router changes. On both local and VPS: remove
`BIRDEYE_API_KEY` from `.env`, add `ROUTER_API_KEY` (the user's existing
Router consumer key), then restart the Scanner process. Per the
operational gotcha recorded from source #3's deployment, use `pm2 delete`
followed by `pm2 start ecosystem.config.js` (after sourcing the updated
`.env`) rather than `pm2 restart` — `pm2 restart` does not reload a
freshly-edited `.env` file.

## Definition of Done

The process runs continuously (local and/or VPS) with `ROUTER_API_KEY`
set (no `BIRDEYE_API_KEY` present at all), all three scanner tasks stay
alive, and `redis-cli XLEN scanner:events:new_token` shows `source:
"birdeye"` entries continuing to arrive — now proven to be flowing
through Router's proxy rather than hitting Birdeye directly — over a
10+ minute observation window, with zero process crashes.

## Out of Scope (Deferred)

- Any change to Router itself
- Dynamic discovery of which providers/credentials exist in Router
  (Scanner still hardcodes `birdeye` in the proxy URL)
- Migrating any other current or future scanner source to Router in this
  same spec (this proves the pattern for one source; applying it to
  others is separate, incremental work when those sources exist)
- Router-specific retry/backoff behavior beyond the existing generic
  per-cycle error handling
