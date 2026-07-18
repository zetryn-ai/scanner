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

The `raw` field of each entry is the original PumpPortal payload as a JSON
string (`json.loads`-able).

## Tests

```bash
uv run pytest -v
```
