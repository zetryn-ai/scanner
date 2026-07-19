# Scanner-Router Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change Scanner's Birdeye source (source #3) to call Birdeye through Zetryn Router's proxy (`https://apirouter.lemacore.com/proxy/birdeye/...`) using a single Router consumer key, instead of calling Birdeye directly with a Birdeye-specific key.

**Architecture:** `run_birdeye_scanner()` swaps its credential check from `load_birdeye_api_key()` to `load_router_api_key()`, its request URL from Birdeye's direct endpoint to Router's proxy path for the same endpoint, and its credential header from `X-API-KEY` to `Authorization: Bearer`. `parse_birdeye_token` (the parser) is unchanged — Router's proxy is byte-transparent for the response body.

**Tech Stack:** No new dependencies — same `httpx`/`pydantic`/`pytest`/`pytest-asyncio` stack already in place.

## Global Constraints

- This is a Scanner-only change — do not modify anything in `/mnt/data/Project/zetryn/router`.
- Router's `require_api_key` setting is currently `'1'` (enabled) on the live VPS deployment — calls without a valid `Authorization: Bearer zr_...` header get a 401 from Router.
- Router injects only the provider's registered credential (`X-API-KEY` for Birdeye) — it does NOT inject `x-chain: solana`. Scanner must keep sending `x-chain: solana` itself in every request to the proxy.
- Proxy URL shape is `/proxy/<slug>/<provider's own path>?<query>` — Birdeye's existing path (`defi/v2/tokens/new_listing`) and query (`chain=solana&limit=5`) carry over unchanged; only the base URL and auth header change.
- `BIRDEYE_POLL_INTERVAL_SECONDS` (30s) is unchanged — Router's proxy doesn't change Birdeye's underlying rate limit.
- The new env var name is `ROUTER_API_KEY` (generic, not `BIRDEYE_ROUTER_API_KEY`) — the long-term Scanner roadmap has every future paid/key-required source eventually going through Router the same way, reusing one Router consumer key.
- On deploy: use `pm2 delete` then `pm2 start ecosystem.config.js` (after sourcing the updated `.env`), never `pm2 restart` — `pm2 restart` does not reload a freshly-edited `.env` file (confirmed operational gotcha from source #3's deployment).
- Definition of done: process runs continuously (local and/or VPS) with `ROUTER_API_KEY` set and `BIRDEYE_API_KEY` absent, all three scanner tasks stay alive, `redis-cli XLEN scanner:events:new_token` shows `source: "birdeye"` entries continuing to arrive (now via Router's proxy) over a 10+ minute window, zero crashes.

---

## File Structure (changes only, no new files)

```
scanner/
├── .env.example                 # MODIFIED: BIRDEYE_API_KEY= -> ROUTER_API_KEY=
├── src/scanner/
│   ├── config.py                # MODIFIED: remove load_birdeye_api_key/BIRDEYE_NEW_LISTING_URL, add load_router_api_key/ROUTER_BIRDEYE_PROXY_URL
│   └── birdeye.py               # MODIFIED: use Router proxy URL + Bearer auth
├── tests/
│   └── test_birdeye.py          # MODIFIED: polling-loop tests updated for the new URL/header
```

---

### Task 1: Swap config from direct Birdeye to Router proxy

**Files:**
- Modify: `src/scanner/config.py`
- Modify: `.env.example`

**Interfaces:**
- Removes: `load_birdeye_api_key()`, `BIRDEYE_NEW_LISTING_URL` (no longer referenced by any code after this task's completion — but `birdeye.py` isn't updated until Task 2, so this task alone would leave `birdeye.py` broken; that's expected and fixed immediately in Task 2, which follows without a separate test-passing checkpoint in between since `config.py` has no direct tests of its own, consistent with how Tasks 1 and 4 were handled in the source #2 and #3 plans).
- Produces: `load_router_api_key() -> str | None` (reads `ROUTER_API_KEY`), `ROUTER_BIRDEYE_PROXY_URL = "https://apirouter.lemacore.com/proxy/birdeye/defi/v2/tokens/new_listing"`.

- [ ] **Step 1: Update config.py**

In `src/scanner/config.py`, replace:
```python
BIRDEYE_NEW_LISTING_URL = "https://public-api.birdeye.so/defi/v2/tokens/new_listing"
BIRDEYE_POLL_INTERVAL_SECONDS = 30.0


def load_birdeye_api_key() -> str | None:
    return os.environ.get("BIRDEYE_API_KEY") or None
```
with:
```python
ROUTER_BIRDEYE_PROXY_URL = "https://apirouter.lemacore.com/proxy/birdeye/defi/v2/tokens/new_listing"
BIRDEYE_POLL_INTERVAL_SECONDS = 30.0


def load_router_api_key() -> str | None:
    return os.environ.get("ROUTER_API_KEY") or None
```

- [ ] **Step 2: Update .env.example**

Change `.env.example` from:
```
PUMPPORTAL_API_KEY=
BIRDEYE_API_KEY=
REDIS_URL=redis://127.0.0.1:6379
```
to:
```
PUMPPORTAL_API_KEY=
ROUTER_API_KEY=
REDIS_URL=redis://127.0.0.1:6379
```

- [ ] **Step 3: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/config.py .env.example
git commit -m "refactor: swap Birdeye config for a generic Router consumer key"
```

(No test run in this step — `birdeye.py` still imports the now-removed `load_birdeye_api_key`/`BIRDEYE_NEW_LISTING_URL` names until Task 2 updates it. This is expected; Task 2 fixes the import and both tasks together form one working change. Do not run the test suite between these two tasks.)

---

### Task 2: Update birdeye.py to call Router's proxy

**Files:**
- Modify: `src/scanner/birdeye.py`

**Interfaces:**
- Consumes: `load_router_api_key`, `ROUTER_BIRDEYE_PROXY_URL` (Task 1).
- Produces: `run_birdeye_scanner` keeps its exact existing signature (`publisher`, `api_key`, `http_get_fn`, `max_iterations`, `_sleep_fn`) — only its internal behavior changes (which env var it reads, what URL/headers it sends). `parse_birdeye_token` is completely unchanged.

- [ ] **Step 1: Update the import block in birdeye.py**

In `src/scanner/birdeye.py`, replace:
```python
from scanner.config import (
    BIRDEYE_NEW_LISTING_URL,
    BIRDEYE_POLL_INTERVAL_SECONDS,
    load_birdeye_api_key,
)
```
with:
```python
from scanner.config import (
    BIRDEYE_POLL_INTERVAL_SECONDS,
    ROUTER_BIRDEYE_PROXY_URL,
    load_router_api_key,
)
```

- [ ] **Step 2: Update run_birdeye_scanner's body**

Replace the body of `run_birdeye_scanner` (everything from `resolved_key = ...` to the end of the function) with:
```python
    resolved_key = load_router_api_key() if api_key is _UNSET else api_key
    if not resolved_key:
        logger.info("Birdeye disabled: no Router API key")
        return

    http_get_fn = http_get_fn or _default_http_get_fn
    headers = {"Authorization": f"Bearer {resolved_key}", "x-chain": "solana"}
    iterations = 0

    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        try:
            response = await http_get_fn(ROUTER_BIRDEYE_PROXY_URL, headers, {"chain": "solana", "limit": 5})
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

Also update the function's docstring — replace:
```python
    """Poll Birdeye's Solana new_listing endpoint every
    BIRDEYE_POLL_INTERVAL_SECONDS, publishing a ScannerEvent per item.

    If no API key is available (either the `api_key` param is None, or it
    is left at its default and load_birdeye_api_key() returns None), this
    logs one INFO line and returns immediately — no HTTP call is ever
    attempted and the polling loop never runs.
```
with:
```python
    """Poll Birdeye's Solana new_listing endpoint through Zetryn Router's
    proxy every BIRDEYE_POLL_INTERVAL_SECONDS, publishing a ScannerEvent
    per item. Router injects Birdeye's actual API key server-side; this
    function only needs a Router consumer key, never Birdeye's key
    directly.

    If no Router API key is available (either the `api_key` param is
    None, or it is left at its default and load_router_api_key() returns
    None), this logs one INFO line and returns immediately — no HTTP call
    is ever attempted and the polling loop never runs.
```

- [ ] **Step 3: Verify the full file is internally consistent**

Read the full updated `src/scanner/birdeye.py` and confirm: no remaining reference to `load_birdeye_api_key`, `BIRDEYE_NEW_LISTING_URL`, or the `X-API-KEY` header anywhere in the file. `parse_birdeye_token` (lines defining it) must be byte-for-byte unchanged from before this task.

- [ ] **Step 4: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add src/scanner/birdeye.py
git commit -m "feat: route Birdeye scanner calls through Zetryn Router's proxy"
```

---

### Task 3: Update tests for the new URL and auth header

**Files:**
- Modify: `tests/test_birdeye.py`

**Interfaces:**
- Consumes: `run_birdeye_scanner`, `parse_birdeye_token` (unchanged signatures from Task 2).

- [ ] **Step 1: Update the missing-key test's log-message expectation and header assertions**

In `tests/test_birdeye.py`, replace the three polling-loop tests
(`test_run_birdeye_scanner_skips_entirely_without_api_key`,
`test_run_birdeye_scanner_publishes_parsed_items`,
`test_run_birdeye_scanner_continues_after_http_error`) with:

```python
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


async def test_run_birdeye_scanner_sends_router_proxy_url_and_bearer_auth():
    from scanner.config import ROUTER_BIRDEYE_PROXY_URL

    captured = {}

    async def fake_http_get_fn(url, headers, params):
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        return _FakeResponse({"success": True, "data": {"items": [SAMPLE_ITEM]}})

    publisher = _RecordingPublisher()

    from scanner.birdeye import run_birdeye_scanner

    await run_birdeye_scanner(
        publisher,
        api_key="fake-router-key",
        http_get_fn=fake_http_get_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert captured["url"] == ROUTER_BIRDEYE_PROXY_URL
    assert captured["headers"]["Authorization"] == "Bearer fake-router-key"
    assert captured["headers"]["x-chain"] == "solana"
    assert "X-API-KEY" not in captured["headers"]


async def test_run_birdeye_scanner_publishes_parsed_items():
    payload = {"success": True, "data": {"items": [SAMPLE_ITEM]}}

    async def fake_http_get_fn(_url, _headers, _params):
        return _FakeResponse(payload)

    publisher = _RecordingPublisher()

    from scanner.birdeye import run_birdeye_scanner

    await run_birdeye_scanner(
        publisher,
        api_key="fake-router-key",
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
        api_key="fake-router-key",
        http_get_fn=fake_http_get_fn,
        max_iterations=2,
        _sleep_fn=_no_op_sleep,
    )

    assert call_count == 2
    assert len(publisher.published) == 1
```

- [ ] **Step 2: Run the full test suite**

Run: `cd /mnt/data/Project/zetryn/scanner && uv run pytest -v`
Expected: all 33 tests PASS — breakdown: `test_events.py` 8, `test_publisher.py` 4, `test_pumpportal.py` 4, `test_geckoterminal.py` 8, `test_birdeye.py` 9 (5 parser tests unchanged + 4 polling-loop tests, one more than source #3's 3 because this task adds the new header-assertion test alongside the 3 existing ones).

- [ ] **Step 3: Commit**

```bash
cd /mnt/data/Project/zetryn/scanner
git add tests/test_birdeye.py
git commit -m "test: verify Birdeye scanner calls Router's proxy with Bearer auth"
```

---

### Task 4: Deploy and verify (Definition of Done)

**Files:** none (deployment + verification only).

- [ ] **Step 1: Sync and run the suite locally in the conda env**

Run:
```bash
conda activate zetryn-scanner
cd /mnt/data/Project/zetryn/scanner
pip install -e ".[dev]"
pytest -v
```
Expected: all 33 tests PASS.

- [ ] **Step 2: Swap the local .env from BIRDEYE_API_KEY to ROUTER_API_KEY**

```bash
cd /mnt/data/Project/zetryn/scanner
grep -v '^BIRDEYE_API_KEY=' .env > .env.tmp && mv .env.tmp .env
echo "ROUTER_API_KEY=<the user's Router consumer key>" >> .env
```

Check the baseline: `redis-cli xlen scanner:events:new_token`

- [ ] **Step 3: Run the scanner locally**

```bash
cd /mnt/data/Project/zetryn/scanner
set -a && . ./.env && set +a
nohup python -m scanner.main > /tmp/scanner-local-router-integration.log 2>&1 &
```

Within the first ~10-15 seconds, check the log does NOT contain "Birdeye disabled":
```bash
grep -i "birdeye disabled" /tmp/scanner-local-router-integration.log || echo "OK: Birdeye is active"
```
Expected: `OK: Birdeye is active` (if "Birdeye disabled: no Router API key" appears instead, the `.env` wasn't loaded correctly — stop and fix before proceeding).

- [ ] **Step 4: Observe for 10+ minutes**

Run (after waiting 10+ minutes):
```bash
redis-cli xlen scanner:events:new_token
redis-cli xrevrange scanner:events:new_token + - COUNT 30 | grep -A1 '"source"' | grep -oE 'pumpportal|geckoterminal|birdeye' | sort | uniq -c
```
Expected: `xlen` count higher than the baseline from Step 2, and the `uniq -c` output shows all three source values present, confirming `birdeye` entries are still arriving — now proven to be flowing through Router's proxy.

- [ ] **Step 5: Confirm zero crashes**

Run: `tail -30 /tmp/scanner-local-router-integration.log`
Expected: only `INFO` connect/poll logs and, if any, `WARNING` poll-failure logs — no unhandled tracebacks, no `401` errors repeating (a repeating 401 would indicate the Router consumer key is invalid or `require_api_key` changed).

- [ ] **Step 6: Deploy to VPS**

Copy the updated project to the VPS (tarball + scp to `/opt/zetryn-scanner`, same pattern as sources #2 and #3), then on the VPS:
```bash
cd /opt/zetryn-scanner
export PATH="$HOME/.local/bin:$PATH"
uv sync
```

Swap the VPS `.env`:
```bash
grep -v '^BIRDEYE_API_KEY=' .env > .env.tmp && mv .env.tmp .env
echo "ROUTER_API_KEY=<the user's Router consumer key>" >> .env
```

Restart using the confirmed-necessary pattern (not `pm2 restart`):
```bash
pm2 delete zetryn-scanner
set -a && . ./.env && set +a
pm2 start ecosystem.config.js
pm2 save
```
Expected: `pm2 describe zetryn-scanner` shows `status: online`.

- [ ] **Step 7: Verify on VPS over 10+ minutes**

Same checks as Steps 3-5, run against the VPS: confirm no "Birdeye disabled" line, `redis-cli xlen scanner:events:new_token` growth over 10+ minutes with all three source values present in recent entries, and `pm2 describe zetryn-scanner | grep restarts` showing no unexpected restarts beyond the one from this deploy.

No commit for this task — it is a deployment and verification checkpoint only.
