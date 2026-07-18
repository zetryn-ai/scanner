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
