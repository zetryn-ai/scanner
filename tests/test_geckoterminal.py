from datetime import datetime

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_GECKOTERMINAL
from scanner.geckoterminal import parse_geckoterminal_pool

# Shape verified via a live fetch to
# https://api.geckoterminal.com/api/v2/networks/solana/new_pools?include=base_token,quote_token
# (July 2026).
SAMPLE_POOL = {
    "id": "solana_8R6B7bC57N3SpZFSt9FGgGq9ZnweAW8w1aauUkDSQZoG",
    "type": "pool",
    "attributes": {
        "address": "8R6B7bC57N3SpZFSt9FGgGq9ZnweAW8w1aauUkDSQZoG",
        "name": "MOG / SOL",
        "pool_created_at": "2026-07-18T13:56:53Z",
        "reserve_in_usd": "1660.49958411643",
    },
    "relationships": {
        "base_token": {
            "data": {"id": "solana_6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump", "type": "token"}
        },
        "quote_token": {
            "data": {"id": "solana_So11111111111111111111111111111111111111112", "type": "token"}
        },
        "dex": {"data": {"id": "pump-fun", "type": "dex"}},
    },
}

SAMPLE_INCLUDED_BY_ID = {
    "solana_6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump": {
        "id": "solana_6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump",
        "type": "token",
        "attributes": {
            "address": "6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump",
            "name": "Mog Coin",
            "symbol": "MOG",
        },
    },
    "solana_So11111111111111111111111111111111111111112": {
        "id": "solana_So11111111111111111111111111111111111111112",
        "type": "token",
        "attributes": {
            "address": "So11111111111111111111111111111111111111112",
            "name": "Wrapped SOL",
            "symbol": "SOL",
        },
    },
}


def test_parse_valid_pool_resolves_mint_from_included():
    event = parse_geckoterminal_pool(SAMPLE_POOL, SAMPLE_INCLUDED_BY_ID)
    assert event is not None
    assert event.event_type == EVENT_TYPE_NEW_TOKEN
    assert event.source == SOURCE_GECKOTERMINAL
    assert event.mint == "6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump"
    assert event.raw == SAMPLE_POOL
    parsed = datetime.fromisoformat(event.received_at)
    assert parsed.tzinfo is not None


def test_parse_missing_base_token_relationship_returns_none():
    pool = {"id": "solana_x", "attributes": {}, "relationships": {}}
    assert parse_geckoterminal_pool(pool, SAMPLE_INCLUDED_BY_ID) is None


def test_parse_base_token_not_in_included_returns_none():
    pool = {
        "id": "solana_x",
        "attributes": {},
        "relationships": {
            "base_token": {"data": {"id": "solana_not_in_included", "type": "token"}}
        },
    }
    assert parse_geckoterminal_pool(pool, SAMPLE_INCLUDED_BY_ID) is None


def test_parse_included_item_missing_address_returns_none():
    pool = {
        "id": "solana_x",
        "attributes": {},
        "relationships": {
            "base_token": {"data": {"id": "solana_no_address", "type": "token"}}
        },
    }
    included = {"solana_no_address": {"id": "solana_no_address", "type": "token", "attributes": {}}}
    assert parse_geckoterminal_pool(pool, included) is None


def test_parse_non_dict_pool_returns_none():
    assert parse_geckoterminal_pool("not a dict", SAMPLE_INCLUDED_BY_ID) is None
    assert parse_geckoterminal_pool(None, SAMPLE_INCLUDED_BY_ID) is None


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


async def test_run_geckoterminal_scanner_publishes_parsed_pools():
    payload = {"data": [SAMPLE_POOL], "included": list(SAMPLE_INCLUDED_BY_ID.values())}

    async def fake_http_get_fn(_url, _params):
        return _FakeResponse(payload)

    publisher = _RecordingPublisher()

    from scanner.geckoterminal import run_geckoterminal_scanner

    await run_geckoterminal_scanner(
        publisher,
        http_get_fn=fake_http_get_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert len(publisher.published) == 1
    assert publisher.published[0].mint == "6cxi1YrhejBBFg6rRQGj3F3KDUoUZpLPYXarQwL2pump"


async def test_run_geckoterminal_scanner_skips_unparseable_pools_without_crashing():
    payload = {
        "data": [{"id": "solana_broken", "attributes": {}, "relationships": {}}],
        "included": [],
    }

    async def fake_http_get_fn(_url, _params):
        return _FakeResponse(payload)

    publisher = _RecordingPublisher()

    from scanner.geckoterminal import run_geckoterminal_scanner

    await run_geckoterminal_scanner(
        publisher,
        http_get_fn=fake_http_get_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert publisher.published == []


async def test_run_geckoterminal_scanner_continues_after_http_error():
    call_count = 0

    async def fake_http_get_fn(_url, _params):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _FakeResponse({}, status_ok=False)
        return _FakeResponse({"data": [SAMPLE_POOL], "included": list(SAMPLE_INCLUDED_BY_ID.values())})

    publisher = _RecordingPublisher()

    from scanner.geckoterminal import run_geckoterminal_scanner

    await run_geckoterminal_scanner(
        publisher,
        http_get_fn=fake_http_get_fn,
        max_iterations=2,
        _sleep_fn=_no_op_sleep,
    )

    assert call_count == 2
    assert len(publisher.published) == 1
