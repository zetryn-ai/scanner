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
