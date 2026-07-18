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
