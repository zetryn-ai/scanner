from datetime import datetime

from scanner.events import ScannerEvent, parse_pumpportal_message


def test_scanner_event_requires_all_fields():
    event = ScannerEvent(
        event_type="new_token",
        source="pumpportal",
        mint="ABC123mintaddress",
        raw={"foo": "bar"},
        received_at="2026-07-18T00:00:00+00:00",
    )
    assert event.event_type == "new_token"
    assert event.mint == "ABC123mintaddress"


def test_parse_new_token_payload():
    # Shape based on PumpPortal's documented subscribeNewToken payload.
    payload = {
        "txType": "create",
        "mint": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "name": "Example Token",
        "symbol": "EXPL",
    }
    event = parse_pumpportal_message(payload)
    assert event is not None
    assert event.event_type == "new_token"
    assert event.source == "pumpportal"
    assert event.mint == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    assert event.raw == payload
    # received_at must be a parseable ISO8601 UTC timestamp
    parsed = datetime.fromisoformat(event.received_at)
    assert parsed.tzinfo is not None


def test_parse_migration_payload():
    payload = {
        "txType": "migrate",
        "mint": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "pool": "raydium",
    }
    event = parse_pumpportal_message(payload)
    assert event is not None
    assert event.event_type == "migration"
    assert event.mint == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"


def test_parse_unknown_tx_type_returns_none():
    payload = {"txType": "trade", "mint": "someMint"}
    assert parse_pumpportal_message(payload) is None


def test_parse_missing_mint_returns_none():
    payload = {"txType": "create", "name": "No Mint Field"}
    assert parse_pumpportal_message(payload) is None


def test_parse_non_dict_returns_none():
    assert parse_pumpportal_message("not a dict") is None
    assert parse_pumpportal_message(None) is None
    assert parse_pumpportal_message([1, 2, 3]) is None


def test_event_type_constants_match_expected_values():
    from scanner.events import (
        EVENT_TYPE_MIGRATION,
        EVENT_TYPE_NEW_TOKEN,
        SOURCE_GECKOTERMINAL,
        SOURCE_PUMPPORTAL,
    )

    assert EVENT_TYPE_NEW_TOKEN == "new_token"
    assert EVENT_TYPE_MIGRATION == "migration"
    assert SOURCE_PUMPPORTAL == "pumpportal"
    assert SOURCE_GECKOTERMINAL == "geckoterminal"


def test_scanner_event_accepts_any_string_event_type_and_source():
    # event_type/source are now open str fields, not closed Literals — a
    # future source (e.g. a third scanner) must be representable without
    # a schema change.
    event = ScannerEvent(
        event_type="price_update",
        source="some_future_source",
        mint="ABC123mintaddress",
        raw={},
        received_at="2026-07-18T00:00:00+00:00",
    )
    assert event.event_type == "price_update"
    assert event.source == "some_future_source"
