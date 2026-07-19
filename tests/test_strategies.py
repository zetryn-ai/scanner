import pytest
from pydantic import ValidationError

from scanner.events import ScannerEvent
from scanner.strategies import Strategy, event_matches_strategy, strategy_stream_name


def make_event(source="pumpportal", event_type="new_token") -> ScannerEvent:
    return ScannerEvent(
        event_type=event_type,
        source=source,
        mint="MINTxxx",
        raw={},
        received_at="2026-07-19T00:00:00+00:00",
    )


def test_no_filters_matches_anything():
    s = Strategy(name="everything")
    assert event_matches_strategy(make_event(), s) is True
    assert event_matches_strategy(make_event(source="birdeye", event_type="migration"), s) is True


def test_source_allowlist_hit_and_miss():
    s = Strategy(name="birdeye-only", source_allowlist=["birdeye"])
    assert event_matches_strategy(make_event(source="birdeye"), s) is True
    assert event_matches_strategy(make_event(source="pumpportal"), s) is False


def test_event_types_hit_and_miss():
    s = Strategy(name="launches", event_types=["new_token"])
    assert event_matches_strategy(make_event(event_type="new_token"), s) is True
    assert event_matches_strategy(make_event(event_type="migration"), s) is False


def test_both_conditions_are_anded():
    s = Strategy(name="bd-launch", source_allowlist=["birdeye"], event_types=["new_token"])
    assert event_matches_strategy(make_event(source="birdeye", event_type="new_token"), s) is True
    # right source, wrong type
    assert event_matches_strategy(make_event(source="birdeye", event_type="migration"), s) is False
    # wrong source, right type
    assert event_matches_strategy(make_event(source="pumpportal", event_type="new_token"), s) is False


def test_stream_name_derivation():
    assert strategy_stream_name(Strategy(name="sniper")) == "scanner:strategy:sniper"


def test_valid_names_accepted():
    for name in ["sniper", "birdeye-only", "launch_2", "abc123"]:
        assert Strategy(name=name).name == name


def test_invalid_names_rejected():
    for name in ["Sniper", "has space", "bad/slash", "colon:name", ""]:
        with pytest.raises(ValidationError):
            Strategy(name=name)
