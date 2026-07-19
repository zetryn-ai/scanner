import json

from scanner.strategy_config import load_strategies


def test_load_valid_config(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text(json.dumps([
        {"name": "birdeye-only", "source_allowlist": ["birdeye"]},
        {"name": "launches", "event_types": ["new_token"]},
        {"name": "everything"},
    ]))
    strategies = load_strategies(str(p))
    assert [s.name for s in strategies] == ["birdeye-only", "launches", "everything"]
    assert strategies[0].source_allowlist == ["birdeye"]


def test_missing_file_returns_empty(tmp_path):
    assert load_strategies(str(tmp_path / "does-not-exist.json")) == []


def test_empty_list_returns_empty(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text("[]")
    assert load_strategies(str(p)) == []


def test_malformed_entry_is_skipped_valid_ones_kept(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text(json.dumps([
        {"name": "good"},
        {"name": "Bad Name With Spaces"},   # invalid name -> skipped
        {"source_allowlist": ["birdeye"]},   # missing name -> skipped
        {"name": "also-good"},
    ]))
    strategies = load_strategies(str(p))
    assert [s.name for s in strategies] == ["good", "also-good"]


def test_completely_unparseable_file_returns_empty(tmp_path):
    p = tmp_path / "strategies.json"
    p.write_text("this is not json {{{")
    assert load_strategies(str(p)) == []
