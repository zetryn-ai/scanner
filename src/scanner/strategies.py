import re

from pydantic import BaseModel, field_validator

from scanner.events import ScannerEvent

_NAME_RE = re.compile(r"^[a-z0-9_-]+$")


class Strategy(BaseModel):
    name: str
    source_allowlist: list[str] | None = None
    event_types: list[str] | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError(f"strategy name must match [a-z0-9_-]+, got {v!r}")
        return v


def event_matches_strategy(event: ScannerEvent, strategy: Strategy) -> bool:
    """Pure per-event pass/reject. A None field means 'don't filter on it'
    (match anything). Both conditions are AND-ed."""
    if strategy.source_allowlist is not None and event.source not in strategy.source_allowlist:
        return False
    if strategy.event_types is not None and event.event_type not in strategy.event_types:
        return False
    return True


def strategy_stream_name(strategy: Strategy) -> str:
    return f"scanner:strategy:{strategy.name}"
