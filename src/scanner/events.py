from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel


class ScannerEvent(BaseModel):
    event_type: Literal["new_token", "migration"]
    source: Literal["pumpportal"]
    mint: str
    raw: dict
    received_at: str


# PumpPortal's txType field values that map to each event_type we care about.
# Any other txType (e.g. "buy", "sell", "trade") is not relevant to this
# phase and is intentionally ignored, not an error.
_TX_TYPE_TO_EVENT_TYPE: dict[str, Literal["new_token", "migration"]] = {
    "create": "new_token",
    "migrate": "migration",
}


def parse_pumpportal_message(payload: object) -> ScannerEvent | None:
    """Parse a raw PumpPortal WebSocket message into a ScannerEvent.

    Returns None (never raises) for any payload that isn't a dict, doesn't
    have a recognized txType, or is missing a mint address — the caller is
    expected to log and skip rather than crash the read loop on bad data.
    """
    if not isinstance(payload, dict):
        return None

    tx_type = payload.get("txType")
    event_type = _TX_TYPE_TO_EVENT_TYPE.get(tx_type)
    if event_type is None:
        return None

    mint = payload.get("mint")
    if not isinstance(mint, str) or not mint:
        return None

    return ScannerEvent(
        event_type=event_type,
        source="pumpportal",
        mint=mint,
        raw=payload,
        received_at=datetime.now(timezone.utc).isoformat(),
    )
