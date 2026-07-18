from datetime import datetime, timezone

from pydantic import BaseModel

EVENT_TYPE_NEW_TOKEN = "new_token"
EVENT_TYPE_MIGRATION = "migration"

SOURCE_PUMPPORTAL = "pumpportal"
SOURCE_GECKOTERMINAL = "geckoterminal"


class ScannerEvent(BaseModel):
    event_type: str
    source: str
    mint: str
    raw: dict
    received_at: str


# PumpPortal's txType field values that map to each event_type we care about.
# Any other txType (e.g. "buy", "sell", "trade") is not relevant to this
# phase and is intentionally ignored, not an error.
_TX_TYPE_TO_EVENT_TYPE: dict[str, str] = {
    "create": EVENT_TYPE_NEW_TOKEN,
    "migrate": EVENT_TYPE_MIGRATION,
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
        source=SOURCE_PUMPPORTAL,
        mint=mint,
        raw=payload,
        received_at=datetime.now(timezone.utc).isoformat(),
    )
