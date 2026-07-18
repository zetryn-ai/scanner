from datetime import datetime, timezone

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_BIRDEYE, ScannerEvent


def parse_birdeye_token(item: object) -> ScannerEvent | None:
    """Parse one item from Birdeye's new_listing response into a
    ScannerEvent.

    Unlike GeckoTerminal, Birdeye's response has the mint address directly
    on each item (`address`) — no JSON:API relationship resolution needed.

    Returns None (never raises) if `address` is missing, not a string, or
    empty — the caller logs and skips rather than crashing the polling
    loop.
    """
    if not isinstance(item, dict):
        return None

    mint = item.get("address")
    if not isinstance(mint, str) or not mint:
        return None

    return ScannerEvent(
        event_type=EVENT_TYPE_NEW_TOKEN,
        source=SOURCE_BIRDEYE,
        mint=mint,
        raw=item,
        received_at=datetime.now(timezone.utc).isoformat(),
    )
