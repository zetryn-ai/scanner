from datetime import datetime, timezone

from scanner.events import EVENT_TYPE_NEW_TOKEN, SOURCE_GECKOTERMINAL, ScannerEvent

NEW_POOLS_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"


def parse_geckoterminal_pool(pool: object, included_by_id: dict) -> ScannerEvent | None:
    """Parse one pool object from GeckoTerminal's new_pools response into a
    ScannerEvent.

    `pool` is one item from the response's top-level `data` array (JSON:API
    format). `included_by_id` is a lookup built from the response's
    top-level `included` array, keyed by each included item's `id`
    (required to resolve `base_token`'s bare mint address — the
    relationship itself only carries a prefixed id like
    "solana_<mint>", not the resolved token attributes).

    Returns None (never raises) if any expected field is missing —
    the caller logs and skips rather than crashing the polling loop.
    """
    if not isinstance(pool, dict):
        return None

    relationships = pool.get("relationships")
    if not isinstance(relationships, dict):
        return None

    base_token = relationships.get("base_token")
    if not isinstance(base_token, dict):
        return None

    base_token_data = base_token.get("data")
    if not isinstance(base_token_data, dict):
        return None

    base_token_id = base_token_data.get("id")
    if not isinstance(base_token_id, str):
        return None

    included_token = included_by_id.get(base_token_id)
    if not isinstance(included_token, dict):
        return None

    token_attributes = included_token.get("attributes")
    if not isinstance(token_attributes, dict):
        return None

    mint = token_attributes.get("address")
    if not isinstance(mint, str) or not mint:
        return None

    return ScannerEvent(
        event_type=EVENT_TYPE_NEW_TOKEN,
        source=SOURCE_GECKOTERMINAL,
        mint=mint,
        raw=pool,
        received_at=datetime.now(timezone.utc).isoformat(),
    )
