import os

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"

# Exponential backoff for WebSocket reconnects: 1s, 2s, 4s, 8s, 16s, capped at 30s,
# plus up to 20% random jitter. Disconnects are routine (per PumpPortal's own FAQ),
# not exceptional — this loop is expected to run indefinitely.
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_JITTER_FRACTION = 0.2

PUBLISHER_BUFFER_SIZE = 500


def load_redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")


def load_pumpportal_api_key() -> str | None:
    return os.environ.get("PUMPPORTAL_API_KEY") or None


BIRDEYE_NEW_LISTING_URL = "https://public-api.birdeye.so/defi/v2/tokens/new_listing"
BIRDEYE_POLL_INTERVAL_SECONDS = 30.0


def load_birdeye_api_key() -> str | None:
    return os.environ.get("BIRDEYE_API_KEY") or None
