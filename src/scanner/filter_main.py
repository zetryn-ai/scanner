import asyncio
import json
import logging
import os
import sys
import time

import redis.asyncio as redis

from scanner.config import load_redis_url
from scanner.filter_runner import run_filter
from scanner.strategy_config import load_strategies


class _JsonLinesFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
        }
        return json.dumps(payload)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonLinesFormatter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


async def _main() -> None:
    _configure_logging()
    logger = logging.getLogger("scanner.filter_main")

    redis_url = load_redis_url()
    config_path = os.environ.get("STRATEGIES_CONFIG_PATH", "./strategies.json")
    strategies = load_strategies(config_path)
    logger.info("starting filter, redis_url=%s, strategies=%d", redis_url, len(strategies))

    redis_client = redis.from_url(redis_url)
    try:
        await run_filter(redis_client, strategies)
    finally:
        await redis_client.aclose()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
