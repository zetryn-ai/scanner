import asyncio
import json
import logging
import sys
import time

from scanner.config import PUBLISHER_BUFFER_SIZE, load_redis_url
from scanner.geckoterminal import run_geckoterminal_scanner
from scanner.publisher import Publisher
from scanner.pumpportal import run_pumpportal_scanner


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
    logger = logging.getLogger("scanner.main")

    redis_url = load_redis_url()
    logger.info("starting scanner, redis_url=%s", redis_url)

    publisher = Publisher(redis_url, buffer_size=PUBLISHER_BUFFER_SIZE)
    try:
        await asyncio.gather(
            run_pumpportal_scanner(publisher),
            run_geckoterminal_scanner(publisher),
        )
    finally:
        await publisher.close()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
