import json
import logging
import os

from pydantic import ValidationError

from scanner.strategies import Strategy

logger = logging.getLogger("scanner.strategy_config")


def load_strategies(path: str) -> list[Strategy]:
    """Load strategy definitions from a JSON file (a list of objects).

    Returns [] for a missing, empty, or unparseable file. Individual
    malformed entries are logged and skipped; valid entries still load.
    Never raises — a bad config idles the filter, it doesn't crash it.
    """
    if not os.path.exists(path):
        logger.info("strategies config not found at %s, no strategies configured", path)
        return []

    try:
        with open(path) as f:
            data = json.load(f)
    except (ValueError, OSError) as exc:
        logger.warning("failed to read strategies config %s (%s), treating as empty", path, exc)
        return []

    if not isinstance(data, list):
        logger.warning("strategies config %s is not a list, treating as empty", path)
        return []

    strategies: list[Strategy] = []
    for entry in data:
        try:
            strategies.append(Strategy(**entry))
        except (ValidationError, TypeError) as exc:
            logger.warning("skipping malformed strategy entry %r (%s)", entry, exc)
    return strategies
