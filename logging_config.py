import os
import logging
from typing import Optional


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(level: Optional[str] = None) -> None:
    """Initialize application logging once.

    If level is not provided, it is read from the LOG_LEVEL env var (default INFO).
    Safe to call multiple times; it won't re-add handlers if already configured.
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")
    level = level.upper()

    root_logger = logging.getLogger()
    if root_logger.handlers:
        # Already configured; just adjust level
        root_logger.setLevel(getattr(logging, level, logging.INFO))
        return

    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format=LOG_FORMAT,
    )


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger."""
    return logging.getLogger(name)
