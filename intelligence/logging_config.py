"""
Logging configuration for the Sparkle & Shine intelligence layer.
Every module calls setup_logging(__name__) at the top to get a configured logger.
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import date


def setup_logging(module_name: str) -> logging.Logger:
    """
    Returns a configured logger for the given module name.

    Log output:
    - Console: INFO level
    - File (logs/intelligence_{date}.log): DEBUG level, rotates daily, keeps 14 days
    """
    logger = logging.getLogger(module_name)

    # Avoid adding duplicate handlers if called more than once in the same process
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")

    # --- Console handler (INFO) ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    # --- File handler (DEBUG, daily rotation, 14-day retention) ---
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    log_filename = os.path.join(logs_dir, f"intelligence_{date.today().strftime('%Y-%m-%d')}.log")
    file_handler = TimedRotatingFileHandler(
        log_filename,
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
