"""
app/logging_config.py
---------------------
Production-safe logging setup.

- Writes to logs/api.log (rotating, max 10 MB, keeps 5 backups)
- Also writes to stdout for systemd / docker log capture
- Thread-safe (Python's logging module is thread-safe by default)
- No sensitive data logged (passwords, tokens scrubbed at call sites)
- Suppresses noisy third-party loggers (uvicorn.access, sqlalchemy.engine)
- Single call: setup_logging() — called once at app startup
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

LOG_DIR  = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "api.log")

LOG_FORMAT  = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Third-party loggers to silence or reduce
NOISY_LOGGERS = {
    "uvicorn":             logging.WARNING,
    "uvicorn.access":      logging.WARNING,   # suppress per-request access logs
    "uvicorn.error":       logging.WARNING,
    "fastapi":             logging.WARNING,
    "sqlalchemy.engine":   logging.WARNING,   # suppress SQL echo
    "sqlalchemy.pool":     logging.WARNING,
    "asyncio":             logging.WARNING,
    "multipart":           logging.WARNING,
    "watchfiles":          logging.WARNING,
}


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Configure root + app logger. Safe to call multiple times (idempotent).
    Returns the main 'data_ingestion' logger.
    """
    # Create logs directory if needed
    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # ── rotating file handler ──
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,   # 10 MB per file
        backupCount=5,               # keep 5 rotated files
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    # ── stdout handler (for systemd / docker) ──
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    # ── app logger ──
    app_logger = logging.getLogger("data_ingestion")
    if not app_logger.handlers:        # avoid duplicate handlers on reload
        app_logger.setLevel(level)
        app_logger.addHandler(file_handler)
        app_logger.addHandler(console_handler)
        app_logger.propagate = False   # don't bubble up to root

    # ── suppress noisy third-party loggers ──
    for name, lvl in NOISY_LOGGERS.items():
        logging.getLogger(name).setLevel(lvl)

    return app_logger


# Module-level logger for import convenience
logger = logging.getLogger("data_ingestion")
