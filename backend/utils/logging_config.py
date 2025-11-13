"""
Logging configuration for the application.
"""

import json
import logging
import logging.handlers
import os
from datetime import datetime
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as JSON."""
        log_object: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_object["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        # Add extra fields if present
        if hasattr(record, "extra"):
            log_object.update(record.extra)

        return json.dumps(log_object)


def setup_logging(
    log_level: str = "INFO",
    log_file: str = "logs/app.log",
    max_bytes: int = 10485760,  # 10MB
    backup_count: int = 5,
) -> None:
    """
    Set up application logging with both file and console handlers.

    Args:
        log_level: Logging level (default: INFO)
        log_file: Path to log file (default: logs/app.log)
        max_bytes: Maximum size of log file before rotation (default: 10MB)
        backup_count: Number of backup log files to keep (default: 5)
    """
    # Create logs directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create JSON formatter for file
    json_formatter = JSONFormatter()

    # Create human-readable formatter for console
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Clear any existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # File handler with rotation (JSON format)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    file_handler.setFormatter(json_formatter)
    root_logger.addHandler(file_handler)

    # Console handler for development (human-readable format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    # Force immediate flush for real-time logs
    import sys

    console_handler.stream = sys.stdout
    root_logger.addHandler(console_handler)

    # Create separate loggers for different components
    loggers = {
        "email_generator": logging.getLogger("email_generator"),
        "cache": logging.getLogger("cache"),
        "drive": logging.getLogger("drive"),
        "templates": logging.getLogger("templates"),
        "research": logging.getLogger("research"),
    }

    for logger in loggers.values():
        logger.setLevel(getattr(logging, log_level.upper()))
