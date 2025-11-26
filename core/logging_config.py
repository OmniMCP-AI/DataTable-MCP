"""
Centralized logging configuration for DataTable MCP Server
Import logger from this module in all files to ensure consistent logging.
"""
import os
import logging
from datetime import datetime, timezone
from pathlib import Path

import structlog
from structlog.processors import JSONRenderer
from structlog.stdlib import BoundLogger

from core.settings import SETTINGS, Env


def configure_logging():
    """
    Configure structlog and integrate with stdlib logging.
    This ensures both structlog and standard logging.getLogger() write to the same destination.
    """
    # Get root logger and remove all existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Configure structlog based on environment
    if os.getenv("ENV", Env.Local) in [Env.Production, Env.Development]:
        # File logging for production and development
        log_name = f"{datetime.now(tz=timezone.utc).strftime('%Y%m%d')}"
        log_file = os.path.join(SETTINGS.log_folder, log_name)

        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        print(f"📝 Log file path: {log_file}.log")
        print(f"📝 Console logging disabled - all logs going to file only")

        # Configure structlog for file logging with JSON format
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        # Configure stdlib logging to write ONLY to file (no console)
        file_handler = logging.FileHandler(Path(log_file).with_suffix(".log"), mode="a")
        file_handler.setLevel(SETTINGS.log_level)

        # Use structlog formatter for stdlib logging
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=JSONRenderer(),
            foreign_pre_chain=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
            ],
        )
        file_handler.setFormatter(formatter)

        # Configure root logger - ONLY file handler, no console
        root_logger.addHandler(file_handler)
        root_logger.setLevel(SETTINGS.log_level)

        # Disable propagation to avoid duplicate logging
        root_logger.propagate = False

        # Also configure third-party library loggers to use file only
        for logger_name in ['uvicorn', 'uvicorn.access', 'uvicorn.error', 'fastapi', 'mcp', 'mcp.server']:
            lib_logger = logging.getLogger(logger_name)
            lib_logger.handlers.clear()
            lib_logger.addHandler(file_handler)
            lib_logger.setLevel(SETTINGS.log_level)
            lib_logger.propagate = False

    else:
        # Console logging for local development
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.dev.set_exc_info,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        # Configure console handler for stdlib logging
        console_handler = logging.StreamHandler()
        console_handler.setLevel(SETTINGS.log_level)

        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(),
            foreign_pre_chain=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
            ],
        )
        console_handler.setFormatter(formatter)

        # Configure root logger
        root_logger.addHandler(console_handler)
        root_logger.setLevel(SETTINGS.log_level)


def get_logger(name: str = None) -> BoundLogger:
    """
    Get a structlog logger instance.

    Usage in any file:
        from core.logging_config import get_logger
        logger = get_logger(__name__)
        logger.info("message", key="value")

    Args:
        name: Logger name (typically __name__ of the module)

    Returns:
        Structured logger instance
    """
    return structlog.get_logger(name)


# For backward compatibility with existing code using logging.getLogger()
# After calling configure_logging(), both approaches will work:
# 1. logger = get_logger(__name__)  # structlog
# 2. logger = logging.getLogger(__name__)  # stdlib logging (will use same config)
