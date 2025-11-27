"""
Centralized logging configuration for DataTable MCP Server
Import logger from this module in all files to ensure consistent logging.
"""
import os
import logging
from datetime import datetime, timezone

import structlog
from structlog.processors import JSONRenderer
from structlog.stdlib import BoundLogger

from core.settings import SETTINGS, Env


def configure_logging():
    """
    Configure structlog and integrate with stdlib logging.
    This ensures both structlog and standard logging.getLogger() write to the same destination.

    Includes robust error handling to prevent crashes from misconfigured logging paths.
    Falls back to console logging if file logging fails.
    """
    # Get root logger and remove all existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Configure structlog based on environment
    if os.getenv("ENV", Env.Local) in [Env.Production, Env.Development]:
        # File logging for production and development
        file_handler = None
        file_logging_enabled = False

        try:
            # Validate and create log folder
            log_folder = SETTINGS.log_folder
            if not log_folder:
                raise ValueError("LOG_FOLDER is empty or not set")

            # Convert to absolute path to avoid relative path issues
            log_folder = os.path.abspath(log_folder)

            # Create log directory with proper error handling
            try:
                os.makedirs(log_folder, exist_ok=True)
            except (OSError, PermissionError) as e:
                raise RuntimeError(f"Cannot create log folder '{log_folder}': {e}")

            # Generate log file path
            log_name = f"{datetime.now(tz=timezone.utc).strftime('%Y%m%d')}"
            log_file = os.path.join(log_folder, f"{log_name}.log")

            # Test write permissions by attempting to create/open the file
            try:
                # Use 'a' mode to create if not exists, or append if exists
                test_handler = logging.FileHandler(log_file, mode="a")
                test_handler.close()

                # If successful, create the actual handler
                file_handler = logging.FileHandler(log_file, mode="a")
                file_handler.setLevel(SETTINGS.log_level)
                file_logging_enabled = True

                print(f"📝 Log file path: {log_file}")
                print(f"📝 Console logging disabled - all logs going to file only")

            except (OSError, PermissionError) as e:
                raise RuntimeError(f"Cannot write to log file '{log_file}': {e}")

        except Exception as e:
            # File logging setup failed - print warning and fall back to console
            print(f"⚠️  Warning: File logging configuration failed: {e}")
            print(f"⚠️  Falling back to console logging for safety")
            print(f"⚠️  Please check LOG_FOLDER setting: '{SETTINGS.log_folder}'")
            file_logging_enabled = False

        if file_logging_enabled and file_handler:
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
            # Fallback to console logging if file logging failed
            _configure_console_logging(root_logger)

    else:
        # Console logging for local development
        _configure_console_logging(root_logger)


def _configure_console_logging(root_logger: logging.Logger):
    """
    Helper function to configure console logging.
    Used for local development and as fallback when file logging fails.

    Args:
        root_logger: The root logger instance to configure
    """
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
