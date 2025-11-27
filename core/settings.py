"""
Settings and configuration for DataTable MCP Server
"""
import os
from enum import Enum
from pathlib import Path


class Env(str, Enum):
    """Environment types"""
    Local = "local"
    Development = "development"
    Production = "production"


class Settings:
    """Application settings"""

    def __init__(self):
        # Environment
        self.env = os.getenv("ENV", Env.Local)

        # Logging configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_folder = os.getenv("LOG_FOLDER", "logs")

        # Ensure log folder exists (with error handling)
        try:
            os.makedirs(self.log_folder, exist_ok=True)
        except (OSError, PermissionError) as e:
            # If we can't create the configured log folder, try a fallback
            print(f"⚠️  Warning: Cannot create log folder '{self.log_folder}': {e}")

            # Try common fallback locations
            fallback_folders = [
                "/tmp/datatable-mcp-logs",  # Unix/Linux/Mac fallback
                "~/datatable-mcp-logs",  # Home directory fallback
                "./logs"  # Current directory fallback
            ]

            folder_created = False
            for fallback in fallback_folders:
                try:
                    expanded_path = os.path.expanduser(fallback)
                    os.makedirs(expanded_path, exist_ok=True)
                    self.log_folder = expanded_path
                    print(f"✅ Using fallback log folder: {self.log_folder}")
                    folder_created = True
                    break
                except (OSError, PermissionError):
                    continue

            if not folder_created:
                # If all fallbacks fail, disable file logging by setting to None
                print(f"⚠️  Warning: All log folder locations failed. File logging will be disabled.")
                self.log_folder = None

    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.env == Env.Production

    @property
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.env == Env.Development

    @property
    def is_local(self) -> bool:
        """Check if running in local environment"""
        return self.env == Env.Local


# Global settings instance
SETTINGS = Settings()
