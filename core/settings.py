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

        # Ensure log folder exists
        os.makedirs(self.log_folder, exist_ok=True)

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
