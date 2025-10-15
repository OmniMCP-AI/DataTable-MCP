"""
Service Factory - Framework-Agnostic Google OAuth Service Creation

This module provides reusable functions to create authenticated Google API service objects
without any FastMCP dependencies. Can be used in any Python project.

Usage:
    from datatable_tools.auth.service_factory import create_google_service

    service = create_google_service(
        refresh_token="your_refresh_token",
        client_id="your_client_id",
        client_secret="your_client_secret"
    )

    # Use the service with any Google API
    result = service.spreadsheets().values().get(...).execute()
"""

import logging
from typing import Any, Optional
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


def create_google_service(
    refresh_token: str,
    client_id: str,
    client_secret: str,
    service_name: str = "sheets",
    api_version: str = "v4"
) -> Any:
    """
    Create authenticated Google API service using OAuth credentials.

    This is a framework-agnostic function that works without FastMCP.
    It uses the standard Google OAuth2 library to create credentials
    and build a service object.

    Args:
        refresh_token: OAuth refresh token for persistent authentication
        client_id: OAuth client ID from Google Cloud Console
        client_secret: OAuth client secret from Google Cloud Console
        service_name: Google API service name (default: "sheets")
        api_version: API version to use (default: "v4")

    Returns:
        Authenticated Google API service object (e.g., Google Sheets service)

    Raises:
        ValueError: If any required credentials are missing or invalid
        Exception: If service creation fails

    Examples:
        # Create Google Sheets service
        service = create_google_service(
            refresh_token=os.getenv("GOOGLE_OAUTH_REFRESH_TOKEN"),
            client_id=os.getenv("GOOGLE_OAUTH_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
        )

        # Use the service
        result = await asyncio.to_thread(
            service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range="A1:D10"
            ).execute
        )

        # Create Google Drive service
        drive_service = create_google_service(
            refresh_token=token,
            client_id=client_id,
            client_secret=secret,
            service_name="drive",
            api_version="v3"
        )
    """
    # Validate required credentials
    if not refresh_token:
        raise ValueError("Missing refresh_token. Cannot authenticate without OAuth refresh token.")

    if not client_id:
        raise ValueError("Missing client_id. Cannot authenticate without OAuth client ID.")

    if not client_secret:
        raise ValueError("Missing client_secret. Cannot authenticate without OAuth client secret.")

    try:
        # Create OAuth credentials using standard Google library
        # The token field is None initially - it will be auto-refreshed on first use
        creds = Credentials(
            token=None,  # Access token (will be auto-refreshed)
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret
        )

        logger.info(f"Creating Google {service_name} service (API version: {api_version})")

        # Build Google API service
        service = build(service_name, api_version, credentials=creds)

        logger.info(f"Successfully created Google {service_name} service")

        return service

    except Exception as e:
        logger.error(f"Failed to create Google {service_name} service: {e}")
        raise Exception(f"Service creation failed: {e}") from e


def create_google_service_from_env(
    service_name: str = "sheets",
    api_version: str = "v4",
    env_prefix: str = "GOOGLE_OAUTH"
) -> Any:
    """
    Create Google API service using environment variables.

    Convenience wrapper around create_google_service() that reads
    OAuth credentials from environment variables.

    Args:
        service_name: Google API service name (default: "sheets")
        api_version: API version to use (default: "v4")
        env_prefix: Prefix for environment variables (default: "GOOGLE_OAUTH")

    Returns:
        Authenticated Google API service object

    Raises:
        ValueError: If required environment variables are missing

    Environment Variables Required:
        - {env_prefix}_REFRESH_TOKEN
        - {env_prefix}_CLIENT_ID
        - {env_prefix}_CLIENT_SECRET

    Examples:
        # Set environment variables
        export GOOGLE_OAUTH_REFRESH_TOKEN="your_token"
        export GOOGLE_OAUTH_CLIENT_ID="your_client_id"
        export GOOGLE_OAUTH_CLIENT_SECRET="your_secret"

        # Create service
        service = create_google_service_from_env()

        # With custom prefix
        export TEST_GOOGLE_OAUTH_REFRESH_TOKEN="..."
        service = create_google_service_from_env(env_prefix="TEST_GOOGLE_OAUTH")
    """
    import os

    # Read credentials from environment
    refresh_token = os.getenv(f"{env_prefix}_REFRESH_TOKEN")
    client_id = os.getenv(f"{env_prefix}_CLIENT_ID")
    client_secret = os.getenv(f"{env_prefix}_CLIENT_SECRET")

    # Validate all credentials are present
    missing = []
    if not refresh_token:
        missing.append(f"{env_prefix}_REFRESH_TOKEN")
    if not client_id:
        missing.append(f"{env_prefix}_CLIENT_ID")
    if not client_secret:
        missing.append(f"{env_prefix}_CLIENT_SECRET")

    if missing:
        raise ValueError(
            f"Missing required environment variables:\n" +
            "\n".join(f"- {var}" for var in missing)
        )

    # Create and return service
    return create_google_service(
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        service_name=service_name,
        api_version=api_version
    )


def create_google_service_from_dict(
    auth_info: dict,
    service_name: str = "sheets",
    api_version: str = "v4"
) -> Any:
    """
    Create Google API service from auth info dictionary.

    Convenience wrapper for creating service from OAuth info dict
    (e.g., from omnimcp_be API response).

    Args:
        auth_info: Dictionary containing OAuth credentials with keys:
                   - GOOGLE_OAUTH_REFRESH_TOKEN
                   - GOOGLE_OAUTH_CLIENT_ID
                   - GOOGLE_OAUTH_CLIENT_SECRET
        service_name: Google API service name (default: "sheets")
        api_version: API version to use (default: "v4")

    Returns:
        Authenticated Google API service object

    Raises:
        ValueError: If required keys are missing from auth_info
        KeyError: If expected keys are not found

    Examples:
        # From omnimcp_be API response
        oauth_result = await query_user_oauth_info_by_sse(sse_url, provider)
        auth_info = oauth_result.get('auth_info', {})

        service = create_google_service_from_dict(auth_info)

        # Direct dictionary
        auth_info = {
            'GOOGLE_OAUTH_REFRESH_TOKEN': 'token',
            'GOOGLE_OAUTH_CLIENT_ID': 'client_id',
            'GOOGLE_OAUTH_CLIENT_SECRET': 'secret'
        }
        service = create_google_service_from_dict(auth_info)
    """
    # Extract credentials from dictionary
    try:
        refresh_token = auth_info['GOOGLE_OAUTH_REFRESH_TOKEN']
        client_id = auth_info['GOOGLE_OAUTH_CLIENT_ID']
        client_secret = auth_info['GOOGLE_OAUTH_CLIENT_SECRET']
    except KeyError as e:
        raise ValueError(
            f"Missing required key in auth_info: {e.args[0]}\n"
            f"Expected keys: GOOGLE_OAUTH_REFRESH_TOKEN, GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET"
        ) from e

    # Create and return service
    return create_google_service(
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        service_name=service_name,
        api_version=api_version
    )
