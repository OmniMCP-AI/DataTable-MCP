"""
MCP++ Bridge Layer - Enhanced MCP tool calling with DataFrame/Series/Scalar support

This module provides a bridge between high-level data types (DataFrame, Series, scalars)
and MCP tools that expect primitive types (lists, dicts, etc.).

Usage pattern mirrors MCP ClientSession:
    from mcp.client.sse import sse_client
    from fastestai.tools.mcpplus import MCPPlus

    async with sse_client(url=url, headers=headers) as (read, write):
        async with MCPPlus(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            result = await session.call_tool("write_new_sheet", {"data": df})
"""

import mcp.types as types
from typing import Any, Dict, List, Optional
import polars as pl
import numpy as np
import httpx
from mcp import ClientSession
import logging

# Optional SETTINGS import for backward compatibility
try:
    from fastestai.settings import SETTINGS
except ImportError:
    # Create a mock SETTINGS object when fastestai is not available
    class MockSettings:
        class Tool:
            url = "https://be.omnimcp.ai"
        tool = Tool()
    SETTINGS = MockSettings()

logger = logging.getLogger(__name__)


class MCPPlus:
    """
    MCP++ Bridge that wraps MCP ClientSession with automatic DataFrame/Series/scalar conversion.

    Usage mirrors standard MCP ClientSession pattern:
        async with MCPPlus(read, write) as session:
            await session.initialize()
            result = await session.call_tool("write_new_sheet", {"data": df})
    """

    def __init__(self, read, write):
        """
        Initialize MCP++ with read/write transports.

        Args:
            read: Read stream from sse_client
            write: Write stream from sse_client
        """
        self._read = read
        self._write = write
        self._session: Optional[ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry - creates underlying MCP session"""
        self._session = ClientSession(self._read, self._write)
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleans up underlying MCP session"""
        if self._session:
            await self._session.__aexit__(exc_type, exc_val, exc_tb)

    async def initialize(self):
        """Initialize the MCP session (required before calling tools)"""
        if not self._session:
            raise RuntimeError("MCPPlus must be used as async context manager")
        await self._session.initialize()

    async def list_tools(self):
        """List available MCP tools"""
        if not self._session:
            raise RuntimeError("MCPPlus must be used as async context manager")
        return await self._session.list_tools()

    async def call_tool(self, tool_name: str, arguments: Optional[Dict[str, Any]] = None) -> types.CallToolResult:
        """
        Call an MCP tool with automatic DataFrame/Series/scalar conversion.

        Args:
            tool_name: Name of the MCP tool to call
            arguments: Dictionary of arguments (can include DataFrame/Series/scalars)

        Returns:
            Result from the MCP tool call

        Examples:
            # DataFrame example
            df = pd.DataFrame({'name': ['Alice', 'Bob'], 'dept': ['HR', 'IT']})
            result = await session.call_tool('write_new_sheet', {
                'data': df,
                'headers': ['name', 'department']
            })
        """
        if not self._session:
            raise RuntimeError("MCPPlus must be used as async context manager")

        # Convert arguments containing DataFrames/Series/scalars
        converted_args = self._convert_args(arguments or {})

        # Call the underlying MCP tool
        return await self._session.call_tool(tool_name, converted_args)

    def _convert_args(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert arguments containing DataFrames/Series/scalars to MCP-compatible types.

        Args:
            args: Dictionary of arguments that may contain DataFrames/Series/scalars

        Returns:
            Dictionary with converted arguments
        """
        converted = {}

        for key, value in args.items():
            converted[key] = self._convert_value(value)

        return converted

    def _convert_value(self, value: Any) -> Any:
        """
        Convert a single value to MCP-compatible format.

        Conversion rules:
        - DataFrame -> List[List[Any]] (2D array)
        - Series -> List[Any] (1D array)
        - numpy scalars -> Python native types
        - numpy arrays -> nested lists
        - Other types -> pass through unchanged

        Args:
            value: Value to convert

        Returns:
            Converted value compatible with MCP tools
        """
        # DataFrame -> List[List[Any]]
        if isinstance(value, pl.DataFrame):
            return value.rows()

        # Series -> List[Any]
        if isinstance(value, pl.Series):
            return value.to_list()

        # numpy array -> nested lists
        if isinstance(value, np.ndarray):
            return value.tolist()

        # numpy scalar -> Python native type
        if isinstance(value, (np.integer, np.floating, np.bool_)):
            return value.item()

        # Handle nested structures (lists/dicts containing DataFrames)
        if isinstance(value, list):
            return [self._convert_value(item) for item in value]

        if isinstance(value, dict):
            return {k: self._convert_value(v) for k, v in value.items()}

        # Pass through other types unchanged
        return value


async def query_user_oauth_info(user_id: str, provider_name: str, base_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Query OAuth information for a user and provider from omnimcp_be API.

    This function retrieves OAuth credentials that can be used as headers
    for sse_client when connecting to MCP tools.

    Args:
        user_id: The user ID to query OAuth info for
        provider_name: The provider name (e.g., 'google_sheets')
        base_url: Base URL of the omnimcp_be API (default: uses SETTINGS.tool.url)

    Returns:
        Dictionary containing OAuth info that can be used as headers

    Example:
        auth_info = await query_user_oauth_info("685a7a18ec9b2c667f66d4bd", "google_sheets")
        headers = auth_info["auth_info"]  # Use as sse_client headers

    Raises:
        httpx.HTTPError: If the API request fails
        ValueError: If the response indicates failure
    """
    if base_url is None:
        base_url = SETTINGS.tool.url

    url = f"{base_url}/api/v1/user/auth/user/{user_id}/provider/{provider_name}"
    params = {"map_oauth_fields": "true"}

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()

        result = response.json()

        if not result.get("success"):
            raise ValueError(f"OAuth query failed: {result.get('message', 'Unknown error')}")

        return result


async def query_user_oauth_info_by_sse(sse_url: str, provider: str, map_oauth_fields: bool = True, base_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Query OAuth information using SSE URL and provider from omnimcp_be API.

    This function extracts sse_key from the SSE URL and queries OAuth credentials
    that can be used as headers for sse_client.

    Args:
        sse_url: SSE URL (e.g., 'https://be.omnimcp.ai/api/v1/mcp/a6ebdc49-50e7-4c54-8d2a-639f10098a63/68d688ee3bced208d241bef6/sse')
        provider: Provider name (e.g., 'google_sheets')
        map_oauth_fields: Whether to map OAuth fields (default: True)

    Returns:
        Dictionary containing OAuth info that can be used as headers

    Example:
        sse_url = "https://be-dev.omnimcp.ai/api/v1/mcp/a6ebdc49-50e7-4c54-8d2a-639f10098a63/68d688ee3bced208d241bef6/sse"
        auth_info = await query_user_oauth_info_by_sse(sse_url, "google_sheets")
        headers = auth_info["auth_info"]  # Use as sse_client headers

    Raises:
        ValueError: If SSE URL format is invalid or response indicates failure
        httpx.HTTPError: If the API request fails
    """
    # Parse SSE URL to extract sse_key and base_url
    # Expected format: {base_url}/api/v1/mcp/{sse_key}/{server_id}/sse
    import re

    # Extract sse_key from URL pattern
    match = re.search(r'/api/v1/mcp/([^/]+)/([^/]+)/sse', sse_url)
    if not match:
        raise ValueError(f"Invalid SSE URL format: {sse_url}. Expected format: {{base_url}}/api/v1/mcp/{{sse_key}}/{{server_id}}/sse")

    sse_key = match.group(1)

    # Extract base URL (everything before /api/v1/mcp/)
    if base_url is None:
        base_url = SETTINGS.tool.url
    else:
        base_url = sse_url.split('/api/v1/mcp/')[0]

    # Construct the OAuth query URL using the new SSE endpoint
    url = f"{base_url}/api/v1/user/auth/sse/{sse_key}/provider/{provider}"
    params = {"map_oauth_fields": str(map_oauth_fields).lower()}

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()

        result = response.json()

        if not result.get("success"):
            raise ValueError(f"OAuth query failed: {result.get('message', 'Unknown error')}")

        return result


async def call_tool_by_sse(
    sse_url: str,
    tool_name: str,
    args: Optional[Dict[str, Any]] = None,
    direct_call: bool = False
) -> types.CallToolResult:
    """
    Simplified tool calling using SSE URL directly.

    This function supports two modes:
    1. MCP Protocol Mode (direct_call=False, default): Uses MCP protocol via SSE
    2. Direct Call Mode (direct_call=True): Bypasses MCP and calls core API directly

    Args:
        sse_url: SSE URL (e.g., 'https://be-dev.omnimcp.ai/api/v1/mcp/.../sse')
        tool_name: Full MCP tool name (e.g., "google_sheets__update_range")
        args: Tool arguments (can include DataFrame/Series/scalars)
        direct_call: If True, bypass MCP protocol and call core API directly (default: False)

    Returns:
        Result from the tool call (format depends on mode)

    Examples:
        # MCP Protocol Mode (default)
        result = await call_tool_by_sse(
            sse_url=SSE_URL,
            tool_name="google_sheets__update_range",
            args={'uri': uri, 'data': df, 'range_address': 'A1'}
        )

        # Direct Call Mode (NEW in Stage 5)
        result = await call_tool_by_sse(
            sse_url=SSE_URL,
            tool_name="google_sheets__update_range",
            direct_call=True,  # Bypass MCP protocol
            args={'uri': uri, 'data': df, 'range_address': 'A1'}
        )

    Raises:
        ValueError: If SSE URL/tool name format is invalid
        httpx.HTTPError: If OAuth query fails
        RuntimeError: If MCP session initialization fails (MCP mode only)
    """
    from mcp.client.sse import sse_client

    # Parse provider from tool name
    provider = tool_name.split('__')[0]
    method_name = tool_name.split('__')[1]

    # Step 1: Get OAuth credentials using SSE URL
    oauth_result = await query_user_oauth_info_by_sse(
        sse_url=sse_url,
        provider=provider
    )

    # Extract auth_info for use
    auth_info = oauth_result.get('auth_info', {})

    # NEW: Direct call mode - bypass MCP protocol
    if direct_call:
        logger.info(f"Direct call mode: calling {tool_name} directly via core API")

        # Import dependencies for direct call
        from datatable_tools.auth.service_factory import create_google_service_from_dict
        from datatable_tools.tool_registry import get_tool_method

        # Create Google service from OAuth credentials
        service = create_google_service_from_dict(auth_info)

        # Get the actual method to call
        method = get_tool_method(tool_name)

        # Call the method directly with service + args
        result = await method(service=service, **(args or {}))

        # Wrap result in CallToolResult format for consistency
        logger.debug(f"Direct call completed: {result.success}")
        return _wrap_result_as_call_tool_result(result)

    # Existing MCP protocol logic (when direct_call=False)
    logger.info(f"MCP protocol mode: calling {tool_name} via SSE")

    try:
        async with sse_client(url=sse_url, headers=auth_info) as (read, write):
            async with MCPPlus(read, write) as session:
                await session.initialize()
                return await session.call_tool(method_name, args)
    except* Exception as eg:
        # Handle ExceptionGroup from sse_client
        # Re-raise the first exception for debugging
        raise eg.exceptions[0] if eg.exceptions else eg


def _wrap_result_as_call_tool_result(result) -> types.CallToolResult:
    """
    Convert GoogleSheetDataTable result (Pydantic model) to CallToolResult format.

    This ensures that direct call results have the same format as MCP protocol results,
    allowing them to be used interchangeably.

    Args:
        result: Result Pydantic model from GoogleSheetDataTable method
                (TableResponse, UpdateResponse, or SpreadsheetResponse)

    Returns:
        CallToolResult with properly formatted content

    Examples:
        >>> result = UpdateResponse(success=True, message='Updated', ...)
        >>> call_tool_result = _wrap_result_as_call_tool_result(result)
        >>> call_tool_result.isError
        False
        >>> call_tool_result.content[0].text
        '{"success": true, "message": "Updated", ...}'
    """
    import json

    # Convert Pydantic model to dict, then to JSON string
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result.dict()
    text_content = json.dumps(result_dict, indent=2, default=str)

    # Create CallToolResult
    return types.CallToolResult(
        content=[
            types.TextContent(
                type="text",
                text=text_content
            )
        ],
        isError=not result.success
    )
