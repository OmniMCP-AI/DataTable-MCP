"""
Tool Registry - Mapping MCP Tool Names to Implementation Methods

This module provides a registry that maps MCP tool names (format: "provider__method")
to their actual implementation methods in GoogleSheetDataTable.

This enables direct calling of core methods when bypassing the MCP protocol layer.

Usage:
    from datatable_tools.tool_registry import get_tool_method, parse_tool_name

    # Parse tool name
    provider, method = parse_tool_name("google_sheets__update_range")
    # Returns: ("google_sheets", "update_range")

    # Get the actual method
    method_func = get_tool_method("google_sheets__update_range")
    # Returns: GoogleSheetDataTable().update_range

    # Call it directly
    result = await method_func(service=service, uri=uri, data=data, range_address="A1")
"""

import logging
from typing import Callable, Tuple
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

logger = logging.getLogger(__name__)


# Singleton instance of GoogleSheetDataTable for registry
_google_sheet_instance = GoogleSheetDataTable()


# Tool name mapping: "provider__method" -> actual method
# This registry enables direct calls bypassing MCP protocol
TOOL_REGISTRY = {
    # Google Sheets tools
    "google_sheets__load_data_table": _google_sheet_instance.load_data_table,
    "google_sheets__update_range": _google_sheet_instance.update_range,
    "google_sheets__append_rows": _google_sheet_instance.append_rows,
    "google_sheets__append_columns": _google_sheet_instance.append_columns,
    "google_sheets__write_new_sheet": _google_sheet_instance.write_new_sheet,
}


def parse_tool_name(tool_name: str) -> Tuple[str, str]:
    """
    Parse MCP tool name into provider and method components.

    MCP tool names follow the format: "provider__method"
    Example: "google_sheets__update_range" -> ("google_sheets", "update_range")

    Args:
        tool_name: Full MCP tool name in "provider__method" format

    Returns:
        Tuple of (provider_name, method_name)

    Raises:
        ValueError: If tool name doesn't follow expected format

    Examples:
        >>> parse_tool_name("google_sheets__update_range")
        ('google_sheets', 'update_range')

        >>> parse_tool_name("google_sheets__write_new_sheet")
        ('google_sheets', 'write_new_sheet')

        >>> parse_tool_name("invalid_format")
        ValueError: Invalid tool name format: invalid_format
    """
    parts = tool_name.split("__", 1)

    if len(parts) != 2:
        raise ValueError(
            f"Invalid tool name format: {tool_name}\n"
            f"Expected format: 'provider__method' (e.g., 'google_sheets__update_range')"
        )

    provider, method = parts[0], parts[1]

    logger.debug(f"Parsed tool name '{tool_name}' -> provider='{provider}', method='{method}'")

    return provider, method


def get_tool_method(tool_name: str) -> Callable:
    """
    Get the actual implementation method for an MCP tool name.

    Looks up the tool name in the registry and returns the corresponding method.
    The returned method can be called directly with service and parameters.

    Args:
        tool_name: Full MCP tool name (e.g., "google_sheets__update_range")

    Returns:
        Callable method from GoogleSheetDataTable that can be invoked

    Raises:
        ValueError: If tool name is not found in registry

    Examples:
        >>> # Get method
        >>> method = get_tool_method("google_sheets__update_range")
        >>> # Call it directly
        >>> result = await method(service=service, uri=uri, data=data, range_address="A1")

        >>> # All available tools
        >>> for tool_name in get_available_tools():
        ...     method = get_tool_method(tool_name)
        ...     print(f"{tool_name} -> {method.__name__}")
    """
    if tool_name not in TOOL_REGISTRY:
        available_tools = list(TOOL_REGISTRY.keys())
        raise ValueError(
            f"Unknown tool: {tool_name}\n"
            f"Available tools: {', '.join(available_tools)}"
        )

    method = TOOL_REGISTRY[tool_name]
    logger.debug(f"Retrieved method '{method.__name__}' for tool '{tool_name}'")

    return method


def get_available_tools() -> list[str]:
    """
    Get list of all available tool names in the registry.

    Returns:
        List of tool names in "provider__method" format

    Examples:
        >>> tools = get_available_tools()
        >>> print(tools)
        ['google_sheets__load_data_table', 'google_sheets__update_range', ...]

        >>> # Check if tool is supported
        >>> if "google_sheets__update_range" in get_available_tools():
        ...     print("Tool is supported")
    """
    return list(TOOL_REGISTRY.keys())


def get_provider_tools(provider: str) -> list[str]:
    """
    Get all tool names for a specific provider.

    Args:
        provider: Provider name (e.g., "google_sheets")

    Returns:
        List of tool names for that provider

    Examples:
        >>> tools = get_provider_tools("google_sheets")
        >>> print(tools)
        ['google_sheets__load_data_table', 'google_sheets__update_range', ...]
    """
    prefix = f"{provider}__"
    return [tool for tool in TOOL_REGISTRY.keys() if tool.startswith(prefix)]


def is_tool_supported(tool_name: str) -> bool:
    """
    Check if a tool name is supported in the registry.

    Args:
        tool_name: Full MCP tool name

    Returns:
        True if tool is supported, False otherwise

    Examples:
        >>> is_tool_supported("google_sheets__update_range")
        True

        >>> is_tool_supported("unknown_tool__some_method")
        False
    """
    return tool_name in TOOL_REGISTRY


# Validation helper
def validate_tool_name(tool_name: str) -> None:
    """
    Validate that a tool name exists in registry.

    Args:
        tool_name: Full MCP tool name

    Raises:
        ValueError: If tool name is invalid or not found

    Examples:
        >>> validate_tool_name("google_sheets__update_range")  # OK, no error
        >>> validate_tool_name("invalid_tool")  # Raises ValueError
    """
    # First check format
    parse_tool_name(tool_name)

    # Then check if it exists
    if not is_tool_supported(tool_name):
        available = get_available_tools()
        raise ValueError(
            f"Tool '{tool_name}' not found in registry.\n"
            f"Available tools:\n" +
            "\n".join(f"  - {tool}" for tool in available)
        )
