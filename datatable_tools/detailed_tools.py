"""
MCP Tools - Detailed Operations

Thin wrapper layer that delegates to GoogleSheetDataTable implementation.
These @mcp.tool functions serve as the API entry points for MCP clients.
"""

from typing import Dict, List, Optional, Any
import logging
from pydantic import Field
from fastmcp import Context
from core.server import mcp
from datatable_tools.lifecycle_tools import SpreadsheetResponse
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

logger = logging.getLogger(__name__)


@mcp.tool
async def write_new_sheet(
    ctx: Context,
    data: list[list[int | str | float | bool | None]] = Field(
        description="Data Accepts: List[List[int| str |float| bool | None]] (2D array)"
    ),
    headers: Optional[List[str]] = Field(
        default=None,
        description="Optional column headers. If None, headers will be auto-detected from first row if it contains short strings followed by longer content"
    ),
    sheet_name: Optional[str] = Field(
        default=None,
        description="Optional name for the new spreadsheet (default: 'New DataTable')"
    )
) -> SpreadsheetResponse:
    """
    Create a new Google Sheets spreadsheet with the provided data.

    Args:
        data: Data in pandas-like formats. Accepts:
              - List[List[int| str |float| bool | None]]: 2D array of table data (rows x columns)
        headers: Optional column headers. If None and first row contains short strings followed
                by rows with longer content (>50 chars), headers will be auto-detected and
                extracted from the first row.
        sheet_name: Optional name for the new spreadsheet (default: "New DataTable")

    Returns:
        SpreadsheetResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_url: Full URL to the created spreadsheet
            - rows_created: Number of rows written
            - columns_created: Number of columns written
            - data_shape: Tuple of (rows, columns)
            - error: Error message if failed, None otherwise
            - message: Human-readable result message

    Examples:
        # Create new spreadsheet with data
        write_new_sheet(ctx, data=[["John", 25], ["Jane", 30]],
                        headers=["name", "age"])

        # Create with custom name
        write_new_sheet(ctx, data=[["Product", "Price"], ["Widget", 9.99]],
                        sheet_name="Product Catalog")

        # Create with auto-detected headers
        write_new_sheet(ctx, data=[["name", "description"],
                                   ["Item1", "This is a long description"]])
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.write_new_sheet(ctx, data, headers, sheet_name)


@mcp.tool
async def append_rows(
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: list[list[int | str | float | bool | None]] = Field(
        description="Data Accepts: List[List[int| str |float| bool | None]] (2D array)"
    )
) -> Dict[str, Any]:
    """
    Append data as new rows below existing data in Google Sheets.
    Automatically detects the last row and appends below it starting from column A.

    Args:
        uri: Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})
        data: Data Accepts:
              - List[List[int| str |float| bool | None]]: 2D array of table data (rows x columns)

    Returns:
        Dict containing update results and file/spreadsheet information

    Examples:
        # Append new records to Google Sheets
        append_rows(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit?gid={gid}",
                   data=[["John", 25], ["Jane", 30]])

        # Append with auto-detected headers (first row = headers if long content follows)
        append_rows(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit?gid={gid}",
                   data=[["name", "description"],
                         ["Item1", "This is a long description that will trigger header detection"]])
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.append_rows(ctx, uri, data)


@mcp.tool
async def append_columns(
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: list[list[int | str | float | bool | None]] = Field(
        description="Data Accepts: List[List[int| str |float| bool | None]] (2D array)"
    ),
    headers: Optional[List[str]] = Field(
        default=None,
        description="Optional column headers. If None, headers will be auto-detected from first row if it contains short strings followed by longer content"
    )
) -> Dict[str, Any]:
    """
    Append data as new columns to the right of existing data in Google Sheets.
    Automatically detects the last column and appends to its right starting from row 1.

    Args:
        uri: Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})
        data: Data Accepts:
              - List[List[int| str |float| bool | None]]: 2D array of table data (rows x columns)
        headers: Optional column headers. If None and first row contains short strings followed
                by rows with longer content (>50 chars), headers will be auto-detected and
                extracted from the first row.

    Returns:
        Dict containing update results and file/spreadsheet information

    Examples:
        # Append new columns to Google Sheets
        append_columns(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit?gid={gid}",
                      data=[["Feature1"], ["Feature2"]], headers=["new_feature"])
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.append_columns(ctx, uri, data, headers)


@mcp.tool
async def update_range(
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: list[list[int | str | float | bool | None]] = Field(
        description="2D array of cell values (rows × columns). CRITICAL: Must be a nested list/array structure [[row1_col1, row1_col2], [row2_col1, row2_col2]], NOT a string. Each inner list represents one row. Accepts int, str, float, bool, or None values."
    ),
    range_address: str = Field(
        description="Range in A1 notation. Examples: single cell 'B5', row range 'A1:E1', column range 'B:B' or 'B1:B10', 2D range 'A1:C3'. Range auto-expands if data dimensions exceed specified range."
    )
) -> Dict[str, Any]:
    """
    Writes cell values to a Google Sheets range, replacing existing content. Auto-expands range if data exceeds specified bounds.

    <description>Overwrites cell values in a specified range with provided 2D array data. Replaces existing content completely - does not merge or append. Auto-expands range when data dimensions exceed specified range.</description>

    <use_case>Use for bulk data updates, replacing table contents, writing processed results, or updating structured data blocks with precise placement control.</use_case>

    <limitation>Cannot update non-contiguous ranges. Overwrites existing formulas and cell formatting.</limitation>

    <failure_cases>Fails if range_address is invalid A1 notation, expanded range exceeds sheet bounds, or data parameter is not a proper 2D array structure (common error: passing string instead of nested lists). Data truncation on cells >50,000 characters.</failure_cases>

    Args:
        uri: Google Sheets URI (supports full URL pattern)
        data: 2D array [[row1_col1, row1_col2], [row2_col1, row2_col2]].
              CRITICAL: Must be nested list structure, NOT a string.
              Values: int, str, float, bool, or None.
        range_address: A1 notation (e.g., "B5", "A1:E1", "B:B", "A1:C3"). Auto-expands to fit data.

    Returns:
        Dict containing update results and spreadsheet information

    Examples:
        # Update at specific position
        update_range(ctx, uri, data=[["Value1", "Value2"]], range_address="B5")

        # Write table from A1 with auto-expansion
        update_range(ctx, uri, data=[["Col1", "Col2"], [1, 2], [3, 4]], range_address="A1")
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_range(ctx, uri, data, range_address)
