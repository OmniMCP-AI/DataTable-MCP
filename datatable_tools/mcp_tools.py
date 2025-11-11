"""
MCP Tools - All Core Operations

Thin wrapper layer that delegates to GoogleSheetDataTable implementation.
These @mcp.tool functions serve as the API entry points for MCP clients.

Contains all 5 core MCP tools:
- load_data_table: Load data from Google Sheets
- write_new_sheet: Create new Google Sheets spreadsheet
- append_rows: Append rows to existing sheet
- append_columns: Append columns to existing sheet
- update_range: Update specific cell range
"""

from typing import Optional, List, Any, Dict
import logging
from pydantic import Field
from fastmcp import Context
from core.server import mcp
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable
from datatable_tools.auth.service_decorator import require_google_service
from datatable_tools.models import TableResponse, SpreadsheetResponse, UpdateResponse, TableData

# Optional Polars import for type hints
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

logger = logging.getLogger(__name__)


# MCP Tools
@mcp.tool
@require_google_service("sheets", "sheets_read")
async def load_data_table(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    )
) -> TableResponse:
    """
    Load a table from Google Sheets using URI-based auto-detection

    Args:
        uri: Google Sheets URI. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}

    Returns:
        Dict containing table_id and loaded Google Sheets table information

    Examples:
        # Google Sheets URL
        uri = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit?gid=0#gid=0"
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.load_data_table(service, uri)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def write_new_sheet(
    service,  # Injected by @require_google_service
    ctx: Context,
    data: TableData = Field(
        description=(
            "Data in pandas-like formats. Accepts:\n"
            "- List[List[int|str|float|bool|None]]: 2D array\n"
            "- List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like)\n"
            "- List[int|str|float|bool|None]: 1D array (single row/column)\n"
            "- polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)"
        )
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
              - List[List[int|str|float|bool|None]]: 2D array of table data (rows x columns)
              - List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like), each dict represents a row
              - polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)
              Headers are automatically extracted from list of dicts or DataFrames, or auto-detected from 2D arrays.
        sheet_name: Optional name for the new spreadsheet (default: "New DataTable")

    Returns:
        SpreadsheetResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_url: Full URL to the created spreadsheet
            - rows_created: Number of rows written
            - columns_created: Number of columns written
            - shape: String of "(rows,columns)"
            - error: Error message if failed, None otherwise
            - message: Human-readable result message

    Examples:
        # Create new spreadsheet with 2D array data (headers auto-detected)
        write_new_sheet(ctx, data=[["name", "age"], ["John", 25], ["Jane", 30]])

        # Create with list of dicts (DataFrame-like, headers extracted from dict keys)
        write_new_sheet(ctx, data=[{"name": "John", "age": 25}, {"name": "Jane", "age": 30}])

        # Create with Polars DataFrame (via MCPPlus bridge)
        import polars as pl
        df = pl.DataFrame({"name": ["John", "Jane"], "age": [25, 30]})
        result = await call_tool_by_sse(
            sse_url=SSE_URL,
            tool_name="google_sheets__write_new_sheet",
            direct_call=True,
            args={"data": df, "sheet_name": "My Data"}
        )

        # Create with custom name
        write_new_sheet(ctx, data=[["Product", "Price"], ["Widget", 9.99]],
                        sheet_name="Product Catalog")
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.write_new_sheet(service, data, sheet_name)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def append_rows(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: TableData = Field(
        description=(
            "Data to append as rows. Accepts:\n"
            "- List[List[int|str|float|bool|None]]: 2D array\n"
            "- List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like)\n"
            "- List[int|str|float|bool|None]: 1D array (single row)\n"
            "- polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)"
        )
    )
) -> UpdateResponse:
    """
    Append data as new rows below existing data in Google Sheets.
    Automatically detects the last row and appends below it starting from column A.

    Args:
        uri: Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})
        data: Data to append. Accepts:
              - List[List[int|str|float|bool|None]]: 2D array of table data (rows x columns)
              - List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like), each dict represents a row
              - polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)

    Returns:
        UpdateResponse containing success status, range, updated cells, shape, etc.

    Examples:
        # Append new records to Google Sheets (2D array)
        append_rows(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit?gid={gid}",
                   data=[["John", 25], ["Jane", 30]])

        # Append with list of dicts (DataFrame-like)
        append_rows(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit?gid={gid}",
                   data=[{"name": "John", "age": 25}, {"name": "Jane", "age": 30}])

        # Append with Polars DataFrame (via MCPPlus bridge)
        import polars as pl
        df = pl.DataFrame({"name": ["John", "Jane"], "age": [25, 30]})
        result = await call_tool_by_sse(
            sse_url=SSE_URL,
            tool_name="google_sheets__append_rows",
            direct_call=True,
            args={"uri": uri, "data": df}
        )
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.append_rows(service, uri, data)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def append_columns(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: TableData = Field(
        description=(
            "Data to append as columns. Accepts:\n"
            "- List[List[int|str|float|bool|None]]: 2D array\n"
            "- List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like)\n"
            "- List[int|str|float|bool|None]: 1D array (single column)\n"
            "- polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)"
        )
    )
) -> UpdateResponse:
    """
    Append data as new columns to the right of existing data in Google Sheets.
    Automatically detects the last column and appends to its right starting from row 1.

    Args:
        uri: Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})
        data: Data in pandas-like formats. Accepts:
              - List[List[int|str|float|bool|None]]: 2D array of table data (rows x columns)
              - List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like), each dict represents a row
              Headers are automatically extracted from list of dicts or DataFrames, or auto-detected from 2D arrays.

    Returns:
        UpdateResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_url: Full URL to the spreadsheet with gid
            - spreadsheet_id: The spreadsheet ID
            - worksheet: The worksheet name
            - range: The range where data was appended
            - updated_cells: Number of cells updated
            - shape: String of "(rows,columns)"
            - error: Error message if failed, None otherwise
            - message: Human-readable result message

    Examples:
        # Append new columns to Google Sheets (2D array with headers auto-detected)
        append_columns(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit?gid={gid}",
                      data=[["new_feature"], ["Feature1"], ["Feature2"]])

        # Append with list of dicts (DataFrame-like, headers extracted from dict keys)
        append_columns(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit?gid={gid}",
                      data=[{"new_col": "Value1"}, {"new_col": "Value2"}])
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.append_columns(service, uri, data)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def update_range(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: TableData = Field(
        description=(
            "Data to write to range. Accepts:\n"
            "- List[List[int|str|float|bool|None]]: 2D array of cell values (rows × columns)\n"
            "- List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like)\n"
            "- List[int|str|float|bool|None]: 1D array (single row/column)\n"
            "- polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)\n"
            "CRITICAL: Must be proper data structure, NOT a string. Each inner list represents one row."
        )
    ),
    range_address: str = Field(
        description="Range in A1 notation. Examples: single cell 'B5', row range 'A1:E1', column range 'B:B' or 'B1:B10', 2D range 'A1:C3'. Range auto-expands if data dimensions exceed specified range."
    )
) -> UpdateResponse:
    """
    Writes cell values to a Google Sheets range, replacing existing content. Auto-expands range if data exceeds specified bounds.

    Automatically detects if the original URI data has headers and handles updates accordingly:
    - If original data has headers and new data has headers: skips header and updates only data rows
    - If original data has no headers: updates all data including first row

    <description>Overwrites cell values in a specified range with provided 2D array data or list of dicts. Replaces existing content completely - does not merge or append. Auto-expands range when data dimensions exceed specified range.</description>

    <use_case>Use for bulk data updates, replacing table contents, writing processed results, or updating structured data blocks with precise placement control.</use_case>

    <limitation>Cannot update non-contiguous ranges. Overwrites existing formulas and cell formatting.</limitation>

    <failure_cases>Fails if range_address is invalid A1 notation, expanded range exceeds sheet bounds, or data parameter is not a proper 2D array structure or list of dicts (common error: passing string instead of nested lists). Data truncation on cells >50,000 characters.</failure_cases>

    Args:
        uri: Google Sheets URI (supports full URL pattern)
        data: 2D array [[row1_col1, row1_col2], [row2_col1, row2_col2]] or list of dicts (DataFrame-like).
              CRITICAL: Must be nested list structure or list of dicts, NOT a string.
              Values: int, str, float, bool, or None.
        range_address: A1 notation (e.g., "B5", "A1:E1", "B:B", "A1:C3"). Auto-expands to fit data.

    Returns:
        UpdateResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_url: Full URL to the spreadsheet with gid
            - spreadsheet_id: The spreadsheet ID
            - worksheet: The worksheet name
            - range: The range that was updated
            - updated_cells: Number of cells updated
            - shape: String of "(rows,columns)"
            - error: Error message if failed, None otherwise
            - message: Human-readable result message

    Examples:
        # Update at specific position (2D array)
        update_range(ctx, uri, data=[["Value1", "Value2"]], range_address="B5")

        # Update with list of dicts (DataFrame-like)
        update_range(ctx, uri, data=[{"col1": "Value1", "col2": "Value2"}], range_address="A1")

        # Write table from A1 with auto-expansion
        update_range(ctx, uri, data=[["Col1", "Col2"], [1, 2], [3, 4]], range_address="A1")
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_range(service, uri, data, range_address)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def update_range_by_lookup(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: List[Dict[str, Any]] = Field(
        description=(
            "Update data as list of dicts (DataFrame-like). Each dict represents a row with column names as keys.\n"
            "Must include the lookup column specified in 'on' parameter.\n"
            "Example: [{'username': '@user1', 'status': 'active'}, {'username': '@user2', 'status': 'inactive'}]"
        )
    ),
    on: str = Field(
        description="Column name to use as lookup key for matching rows (e.g., 'username', 'id', 'email'). Must exist in both the sheet and update data. Matching is case-insensitive."
    ),
    override: bool = Field(
        default=False,
        description="If True, empty/null values in update data will clear existing cells. If False (default), empty/null values preserve existing cell values."
    )
) -> UpdateResponse:
    """
    Update Google Sheets data by looking up rows using a key column, similar to SQL UPDATE with JOIN.

    <description>Performs selective column updates by matching rows via a lookup key. Only updates columns present in the new data while preserving other columns and unmatched rows. Similar to a database UPDATE with JOIN operation.</description>

    <use_case>Use for updating specific columns in a sheet based on a key column (like username or ID), enriching existing data with new fields, or syncing partial data from external sources without overwriting the entire sheet.</use_case>

    <limitation>Duplicate lookup keys: only first match is updated. Unmatched rows in update data are silently ignored. Case-insensitive matching only.</limitation>

    <failure_cases>Fails if lookup column doesn't exist in sheet or update data, if data is not list of dicts, or if sheet cannot be loaded. Returns warning if no matching rows found.</failure_cases>

    Args:
        uri: Google Sheets URI (supports full URL pattern)
        data: List of dicts with update data. Must include the lookup column.
              Example: [{"username": "@user1", "latest_tweet": "2025-09-18"}, ...]
        on: Lookup column name (must exist in both sheet and data). Case-insensitive matching.
        override: If True, empty values clear cells. If False, empty values preserve existing values. Default False.

    Returns:
        UpdateResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_url: Full URL to the spreadsheet with gid
            - spreadsheet_id: The spreadsheet ID
            - worksheet: The worksheet name
            - range: The range that was updated
            - updated_cells: Number of cells updated
            - shape: String of "(rows,columns)"
            - error: Error message if failed, None otherwise
            - message: Human-readable result with match statistics

    Behavior:
        - Lookup matching: Case-insensitive
        - Duplicate keys: Updates first match only
        - Unmatched rows: Ignored (skipped silently)
        - New columns: Automatically added at the end
        - Empty values: Clears cell if override=True, preserves if override=False

    Examples:
        # Basic update by username
        update_range_by_lookup(
            ctx, uri,
            data=[
                {"username": "@user1", "status": "active"},
                {"username": "@user2", "status": "inactive"}
            ],
            on="username"
        )

        # Update with new columns (automatically added) and override empty values
        update_range_by_lookup(
            ctx, uri,
            data=[
                {"username": "@user1", "latest_tweet": "2025-09-18", "formatted_date": "2025-09-18"},
                {"username": "@user2", "latest_tweet": "", "formatted_date": ""}
            ],
            on="username",
            override=True  # Empty values will clear existing cells
        )
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_by_lookup(service, uri, data, on, override)
