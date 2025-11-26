"""
MCP Tools - All Core Operations

Thin wrapper layer that delegates to GoogleSheetDataTable implementation.
These @mcp.tool functions serve as the API entry points for MCP clients.

Contains all core MCP tools:
- read_sheet: Read data from Google Sheets (preferred name)
- load_data_table: Load data from Google Sheets (legacy, use read_sheet instead)
- write_new_sheet: Create new Google Sheets spreadsheet
- write_new_worksheet: Create new worksheet in existing spreadsheet
- append_rows: Append rows to existing sheet
- append_columns: Append columns to existing sheet
- update_range: Update specific cell range
- update_range_by_lookup: Update rows by lookup key
"""

from typing import Optional, List, Any, Dict, Union
import logging
from pydantic import Field
from fastmcp import Context
from core.server import mcp
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable
from datatable_tools.auth.service_decorator import require_google_service
from datatable_tools.models import (
    TableResponse, SpreadsheetResponse, UpdateResponse, TableData, WorksheetsListResponse,
    GetLastRowResponse, GetUsedRangeResponse, GetLastColumnResponse
)
from datatable_tools.google_sheets_helpers import (
    process_data_input, parse_google_sheets_uri, get_sheet_by_gid,
    get_last_row_with_data, get_used_range_info, get_last_column_with_data
)

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
async def read_sheet(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    range_address: Optional[str] = Field(
        default=None,
        description="Optional range in A1 notation (e.g., 'A2:M1000' for specific range, '2:1000' for rows 2-1000, 'B:Z' for columns B-Z). If not provided, reads entire sheet."
    )
) -> TableResponse:
    """
    Read data from Google Sheets using URI-based auto-detection

    Args:
        uri: Google Sheets URI. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}
        range_address: Optional range in A1 notation:
             - "A2:M1000" - Read specific rectangular range
             - "2:1000" - Read rows 2 to 1000 (all columns)
             - "B:Z" - Read columns B to Z (all rows)
             - None (default) - Read entire sheet with smart header detection

    Returns:
        Dict containing table_id and loaded Google Sheets table information

    Examples:
        # Basic usage with smart header detection (works for partially merged cells)
        uri = "https://docs.google.com/spreadsheets/d/ABC123/edit?gid=0"
        result = read_sheet(ctx, uri)

        # RECOMMENDED for heavily merged title rows: Skip row 1, start from row 2
        # Use this when row 1 is merged across all columns (e.g., "每日库存报表")
        result = read_sheet(ctx, uri, range_address="2:10000")

        # Read specific range only (first row treated as header)
        result = read_sheet(ctx, uri, range_address="A2:M1000")
    """
    google_sheet = GoogleSheetDataTable()
    # When range_address is specified, user knows the exact range, so disable auto-detection
    # When no range_address, use smart detection to find the real header row
    auto_detect_header_row = range_address is None
    return await google_sheet.load_data_table(service, uri, range_address, auto_detect_header_row)


@mcp.tool
@require_google_service("sheets", "sheets_read")
async def load_data_table(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    range_address: Optional[str] = Field(
        default=None,
        description="Optional range in A1 notation (e.g., 'A2:M1000' for specific range, '2:1000' for rows 2-1000, 'B:Z' for columns B-Z). If not provided, reads entire sheet."
    )
) -> TableResponse:
    """
    Load a table from Google Sheets using URI-based auto-detection

    DEPRECATED: Use read_sheet() instead. This function is maintained for backward compatibility.

    Args:
        uri: Google Sheets URI. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}
        range_address: Optional range in A1 notation:
             - "A2:M1000" - Read specific rectangular range
             - "2:1000" - Read rows 2 to 1000 (all columns)
             - "B:Z" - Read columns B to Z (all rows)
             - None (default) - Read entire sheet with smart header detection

    Returns:
        Dict containing table_id and loaded Google Sheets table information

    Examples:
        # Basic usage with smart header detection (works for partially merged cells)
        uri = "https://docs.google.com/spreadsheets/d/ABC123/edit?gid=0"
        result = load_data_table(ctx, uri)

        # RECOMMENDED for heavily merged title rows: Skip row 1, start from row 2
        # Use this when row 1 is merged across all columns (e.g., "每日库存报表")
        result = load_data_table(ctx, uri, range_address="2:10000")

        # Read specific range only (first row treated as header)
        result = load_data_table(ctx, uri, range_address="A2:M1000")
    """
    # Delegate to GoogleSheetDataTable implementation for backward compatibility
    # This ensures both read_sheet and load_data_table use the same underlying logic
    google_sheet = GoogleSheetDataTable()
    auto_detect_header_row = range_address is None
    return await google_sheet.load_data_table(service, uri, range_address, auto_detect_header_row)


@mcp.tool
@require_google_service("sheets", "sheets_read")
async def read_worksheet_with_formulas(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    )
) -> TableResponse:
    """
    Read a worksheet from Google Sheets with cell formulas instead of calculated values.

    Similar to load_data_table, but returns the raw formulas from cells (e.g., "=SUM(A1:A10)")
    instead of the computed values (e.g., "100"). For cells without formulas, returns the plain value.

    <description>Reads worksheet data from Google Sheets with raw formulas preserved. Returns formula strings (e.g., "=A1+A2") instead of calculated results. Useful for inspecting spreadsheet logic, copying formulas, or understanding cell dependencies.</description>

    <use_case>Use when you need to:
    - Inspect or analyze spreadsheet formulas and logic
    - Copy formulas to another sheet while preserving references
    - Understand cell dependencies and calculations
    - Debug formula errors or circular references
    - Document spreadsheet computation logic
    </use_case>

    <limitation>Returns formulas as text strings. Does not evaluate or validate formula syntax. For cells without formulas, returns the plain value (same as load_data_table).</limitation>

    <failure_cases>Fails if URI is invalid, spreadsheet doesn't exist, or user lacks read permissions. No special handling for complex formula types (array formulas, lambda functions, etc.).</failure_cases>

    Args:
        uri: Google Sheets URI. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}

    Returns:
        TableResponse containing:
            - success: Whether the operation succeeded
            - table_id: Unique identifier for this table (with "_formulas" suffix)
            - name: Sheet name with "(Formulas)" indicator
            - shape: String of "(rows,columns)"
            - data: List of dicts with column names as keys and formula strings as values
            - source_info: Metadata including "value_render_option": "FORMULA"
            - error: Error message if failed, None otherwise
            - message: Human-readable result message

    Examples:
        # Read formulas from Google Sheets
        result = read_worksheet_with_formulas(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit?gid=0"
        )

        # Access formula data
        if result.success:
            for row in result.data:
                print(row)  # {"Total": "=SUM(A2:A10)", "Average": "=AVERAGE(B2:B10)"}

        # Compare with regular values
        values = load_data_table(ctx, uri)                    # Returns {"Total": "100", "Average": "25.5"}
        formulas = read_worksheet_with_formulas(ctx, uri)     # Returns {"Total": "=SUM(A2:A10)", "Average": "=AVERAGE(B2:B10)"}
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.read_worksheet_with_formulas(service, uri)


@mcp.tool
@require_google_service("sheets", "sheets_read")
async def preview_worksheet_with_formulas(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    limit: int = Field(
        default=5,
        description="Number of data rows to preview (default: 5, max: 100). Does not include header row."
    )
) -> TableResponse:
    """
    Preview the first N rows of a worksheet with formulas (quick preview).

    Returns only the first few rows with formula strings instead of calculated values.
    Useful for quickly inspecting the beginning of a large sheet without loading all data.

    <description>Quick preview of worksheet data with formulas. Returns only the first N rows (default 5) with raw formula strings. Ideal for inspecting large sheets without loading full data.</description>

    <use_case>Use when you need to:
    - Quickly check what formulas are at the top of a large sheet
    - Inspect formula patterns without loading thousands of rows
    - Get a sample of the data structure and formulas
    - Reduce loading time for large worksheets
    </use_case>

    <limitation>Only returns the first N rows (max 100). For full data with formulas, use read_worksheet_with_formulas instead.</limitation>

    <failure_cases>Fails if URI is invalid, spreadsheet doesn't exist, or user lacks read permissions.</failure_cases>

    Args:
        uri: Google Sheets URI. Supports full URL with gid parameter
        limit: Number of data rows to preview (default: 5, max: 100)

    Returns:
        TableResponse containing:
            - success: Whether the operation succeeded
            - table_id: Unique identifier (with "_preview_formulas" suffix)
            - name: Sheet name with "Preview (Formulas)" indicator
            - shape: String of "(rows,columns)"
            - data: List of dicts with column names as keys and formula strings as values (limited rows)
            - source_info: Metadata including "preview_limit", "is_preview": true, "value_render_option": "FORMULA"
            - error: Error message if failed, None otherwise
            - message: Human-readable result with preview info

    Examples:
        # Preview first 5 rows with formulas (default)
        result = preview_worksheet_with_formulas(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit?gid=0"
        )

        # Preview first 10 rows with formulas
        result = preview_worksheet_with_formulas(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit?gid=0",
            limit=10
        )

        # Access preview data
        if result.success:
            print(f"Preview: {result.source_info['preview_limit']} rows")
            for row in result.data:
                print(row)  # {"Total": "=SUM(A2:A10)", "Average": "=AVERAGE(B2:B10)"}
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.preview_worksheet_with_formulas(service, uri, limit)


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
    case: multiple columns with empty data rows.
    [
        ["formatted_date", "days_diff", "status",
    "unfollow_success"]  # All column headers in ONE row
    ]

    Or if you have data for these columns:
    [
        {"formatted_date": "2025-01-01", "days_diff": 5,
    "status": "active", "unfollow_success": True},
        {"formatted_date": "2025-01-02", "days_diff": 6,
    "status": "inactive", "unfollow_success": False}
    ]

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

    Values are automatically parsed as if typed by user (USER_ENTERED mode):
    - Formulas (=IMAGE, =SUM, etc.) are interpreted as formulas
    - Numbers are stored as numbers (not text)
    - Dates are recognized and formatted appropriately

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

        # Insert image formula (formulas are automatically interpreted)
        update_range(ctx, uri, data=[['=IMAGE("https://example.com/image.jpg", 1)']],
                    range_address="A1")
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_range(service, uri, data, range_address)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def insert_image_in_cell(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    image_url: str = Field(
        description="Public URL of the image to insert. Must be accessible without authentication."
    ),
    cell_address: str = Field(
        description="Cell address in A1 notation where image should be inserted (e.g., 'A1', 'B5', 'C10')."
    ),
    width_pixels: int = Field(
        default=400,
        description="Image and cell width in pixels. Default: 400. The cell column will be auto-resized to this width."
    ),
    height_pixels: int = Field(
        default=300,
        description="Image and cell height in pixels. Default: 300. The cell row will be auto-resized to this height."
    )
) -> UpdateResponse:
    """
    Insert an image into a cell with automatic cell resizing.

    This tool combines two operations:
    1. Inserts IMAGE formula with mode 4 (custom dimensions) into the specified cell
    2. Automatically resizes the row height and column width to match the image dimensions

    The result is a perfectly fitted image that displays at the exact size specified,
    without requiring manual cell resizing.

    <description>Inserts an image into a Google Sheets cell using the IMAGE formula with custom dimensions,
    then automatically resizes the cell to perfectly fit the image. This is a one-step solution for
    adding properly sized images to spreadsheets.</description>

    <use_case>Use when you need to:
    - Add logos, charts, or photos to spreadsheets with exact dimensions
    - Create visual reports or dashboards with properly sized images
    - Insert product images, profile photos, or illustrations
    - Ensure images display at intended size without manual resizing
    - Programmatically add images with consistent sizing
    </use_case>

    <limitation>
    - Image URL must be publicly accessible (no authentication required)
    - Image is placed inside the cell (not floating over cells)
    - Resizing affects the entire row height and column width (all cells in that row/column)
    - Maximum dimensions depend on Google Sheets limits (typically ~1000px recommended)
    - Uses IMAGE formula mode 4 for custom sizing
    </limitation>

    <failure_cases>Fails if: image URL is not publicly accessible, URI is invalid, cell_address format is incorrect,
    dimensions exceed reasonable limits, or image format is not supported by Google Sheets.</failure_cases>

    Args:
        uri: Google Sheets URI
        image_url: Public URL of image (must be accessible without auth)
        cell_address: Cell address in A1 notation (e.g., "A1", "B5")
        width_pixels: Image and cell width in pixels (default: 400)
        height_pixels: Image and cell height in pixels (default: 300)

    Returns:
        UpdateResponse containing success status and details

    Examples:
        # Insert 400x300 image in cell A1 (default size)
        insert_image_in_cell(ctx, uri,
                            image_url="https://example.com/logo.png",
                            cell_address="A1")

        # Insert large 800x600 image in cell B5
        insert_image_in_cell(ctx, uri,
                            image_url="https://example.com/photo.jpg",
                            cell_address="B5",
                            width_pixels=800,
                            height_pixels=600)

        # Insert small 200x150 thumbnail in cell C10
        insert_image_in_cell(ctx, uri,
                            image_url="https://example.com/thumb.jpg",
                            cell_address="C10",
                            width_pixels=200,
                            height_pixels=150)
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.insert_image_in_cell(
        service, uri, image_url, cell_address, width_pixels, height_pixels
    )


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def write_new_worksheet(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: TableData = Field(
        description=(
            "Data to write to the new worksheet. Accepts:\n"
            "- List[List[int|str|float|bool|None]]: 2D array\n"
            "- List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like)\n"
            "- List[int|str|float|bool|None]: 1D array (single row/column)\n"
            "- polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)"
        )
    ),
    worksheet_name: str = Field(
        description="Name for the new worksheet to create in the spreadsheet"
    )
) -> UpdateResponse:
    """
    Create a new worksheet in an existing Google Sheets spreadsheet and write data to it.

    If a worksheet with the same name already exists, it will write to the existing worksheet
    instead of creating a duplicate.

    <description>Creates a new worksheet (tab) within an existing spreadsheet and populates it with data. If the worksheet already exists, writes data to it.</description>

    <use_case>Use for organizing related data into separate tabs within the same spreadsheet, creating categorized views, or splitting large datasets into logical sections.</use_case>

    <limitation>Cannot create worksheets with duplicate names in the same spreadsheet. If worksheet exists, data will overwrite existing content.</limitation>

    <failure_cases>Fails if URI is invalid, spreadsheet doesn't exist, or worksheet name contains invalid characters. Data truncation on cells >50,000 characters.</failure_cases>

    Args:
        uri: Google Sheets URI pointing to the target spreadsheet
        data: Data in pandas-like formats. Accepts:
              - List[List[int|str|float|bool|None]]: 2D array of table data (rows x columns)
              - List[Dict[str, int|str|float|bool|None]]: List of dicts (DataFrame-like), each dict represents a row
              - polars.DataFrame: Polars DataFrame (when called via MCPPlus bridge with direct_call=True)
              Headers are automatically extracted from list of dicts or DataFrames, or auto-detected from 2D arrays.
        worksheet_name: Name for the new worksheet to create

    Returns:
        UpdateResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_url: Full URL to the worksheet with gid
            - spreadsheet_id: The spreadsheet ID
            - worksheet: The worksheet name
            - range: The range where data was written
            - updated_cells: Number of cells written
            - shape: String of "(rows,columns)"
            - error: Error message if failed, None otherwise
            - message: Human-readable result message

    Examples:
        # Create worksheet with 2D array data (headers auto-detected)
        write_new_worksheet(ctx, uri, data=[["name", "age"], ["John", 25], ["Jane", 30]],
                           worksheet_name="Employees")

        # Create with list of dicts (DataFrame-like, headers extracted from dict keys)
        write_new_worksheet(ctx, uri, data=[{"name": "John", "age": 25}, {"name": "Jane", "age": 30}],
                           worksheet_name="Users")

        # Create with Polars DataFrame (via MCPPlus bridge)
        import polars as pl
        df = pl.DataFrame({"name": ["John", "Jane"], "age": [25, 30]})
        result = await call_tool_by_sse(
            sse_url=SSE_URL,
            tool_name="google_sheets__write_new_worksheet",
            direct_call=True,
            args={"uri": uri, "data": df, "worksheet_name": "Analytics"}
        )
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.write_new_worksheet(service, uri, data, worksheet_name)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def update_range_by_lookup(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: Union[List[Dict[str, Any]], str, Any] = Field(
        description=(
            "Update data as list of dicts (DataFrame-like). Each dict represents a row with column names as keys.\n"
            "Also supports Polars DataFrame or its string representation (automatically converted).\n"
            "Must include the lookup column(s) specified in 'on' parameter.\n"
            "Example: [{'username': '@user1', 'status': 'active'}, {'username': '@user2', 'status': 'inactive'}]"
        )
    ),
    on: Union[str, List[str]] = Field(
        description=(
            "Column name(s) to use as lookup key for matching rows. Can be:\n"
            "  - Single column: 'username' or ['username']\n"
            "  - Multiple columns (composite key): ['first_name', 'last_name']\n"
            "All specified columns must exist in both the sheet and update data.\n"
            "Matching is case-insensitive. When multiple keys are provided, ALL must match (AND logic)."
        )
    ),
    override: bool = Field(
        default=False,
        description="If True, empty/null values in update data will clear existing cells. If False (default), empty/null values preserve existing cell values."
    )
) -> UpdateResponse:
    """
    Update Google Sheets data by looking up rows using one or more key columns, similar to SQL UPDATE with JOIN.

    <description>Performs selective column updates by matching rows via lookup key(s). Supports both single and composite keys (multiple columns). Only updates columns present in the new data while preserving other columns and unmatched rows. Similar to a database UPDATE with JOIN operation.</description>

    <use_case>Use for updating specific columns in a sheet based on key column(s) (like username, ID, or composite keys like first_name+last_name), enriching existing data with new fields, or syncing partial data from external sources without overwriting the entire sheet.</use_case>

    <limitation>Unmatched rows in update data are silently ignored. Case-insensitive matching only. When multiple lookup keys are provided, all rows matching the composite key are updated (not just the first).</limitation>

    <failure_cases>Fails if any lookup column doesn't exist in sheet or update data, if data is not list of dicts, or if sheet cannot be loaded. Returns warning if no matching rows found.</failure_cases>

    Args:
        uri: Google Sheets URI (supports full URL pattern)
        data: List of dicts with update data. Must include all lookup column(s).
              Also supports Polars DataFrame or its string representation (automatically converted).
              Example: [{"username": "@user1", "latest_tweet": "2025-09-18"}, ...]
        on: Lookup column name(s). Can be single string or list of strings for composite keys.
            Examples: "username" or ["first_name", "last_name"]
            All specified columns must exist in both sheet and data. Case-insensitive matching.
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
        - Multiple lookup keys: ALL keys must match (AND logic)
        - Duplicate keys: Updates all matching rows
        - Unmatched rows: Ignored (skipped silently)
        - New columns: Automatically added at the end
        - Empty values: Clears cell if override=True, preserves if override=False

    Examples:
        # Basic update by single key
        update_range_by_lookup(
            ctx, uri,
            data=[
                {"username": "@user1", "status": "active"},
                {"username": "@user2", "status": "inactive"}
            ],
            on="username"
        )

        # Update by composite key (multiple columns)
        update_range_by_lookup(
            ctx, uri,
            data=[
                {"first_name": "John", "last_name": "Doe", "status": "active"},
                {"first_name": "Jane", "last_name": "Smith", "status": "inactive"}
            ],
            on=["first_name", "last_name"]
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
    # Preprocess data to handle DataFrame inputs (Polars DataFrame, string repr, etc.)
    # This allows the tool to accept Polars DataFrames or their string representations
    # in addition to the standard List[Dict[str, Any]] format
    processed_data = data
    if isinstance(data, str) or (POLARS_AVAILABLE and isinstance(data, pl.DataFrame)):
        logger.info(f"Preprocessing DataFrame input for update_range_by_lookup")
        # Use process_data_input to convert to (headers, rows) format
        headers, data_rows = process_data_input(data)

        # Convert back to list of dicts format expected by update_by_lookup
        if headers and data_rows:
            processed_data = []
            for row in data_rows:
                row_dict = {}
                for i, header in enumerate(headers):
                    row_dict[header] = row[i] if i < len(row) else ""
                processed_data.append(row_dict)
            logger.info(f"Converted DataFrame to {len(processed_data)} rows with columns: {headers}")
        else:
            raise ValueError("Failed to process DataFrame input: no headers or data rows found")

    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_by_lookup(service, uri, processed_data, on, override)


@mcp.tool
@require_google_service("sheets", "sheets_read")
async def list_worksheets(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports spreadsheet ID or full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/...)"
    )
) -> WorksheetsListResponse:
    """
    List all worksheets (sheets/tabs) in a Google Sheets spreadsheet.

    <description>Retrieves metadata for all worksheets in a spreadsheet, including sheet names, IDs, dimensions, and ordering. Useful for discovering available sheets before performing operations.</description>

    <use_case>Use when you need to discover what sheets exist in a spreadsheet, get sheet IDs for specific operations, or check sheet dimensions before reading/writing data.</use_case>

    <limitation>Read-only operation. Does not modify the spreadsheet. Requires read access to the spreadsheet.</limitation>

    <failure_cases>Fails if spreadsheet doesn't exist, URI is invalid, or user lacks read permissions. Returns empty list for spreadsheets with no sheets (rare).</failure_cases>

    Args:
        uri: Google Sheets URI. Supports:
             - Full URL: https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit
             - Spreadsheet ID: {spreadsheetID}

    Returns:
        WorksheetsListResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_id: The spreadsheet ID
            - spreadsheet_url: Full URL to the spreadsheet
            - spreadsheet_title: The title of the spreadsheet
            - worksheets: List of WorksheetInfo objects with:
                - sheet_id: Unique sheet ID (gid)
                - title: Worksheet name/title
                - index: Zero-based position in the tab list
                - row_count: Number of rows in the sheet
                - column_count: Number of columns in the sheet
                - worksheet_url: Direct URL to the worksheet (includes gid)
            - total_worksheets: Total number of worksheets
            - error: Error message if failed, None otherwise
            - message: Human-readable result message

    Examples:
        # List all worksheets in a spreadsheet
        result = list_worksheets(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit"
        )

        # Use spreadsheet ID directly
        result = list_worksheets(
            ctx,
            uri="16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60"
        )

        # Access worksheet information
        if result.success:
            print(f"Spreadsheet: {result.spreadsheet_title}")
            print(f"Total sheets: {result.total_worksheets}")
            for ws in result.worksheets:
                print(f"  - {ws.title} (gid={ws.sheet_id}, {ws.row_count}x{ws.column_count})")
                print(f"    URL: {ws.worksheet_url}")
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.list_worksheets(service, uri)


@mcp.tool
@require_google_service("sheets", "sheets_write")
async def copy_range_with_formulas(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    from_range: str = Field(
        description="Source range in A1 notation (e.g., 'B2:K2' for row 2, columns B-K)"
    ),
    to_range: Optional[str] = Field(
        default=None,
        description="Destination range in A1 notation (e.g., 'B3:K3' for row 3). Required when auto_fill=False. Ignored when auto_fill=True."
    ),
    auto_fill: bool = Field(
        default=False,
        description="If True, automatically fills formulas down to all rows with data in lookup_column until first empty cell. Ignores to_range parameter."
    ),
    lookup_column: str = Field(
        default="A",
        description="Column to check for data when auto_fill=True. Continues copying until this column is empty. Default: 'A' (typically SKU/ID column)"
    ),
    skip_if_exists: bool = Field(
        default=True,
        description="When auto_fill=True, skips rows where the first destination cell already has a value. Default: True"
    )
) -> UpdateResponse:
    """
    Copy a range with formulas, automatically adapting cell references based on position change.

    This tool supports three modes:
    1. Manual mode - single range: Copy from_range to specific to_range (e.g., B2:K2 → B3:K3)
    2. Manual mode - multi-row: Copy single source row to multiple destination rows (e.g., B2:K2 → B3:K10)
    3. Auto-fill mode: Automatically copy formulas down to all data rows

    Formulas are automatically parsed and interpreted (USER_ENTERED mode), respecting
    absolute ($) and relative cell references.

    <description>Copies a range with formulas from one location to another (or multiple rows when auto-filling),
    automatically adapting cell references based on position changes. Respects absolute ($) references.
    Ideal for replicating formula patterns across rows or auto-filling formulas down to all data rows.</description>

    <use_case>Use when you need to:
    - Auto-fill formulas from a template row down to all data rows (typical: header set, first row has formulas)
    - Copy a row with formulas to another specific row (formulas adapt to new row)
    - Copy a single source row to multiple destination rows (e.g., B2:Z2 → B3:Z10 fills 8 rows)
    - Copy a column with formulas to another column (formulas adapt to new column)
    - Duplicate formula templates while preserving relative/absolute reference behavior
    - Replicate complex formula patterns without manual editing
    </use_case>

    <limitation>
    - Manual mode (single range): Source and destination ranges must have identical dimensions
    - Manual mode (multi-row): Source must be single row, columns must match
    - Only adapts standard A1 notation references
    - Named ranges and structured references are copied as-is (not adapted)
    - Auto-expands grid if destination exceeds current sheet bounds
    - Auto-fill mode: Stops at first empty cell in lookup_column
    </limitation>

    <failure_cases>Fails if: ranges have different dimensions (manual mode), source has >1 row for multi-row copy,
    range addresses are invalid A1 notation, source range is empty, to_range not provided when auto_fill=False,
    or URI is invalid.</failure_cases>

    Args:
        uri: Google Sheets URI
        from_range: Source range (e.g., "B2:K2" for row 2, columns B-K)
        to_range: Destination range:
                 - Single row: "B3:K3" (copies to one row)
                 - Multi-row: "B3:K10" (copies to rows 3-10, requires single source row)
                 - Required when auto_fill=False, ignored when auto_fill=True
        auto_fill: If True, automatically fills down to all rows with data in lookup_column
        lookup_column: Column to check for data when auto_fill=True (default: "A")
        skip_if_exists: If True, skips rows where first destination cell has value (default: True)

    Returns:
        UpdateResponse containing:
            - success: Whether the operation succeeded
            - spreadsheet_url: Full URL to the spreadsheet with gid
            - spreadsheet_id: The spreadsheet ID
            - worksheet: The worksheet name
            - range: The range that was updated (or ranges in auto-fill mode)
            - updated_cells: Number of cells updated
            - shape: String of "(rows,columns)"
            - error: Error message if failed, None otherwise
            - message: Human-readable result with offset information and rows filled

    Examples:
        # Example 1: Auto-fill mode - Copy formulas from row 2 down to all data rows
        # Typical use case: Header in row 1, formulas in row 2, data in rows 3+
        # Automatically detects header row and fills down until column A is empty
        copy_range_with_formulas(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/ABC123/edit?gid=0",
            from_range="B2:K2",
            auto_fill=True,
            lookup_column="A",  # Check column A (SKU) for data
            skip_if_exists=True  # Skip rows that already have formulas
        )
        # Result: Copies B2:K2 → B3:K3, B4:K4, B5:K5... until A column is empty

        # Example 2: Manual mode - Copy specific row to another row
        copy_range_with_formulas(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/ABC123/edit?gid=0",
            from_range="B2:K2",
            to_range="B3:K3"
        )
        # Result: Copies B2:K2 → B3:K3, formulas adapt to row 3

        # Example 3: Manual mode - Multi-row copy (NEW in stage 2)
        copy_range_with_formulas(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/ABC123/edit?gid=0",
            from_range="B2:Z2",
            to_range="B3:Z10"
        )
        # Result: Copies B2:Z2 → B3:Z3, B4:Z4, B5:Z5... B10:Z10 (8 rows total)

        # Example 4: Copy column with formulas
        copy_range_with_formulas(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/ABC123/edit?gid=0",
            from_range="L1:L100",
            to_range="I1:I100"
        )

        # How formula adaptation works:
        # Source B2: =SUMIFS('Sheet1'!$J:$J,'Sheet1'!$F:$F,$A2,'Sheet1'!$A:$A,B$1)
        # When copied from B2 to B3 (row offset +1, col offset 0):
        #   - $J:$J stays (absolute column range)
        #   - $F:$F stays (absolute column range)
        #   - $A2 -> $A3 (absolute column, relative row increments)
        #   - $A:$A stays (absolute column range)
        #   - B$1 stays (relative column same, absolute row)
        # Result B3: =SUMIFS('Sheet1'!$J:$J,'Sheet1'!$F:$F,$A3,'Sheet1'!$A:$A,B$1)
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.copy_range_with_formulas(
        service, uri, from_range, to_range, auto_fill, lookup_column, skip_if_exists
    )


@mcp.tool
@require_google_service("sheets", "sheets_read")
async def get_last_row(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}#gid={gid})"
    ),
    column: Optional[str] = Field(
        default=None,
        description="Optional column letter (e.g., 'B', 'AA') to find last row in specific column only. If not provided, searches across all columns."
    )
) -> GetLastRowResponse:
    """
    Identifies and returns the bottom-most row containing any data.

    Scans entire sheet to find last row with non-empty cells. Useful before appending
    new records, calculating table size, or determining where data ends.

    Optionally can search in a specific column only by providing the column parameter.

    <description>Identifies and returns the bottom-most row containing any data. Scans entire sheet to find last row with non-empty cells. Optionally searches in specific column only.</description>

    <use_case>Use before appending new records, calculating table size, or determining where data ends for processing boundaries. Use column parameter to find last row in specific column (e.g., to find last row in column B even if other columns have more data).</use_case>

    <limitation>Scans entire sheet - slow on large datasets. Considers formatting/formulas as data. Cannot distinguish between actual data and artifacts.</limitation>

    <failure_cases>May return unexpected high row numbers if sheet has trailing formatting. Slow performance on sheets >10,000 rows. Returns row 0 for completely empty sheets.</failure_cases>

    Args:
        uri: Google Sheets URI (full URL with gid parameter)
        column: Optional column letter (e.g., "B", "AA") to search in specific column only

    Returns:
        GetLastRowResponse containing:
            - success: Whether the operation succeeded
            - row_number: 1-based row number of last row with data (0 for empty sheet)
            - spreadsheet_id: The spreadsheet ID
            - spreadsheet_url: Full URL to the spreadsheet
            - worksheet: The worksheet name
            - message: Human-readable result message
            - error: Error message if failed, None otherwise

    Examples:
        # Get last row across all columns
        result = get_last_row(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/18iaWb8OUFdNldk03ESY6indsfrURlMsyBwqwMIRkYJY/edit?gid=1435041919#gid=1435041919"
        )

        if result.success:
            print(f"Last row with data: {result.row_number}")
            print(f"Next available row for append: {result.row_number + 1}")

        # Get last row in column B only
        result = get_last_row(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/18iaWb8OUFdNldk03ESY6indsfrURlMsyBwqwMIRkYJY/edit?gid=1435041919#gid=1435041919",
            column="B"
        )

        if result.success:
            print(f"Last row in column B: {result.row_number}")
    """
    try:
        # Parse URI to extract spreadsheet_id and gid
        spreadsheet_id, gid = parse_google_sheets_uri(uri)

        # Get sheet properties (title, etc.)
        sheet_properties = await get_sheet_by_gid(service, spreadsheet_id, gid)
        sheet_title = sheet_properties['title']
        sheet_id = sheet_properties['sheetId']

        # Construct spreadsheet URL with gid
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

        # Get last row (with optional column parameter)
        last_row = await get_last_row_with_data(service, spreadsheet_id, sheet_title, column)

        # Customize message based on whether column was specified
        if column:
            if last_row > 0:
                message = f"Successfully retrieved last row in column {column}: {last_row}"
            else:
                message = f"No data found in column {column}"
        else:
            if last_row > 0:
                message = f"Successfully retrieved last row: {last_row}"
            else:
                message = "No data found in worksheet"

        return GetLastRowResponse(
            success=True,
            row_number=last_row,
            spreadsheet_id=spreadsheet_id,
            spreadsheet_url=spreadsheet_url,
            worksheet=sheet_title,
            message=message
        )

    except Exception as e:
        logger.error(f"Error getting last row: {e}")
        return GetLastRowResponse(
            success=False,
            row_number=0,
            spreadsheet_id="",
            spreadsheet_url="",
            worksheet="",
            message=f"Failed to get last row",
            error=str(e)
        )


@mcp.tool
@require_google_service("sheets", "sheets_read")
async def get_used_range(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}#gid={gid})"
    )
) -> GetUsedRangeResponse:
    """
    Auto-detects and returns the minimal rectangular range containing all non-empty cells.

    Returns only range boundary information (like A1:C10), row/column counts, and start cell -
    does NOT return actual cell data or values.

    <description>Auto-detects and returns the minimal rectangular range containing all non-empty cells. Returns only range boundary information (like A1:C10), row/column counts, and start cell - does NOT return actual cell data or values.</description>

    <use_case>Use when you need to find actual data boundaries in unknown spreadsheets, before bulk operations, or to avoid processing empty regions. Combine with read_sheet to retrieve actual data from the detected range.</use_case>

    <limitation>Includes cells with spaces or formulas as "used" - not just visible data. Cannot detect non-contiguous data regions. Returns range metadata only - no cell values. Slow on very large sheets (>100,000 cells).</limitation>

    <failure_cases>Returns entire sheet range if contains scattered formatting. May include hidden or formula cells unexpectedly. Timeouts on sheets with extensive formatting but no data.</failure_cases>

    Args:
        uri: Google Sheets URI (full URL with gid parameter)

    Returns:
        GetUsedRangeResponse containing:
            - success: Whether the operation succeeded
            - used_range: A1 notation like "A1:C10"
            - row_count: Number of rows in used range
            - column_count: Number of columns in used range
            - start_cell: Top-left cell (always "A1")
            - end_cell: Bottom-right cell like "C10"
            - spreadsheet_id: The spreadsheet ID
            - worksheet: The worksheet name
            - message: Human-readable result message
            - error: Error message if failed, None otherwise

    Examples:
        # Get used range for a Google Sheets
        result = get_used_range(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/18iaWb8OUFdNldk03ESY6indsfrURlMsyBwqwMIRkYJY/edit?gid=1435041919#gid=1435041919"
        )

        if result.success:
            print(f"Used range: {result.used_range}")
            print(f"Dimensions: {result.row_count} rows x {result.column_count} columns")
            print(f"From {result.start_cell} to {result.end_cell}")

            # Now read the actual data from this range
            data = read_sheet(ctx, uri, range_address=result.used_range)
    """
    try:
        # Parse URI to extract spreadsheet_id and gid
        spreadsheet_id, gid = parse_google_sheets_uri(uri)

        # Get sheet properties (title, etc.)
        sheet_properties = await get_sheet_by_gid(service, spreadsheet_id, gid)
        sheet_title = sheet_properties['title']
        sheet_id = sheet_properties['sheetId']

        # Construct spreadsheet URL with gid
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

        # Get used range info
        used_range, row_count, column_count, start_cell, end_cell = await get_used_range_info(
            service, spreadsheet_id, sheet_title
        )

        return GetUsedRangeResponse(
            success=True,
            used_range=used_range,
            row_count=row_count,
            column_count=column_count,
            start_cell=start_cell,
            end_cell=end_cell,
            spreadsheet_id=spreadsheet_id,
            spreadsheet_url=spreadsheet_url,
            worksheet=sheet_title,
            message=f"Successfully retrieved used range: {used_range} ({row_count}x{column_count})" if row_count > 0 else "No data found in worksheet"
        )

    except Exception as e:
        logger.error(f"Error getting used range: {e}")
        return GetUsedRangeResponse(
            success=False,
            used_range="A1:A1",
            row_count=0,
            column_count=0,
            start_cell="A1",
            end_cell="A1",
            spreadsheet_id="",
            spreadsheet_url="",
            worksheet="",
            message=f"Failed to get used range",
            error=str(e)
        )


@mcp.tool
@require_google_service("sheets", "sheets_read")
async def get_last_column(
    service,  # Injected by @require_google_service
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}#gid={gid})"
    )
) -> GetLastColumnResponse:
    """
    Identifies and returns the rightmost column containing any data.

    Scans entire sheet to find last column with non-empty cells. Useful to determine
    table width, before adding new columns, or finding data boundaries.

    <description>Identifies and returns the rightmost column containing any data. Scans entire sheet to find last column with non-empty cells.</description>

    <use_case>Use to determine table width, before adding new columns, or finding data boundaries for horizontal processing limits.</use_case>

    <limitation>Scans entire sheet - slow on wide datasets. Considers formatting/formulas as data. Cannot distinguish between actual data and artifacts.</limitation>

    <failure_cases>May return unexpected high column numbers if sheet has trailing formatting. Slow performance on sheets >1000 columns. Returns column A for completely empty sheets.</failure_cases>

    Args:
        uri: Google Sheets URI (full URL with gid parameter)

    Returns:
        GetLastColumnResponse containing:
            - success: Whether the operation succeeded
            - column: Column letter like "A", "Z", "AA"
            - column_index: 0-based column index
            - spreadsheet_id: The spreadsheet ID
            - worksheet: The worksheet name
            - message: Human-readable result message
            - error: Error message if failed, None otherwise

    Examples:
        # Get last column for a Google Sheets
        result = get_last_column(
            ctx,
            uri="https://docs.google.com/spreadsheets/d/18iaWb8OUFdNldk03ESY6indsfrURlMsyBwqwMIRkYJY/edit?gid=1435041919#gid=1435041919"
        )

        if result.success:
            print(f"Last column with data: {result.column} (index {result.column_index})")
            print(f"Table width: {result.column_index + 1} columns")
    """
    try:
        # Parse URI to extract spreadsheet_id and gid
        spreadsheet_id, gid = parse_google_sheets_uri(uri)

        # Get sheet properties (title, etc.)
        sheet_properties = await get_sheet_by_gid(service, spreadsheet_id, gid)
        sheet_title = sheet_properties['title']
        sheet_id = sheet_properties['sheetId']

        # Construct spreadsheet URL with gid
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

        # Get last column
        last_column, column_index = await get_last_column_with_data(service, spreadsheet_id, sheet_title)

        return GetLastColumnResponse(
            success=True,
            column=last_column,
            column_index=column_index,
            spreadsheet_id=spreadsheet_id,
            spreadsheet_url=spreadsheet_url,
            worksheet=sheet_title,
            message=f"Successfully retrieved last column: {last_column} (index {column_index})" if last_column != "A" or column_index > 0 else "No data found in worksheet"
        )

    except Exception as e:
        logger.error(f"Error getting last column: {e}")
        return GetLastColumnResponse(
            success=False,
            column="A",
            column_index=0,
            spreadsheet_id="",
            spreadsheet_url="",
            worksheet="",
            message=f"Failed to get last column",
            error=str(e)
        )


