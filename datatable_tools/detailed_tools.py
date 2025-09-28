from typing import Dict, List, Optional, Any
import logging
from fastmcp import Context
from core.server import mcp
from datatable_tools.range_operations import range_operations

logger = logging.getLogger(__name__)

@mcp.tool()
async def update_spreadsheet_cell(
    ctx: Context,
    spreadsheet_id: str,
    worksheet: str,
    cell_address: str,
    value: Any,
    user_id: str
) -> Dict[str, Any]:
    """
    Update a single cell in a Google Spreadsheet.
    Uses /range/update endpoint for simple, robust operation.

    Args:
        spreadsheet_id: Google Spreadsheet ID
        worksheet: Worksheet name (e.g., "Sheet1")
        cell_address: Cell address in A1 notation (e.g., "B5", "A1")
        value: Value to set in the cell
        user_id: User ID for authentication (required)

    Returns:
        Dict containing update results and spreadsheet information
    """
    return await range_operations.update_cell(
        spreadsheet_id=spreadsheet_id,
        worksheet=worksheet,
        cell_address=cell_address,
        value=value,
        user_id=user_id
    )


@mcp.tool()
async def update_spreadsheet_row(
    ctx: Context,
    spreadsheet_id: str,
    worksheet: str,
    row_number: int,
    row_data: List[Any],
    user_id: str,
    start_column: str = "A"
) -> Dict[str, Any]:
    """
    Update an entire row in a Google Spreadsheet.
    Uses /range/update endpoint for simple, robust operation.

    Args:
        spreadsheet_id: Google Spreadsheet ID
        worksheet: Worksheet name (e.g., "Sheet1")
        row_number: Row number to update (1-based, e.g., 1 for first row)
        row_data: List of values for the row
        user_id: User ID for authentication (required)
        start_column: Starting column letter (default "A")

    Returns:
        Dict containing update results and spreadsheet information
    """
    return await range_operations.update_row(
        spreadsheet_id=spreadsheet_id,
        worksheet=worksheet,
        row_number=row_number,
        row_data=row_data,
        user_id=user_id,
        start_column=start_column
    )


@mcp.tool()
async def update_spreadsheet_column(
    ctx: Context,
    spreadsheet_id: str,
    worksheet: str,
    column: str,
    column_data: List[Any],
    user_id: str,
    start_row: int = 1
) -> Dict[str, Any]:
    """
    Update an entire column in a Google Spreadsheet.
    Uses /range/update endpoint for simple, robust operation.

    Args:
        spreadsheet_id: Google Spreadsheet ID
        worksheet: Worksheet name (e.g., "Sheet1")
        column: Column letter to update (e.g., "B", "C")
        column_data: List of values for the column
        user_id: User ID for authentication (required)
        start_row: Starting row number (1-based, default 1)

    Returns:
        Dict containing update results and spreadsheet information
    """
    return await range_operations.update_column(
        spreadsheet_id=spreadsheet_id,
        worksheet=worksheet,
        column=column,
        column_data=column_data,
        user_id=user_id,
        start_row=start_row
    )


@mcp.tool()
async def update_table_to_spreadsheet_range(
    ctx: Context,
    table_id: str,
    spreadsheet_id: str,
    worksheet: str,
    start_cell: str,
    user_id: str,
    include_headers: bool = True
) -> Dict[str, Any]:
    """
    Update a specific range in Google Spreadsheet with DataTable content.
    Uses /range/update endpoint for precise control over data placement.

    Args:
        table_id: DataTable ID to export
        spreadsheet_id: Google Spreadsheet ID
        worksheet: Worksheet name (e.g., "Sheet1")
        start_cell: Starting cell for the data (e.g., "A1", "B5")
        user_id: User ID for authentication (required)
        include_headers: Whether to include table headers (default True)

    Returns:
        Dict containing export results and spreadsheet information
    """
    return await range_operations.update_table_range(
        table_id=table_id,
        spreadsheet_id=spreadsheet_id,
        worksheet=worksheet,
        start_cell=start_cell,
        user_id=user_id,
        include_headers=include_headers
    )