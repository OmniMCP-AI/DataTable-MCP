from typing import Dict, List, Optional, Any, Union
import logging
from fastmcp import Context
from core.server import mcp
from datatable_tools.range_operations import range_operations
from datatable_tools.utils import parse_google_sheets_url

logger = logging.getLogger(__name__)

@mcp.tool()
async def update_spreadsheet_range(
    ctx: Context,
    uri: str,
    range_address: str,
    data: Union[Any, List[Any], List[List[Any]]],
    worksheet: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a range in a Google Spreadsheet using URI-based identification.
    Supports single cells, rows, columns, and 2D ranges.

    Args:
        uri: Google Sheets URL or spreadsheet ID
             - https://docs.google.com/spreadsheets/d/{id}/edit
             - {spreadsheet_id}
        range_address: Range in A1 notation:
                      - Single cell: "B5"
                      - Row range: "A1:E1" or "1:1"
                      - Column range: "B:B" or "B1:B10"
                      - 2D range: "A1:C3"
        data: Data to update:
              - Single value for single cell
              - List for row/column
              - List of lists for 2D range
        worksheet: Worksheet name override (optional, defaults to first sheet)

    Returns:
        Dict containing update results and spreadsheet information

    Examples:
        # Update single cell
        update_spreadsheet_range(ctx, uri, "B5", "New Value")

        # Update row
        update_spreadsheet_range(ctx, uri, "A1:C1", ["Name", "Age", "City"])

        # Update column
        update_spreadsheet_range(ctx, uri, "B1:B3", [25, 30, 28])

        # Update 2D range
        update_spreadsheet_range(ctx, uri, "A1:C2", [
            ["Name", "Age", "City"],
            ["Alice", 25, "NYC"]
        ])
    """
    spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
    if not spreadsheet_id:
        return {
            "success": False,
            "error": "Invalid Google Sheets URI",
            "message": f"Could not parse spreadsheet ID from URI: {uri}"
        }

    # Use provided worksheet name or parsed sheet_name
    final_worksheet = worksheet or sheet_name or "Sheet1"

    # Format the full range including worksheet
    full_range = f"{final_worksheet}!{range_address}"

    return await range_operations.update_range(
        spreadsheet_id=spreadsheet_id,
        range_address=full_range,
        data=data,
        user_id=""  # Remove user_id requirement
    )

@mcp.tool()
async def update_table_to_spreadsheet_range(
    ctx: Context,
    table_id: str,
    uri: str,
    start_cell: str,
    include_headers: bool = True,
    worksheet: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a specific range in Google Spreadsheet with DataTable content using URI-based identification.
    Uses /range/update endpoint for precise control over data placement.

    Args:
        table_id: DataTable ID to export
        uri: Google Sheets URL or spreadsheet ID
             - https://docs.google.com/spreadsheets/d/{id}/edit
             - {spreadsheet_id}
        start_cell: Starting cell for the data (e.g., "A1", "B5")
        include_headers: Whether to include table headers (default True)
        worksheet: Worksheet name override (optional, defaults to first sheet)

    Returns:
        Dict containing export results and spreadsheet information
    """
    spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
    if not spreadsheet_id:
        return {
            "success": False,
            "error": "Invalid Google Sheets URI",
            "message": f"Could not parse spreadsheet ID from URI: {uri}"
        }

    # Use provided worksheet name or parsed sheet_name
    final_worksheet = worksheet or sheet_name or "Sheet1"

    return await range_operations.update_table_range(
        table_id=table_id,
        spreadsheet_id=spreadsheet_id,
        worksheet=final_worksheet,
        start_cell=start_cell,
        user_id="",  # Remove user_id requirement
        include_headers=include_headers
    )