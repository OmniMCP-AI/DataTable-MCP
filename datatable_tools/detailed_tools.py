from typing import Dict, List, Optional, Any, Union
import logging
from fastmcp import Context
from core.server import mcp
from datatable_tools.range_operations import range_operations
from datatable_tools.utils import parse_google_sheets_url

logger = logging.getLogger(__name__)

@mcp.tool()
async def update_range(
    ctx: Context,
    uri: str,
    range_address: str,
    data: Any,
    headers: Optional[List[str]] = None,
    worksheet: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a range in a Google Spreadsheet using URI-based identification.
    Supports various data formats similar to pd.DataFrame and create_table.

    Args:
        uri: Google Sheets URL or spreadsheet ID
             - https://docs.google.com/spreadsheets/d/{id}/edit
             - {spreadsheet_id}
        range_address: Range in A1 notation:
                      - Single cell: "B5"
                      - Row range: "A1:E1" or "1:1"
                      - Column range: "B:B" or "B1:B10"
                      - 2D range: "A1:C3"
        data: Data in various formats (similar to pd.DataFrame):
              - List[List[Any]]: 2D array of table data (rows x columns)
              - Dict[str, List]: Dictionary with column names as keys and column data as values
              - Dict[str, Any]: Dictionary with column names as keys and scalar/sequence values
              - List[Dict]: List of dictionaries (records format)
              - pandas.DataFrame: Existing DataFrame
              - pandas.Series: Single column data
              - List[Any]: Single column or row data
              - scalar: Single value
        headers: Optional column headers for dictionary data
        worksheet: Worksheet name override (optional, defaults to first sheet)

    Returns:
        Dict containing update results and spreadsheet information

    Examples:
        # Update single cell with scalar
        update_range(ctx, uri, "B5", "New Value")

        # Update row with list
        update_range(ctx, uri, "A1:C1", ["Name", "Age", "City"])

        # Update range with DataFrame-like dict
        update_range(ctx, uri, "A1:C2", {"Name": ["Alice", "Bob"], "Age": [25, 30], "City": ["NYC", "LA"]})

        # Update with records format
        update_range(ctx, uri, "A1:C2", [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}])

        # Update with pandas DataFrame
        import pandas as pd
        df = pd.DataFrame({"Name": ["Alice"], "Age": [25]})
        update_range(ctx, uri, "A1:B1", df)
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

    try:
        # Determine the type of range update needed
        import re

        # Check if it's a single cell (e.g., "B5")
        single_cell_pattern = r'^[A-Z]+\d+$'
        if re.match(single_cell_pattern, range_address):
            return await range_operations.update_cell(
                spreadsheet_id=spreadsheet_id,
                worksheet=final_worksheet,
                cell_address=range_address,
                value=data,
                user_id=""
            )

        # For ranges, we'll use the GoogleSheetsService directly through range_operations
        # Create the full range notation
        full_range = f"{final_worksheet}!{range_address}"

        # Format data as 2D array for Google Sheets API
        if isinstance(data, list):
            if data and isinstance(data[0], list):
                # Already 2D
                values = [[str(cell) for cell in row] for row in data]
            else:
                # 1D list - need to determine if it's row or column
                if ':' in range_address:
                    # Parse range to determine orientation
                    if range_address.count(':') == 1:
                        start, end = range_address.split(':')
                        # Check if it's a row range (same row number) or column range (same column)
                        start_match = re.match(r'([A-Z]+)(\d+)', start)
                        end_match = re.match(r'([A-Z]+)(\d+)', end)

                        if start_match and end_match:
                            start_col, start_row = start_match.groups()
                            end_col, end_row = end_match.groups()

                            if start_row == end_row:
                                # Row range - data goes horizontally
                                values = [[str(val) for val in data]]
                            else:
                                # Column range - data goes vertically
                                values = [[str(val)] for val in data]
                        else:
                            # Default to row format
                            values = [[str(val) for val in data]]
                    else:
                        values = [[str(val) for val in data]]
                else:
                    values = [[str(val) for val in data]]
        else:
            # Single value
            values = [[str(data)]]

        # Use the GoogleSheetsService directly
        success = await range_operations.google_sheets_service.update_range(
            user_id="",
            spreadsheet_id=spreadsheet_id,
            range_notation=full_range,
            values=values
        )

        if success:
            return {
                "success": True,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": final_worksheet,
                "range": range_address,
                "updated_cells": sum(len(row) for row in values),
                "message": f"Successfully updated range {range_address} in worksheet '{final_worksheet}'"
            }
        else:
            return {
                "success": False,
                "error": "Failed to update range",
                "message": f"Failed to update range {range_address} in worksheet '{final_worksheet}'"
            }

    except Exception as e:
        logger.error(f"Error updating range {range_address}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Error updating range {range_address}: {e}"
        }

@mcp.tool()
async def export_table_to_range(
    ctx: Context,
    table_id: str,
    uri: str,
    start_cell: str,
    include_headers: bool = True,
    worksheet: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export DataTable content to a specific range in Google Spreadsheet using URI-based identification.
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