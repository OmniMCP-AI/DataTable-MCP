from typing import Dict, List, Optional, Any, Union
import logging
from fastmcp import Context
from core.server import mcp
from datatable_tools.range_operations import range_operations
from datatable_tools.utils import parse_google_sheets_url
from datatable_tools.lifecycle_tools import _process_data_input

logger = logging.getLogger(__name__)

@mcp.tool
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
        # Process data input using the same logic as create_table
        processed_data, processed_headers = _process_data_input(data, headers)

        # Convert processed data to Google Sheets format
        if not processed_data:
            values = [[""]]  # Empty cell
        else:
            # Convert all values to strings for Google Sheets API
            values = [[str(cell) for cell in row] for row in processed_data]

        # Determine if it's a single cell update
        import re
        single_cell_pattern = r'^[A-Z]+\d+$'

        if re.match(single_cell_pattern, range_address) and len(values) == 1 and len(values[0]) == 1:
            # Single cell update
            return await range_operations.update_cell(
                spreadsheet_id=spreadsheet_id,
                worksheet=final_worksheet,
                cell_address=range_address,
                value=values[0][0],
                user_id=""
            )

        # For ranges, use the GoogleSheetsService directly
        full_range = f"{final_worksheet}!{range_address}"

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
                "data_shape": (len(values), len(values[0]) if values else 0),
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

@mcp.tool
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