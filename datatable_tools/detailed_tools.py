from typing import Dict, List, Optional, Any
import logging
from fastmcp import Context
from core.server import mcp
from datatable_tools.range_operations import range_operations
from datatable_tools.utils import parse_google_sheets_url
from datatable_tools.lifecycle_tools import _process_data_input

logger = logging.getLogger(__name__)

@mcp.tool
async def append_rows(
    ctx: Context,
    uri: str,
    data: Any,  # Union[List[List[Any]], Dict[str, List], List[Dict], pd.DataFrame]
    headers: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Append data as new rows below existing data in Google Sheets.
    Automatically detects the last row and appends below it starting from column A.

    Args:
        uri: Google Sheets URI. Supports:
             - https://docs.google.com/spreadsheets/d/{id}/edit
             - spreadsheet ID
        data: Data in pandas-like formats. Accepts:
              - List[List[Any]]: 2D array of table data (rows x columns)
              - Dict[str, List]: Dictionary with column names as keys and column data as values
              - List[Dict]: List of dictionaries (records format)
              - pd.DataFrame: Existing DataFrame
        headers: Optional column headers. If None and first row contains short strings followed
                by rows with longer content (>50 chars), headers will be auto-detected and
                extracted from the first row.

    Returns:
        Dict containing update results and file/spreadsheet information

    Examples:
        # Append new records to Google Sheets
        append_rows(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit",
                   data=[["John", 25], ["Jane", 30]])

        # Append with explicit headers
        append_rows(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit",
                   data=[["John", 25]], headers=["name", "age"])

        # Append with auto-detected headers (first row = headers if long content follows)
        append_rows(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit",
                   data=[["name", "description"],
                         ["Item1", "This is a long description that will trigger header detection"]])
    """
    try:
        from datatable_tools.utils import parse_google_sheets_url

        spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
        if not spreadsheet_id:
            raise ValueError(f"Invalid Google Sheets URI: {uri}")

        return await _handle_google_sheets_append(
            ctx, uri, data, headers, spreadsheet_id, sheet_name, append_mode="rows"
        )

    except Exception as e:
        logger.error(f"Error appending rows to {uri}: {e}")
        raise


@mcp.tool
async def append_columns(
    ctx: Context,
    uri: str,
    data: Any,  # Union[List[List[Any]], Dict[str, List], List[Dict], pd.DataFrame]
    headers: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Append data as new columns to the right of existing data in Google Sheets.
    Automatically detects the last column and appends to its right starting from row 1.

    Args:
        uri: Google Sheets URI. Supports:
             - https://docs.google.com/spreadsheets/d/{id}/edit
             - spreadsheet ID
        data: Data in pandas-like formats. Accepts:
              - List[List[Any]]: 2D array of table data (rows x columns)
              - Dict[str, List]: Dictionary with column names as keys and column data as values
              - List[Dict]: List of dictionaries (records format)
              - pd.DataFrame: Existing DataFrame
        headers: Optional column headers. If None and first row contains short strings followed
                by rows with longer content (>50 chars), headers will be auto-detected and
                extracted from the first row.

    Returns:
        Dict containing update results and file/spreadsheet information

    Examples:
        # Append new columns to Google Sheets
        append_columns(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit",
                      data=[["Feature1"], ["Feature2"]], headers=["new_feature"])
    """
    try:
        from datatable_tools.utils import parse_google_sheets_url

        spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
        if not spreadsheet_id:
            raise ValueError(f"Invalid Google Sheets URI: {uri}")

        return await _handle_google_sheets_append(
            ctx, uri, data, headers, spreadsheet_id, sheet_name, append_mode="columns"
        )

    except Exception as e:
        logger.error(f"Error appending columns to {uri}: {e}")
        raise


@mcp.tool
async def update_range(
    ctx: Context,
    uri: str,
    data: Any,  # Union[List[List[Any]], Dict[str, List], List[Dict], pd.DataFrame]
    range_address: Optional[str] = None,
    headers: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Update data in Google Sheets with precise range placement.

    Args:
        uri: Google Sheets URI. Supports:
             - https://docs.google.com/spreadsheets/d/{id}/edit
             - spreadsheet ID
        data: Data in pandas-like formats. Accepts:
              - List[List[Any]]: 2D array of table data (rows x columns)
              - Dict[str, List]: Dictionary with column names as keys and column data as values
              - List[Dict]: List of dictionaries (records format)
              - pd.DataFrame: Existing DataFrame
        range_address: Range in A1 notation (optional):
                      - Single cell: "B5"
                      - Row range: "A1:E1" or "1:1"
                      - Column range: "B:B" or "B1:B10"
                      - 2D range: "A1:C3"
                      If not provided, replaces entire sheet.
                      If the data dimensions exceed the range, the range will be automatically expanded.
        headers: Optional column headers. If None and first row contains short strings followed
                by rows with longer content (>50 chars), headers will be auto-detected and
                extracted from the first row.

    Returns:
        Dict containing update results and spreadsheet information

    Examples:
        # Update specific range in Google Sheets
        update_range(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit", data, "B5")

        # Replace entire sheet
        update_range(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit", data)

        # With auto-detected headers (first row = headers if long content follows)
        update_range(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit",
                    data=[["name", "description"],
                          ["Item1", "This is a long description that will trigger header detection"]])

        # With explicit headers
        update_range(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit",
                    data=[["John", 25]], headers=["name", "age"])

        # Range auto-expansion: data (2x3) in range A1:B2 will expand to A1:C3
        update_range(ctx, "https://docs.google.com/spreadsheets/d/{id}/edit", large_data, "A1:B2")
    """
    try:
        from datatable_tools.utils import parse_google_sheets_url

        spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
        if not spreadsheet_id:
            raise ValueError(f"Invalid Google Sheets URI: {uri}")

        return await _handle_google_sheets_update(
            ctx, uri, data, range_address, headers, spreadsheet_id, sheet_name
        )

    except Exception as e:
        logger.error(f"Error updating data to {uri}: {e}")
        raise


async def _handle_google_sheets_append(
    ctx: Context,
    uri: str,
    data: Any,
    headers: Optional[List[str]],
    spreadsheet_id: str,
    sheet_name: Optional[str],
    append_mode: str  # "rows" or "columns"
) -> Dict[str, Any]:
    """Handle Google Sheets append operations (rows or columns)"""
    if not spreadsheet_id:
        return {
            "success": False,
            "error": "Invalid Google Sheets URI",
            "message": f"Could not parse spreadsheet ID from URI: {uri}"
        }

    final_worksheet = sheet_name or "Sheet1"

    # Process data input using the same logic as create_table
    processed_data, processed_headers = _process_data_input(data, headers)

    # Convert processed data to Google Sheets format
    if not processed_data:
        values = [[""]]  # Empty cell
    else:
        # Convert all values to strings for Google Sheets API
        values = [[str(cell) for cell in row] for row in processed_data]

    from datatable_tools.third_party.google_sheets.service import GoogleSheetsService
    try:
        # Get current worksheet info to determine used range
        worksheet_info = await GoogleSheetsService.get_worksheet_info(
            ctx=ctx,
            spreadsheet_id=spreadsheet_id,
            sheet_name=final_worksheet
        )

        # Calculate append position based on mode
        if append_mode == "rows":
            # Append below the last row, starting from column A
            start_row = worksheet_info["row_count"] + 1
            start_col_index = 0
        elif append_mode == "columns":
            # Append to the right of the last column, starting from row 1
            start_row = 1
            start_col_index = worksheet_info["col_count"]
        else:
            # Fallback to A1
            start_row = 1
            start_col_index = 0

        # Convert column index to letter (A=0, B=1, ..., Z=25, AA=26, etc.)
        def col_index_to_letter(index):
            result = ""
            while index >= 0:
                result = chr(65 + index % 26) + result
                index = index // 26 - 1
                if index < 0:
                    break
            return result

        start_col = col_index_to_letter(start_col_index)

        # Calculate end position based on data size
        end_row = start_row + len(values) - 1
        end_col_index = start_col_index + (len(values[0]) if values else 1) - 1
        end_col = col_index_to_letter(end_col_index)

        # Create the range address
        range_address = f"{start_col}{start_row}:{end_col}{end_row}"

    except Exception as e:
        logger.error(f"Failed to auto-detect append position: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to determine append position for {append_mode}"
        }

    # Use the range update logic
    full_range = f"{final_worksheet}!{range_address}"

    # Use the GoogleSheetsService directly
    success = await range_operations.google_sheets_service.update_range(
        ctx=ctx,
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
            "append_mode": append_mode,
            "updated_cells": sum(len(row) for row in values),
            "data_shape": (len(values), len(values[0]) if values else 0),
            "message": f"Successfully appended {append_mode} at {range_address} in worksheet '{final_worksheet}'"
        }
    else:
        return {
            "success": False,
            "error": "Failed to append data",
            "message": f"Failed to append {append_mode} at {range_address} in worksheet '{final_worksheet}'"
        }


async def _handle_google_sheets_update(
    ctx: Context,
    uri: str,
    data: Any,
    range_address: Optional[str],
    headers: Optional[List[str]],
    spreadsheet_id: str,
    sheet_name: Optional[str]
) -> Dict[str, Any]:
    """Handle Google Sheets updates with range support"""
    if not spreadsheet_id:
        return {
            "success": False,
            "error": "Invalid Google Sheets URI",
            "message": f"Could not parse spreadsheet ID from URI: {uri}"
        }

    final_worksheet = sheet_name or "Sheet1"

    # Process data input using the same logic as create_table
    processed_data, processed_headers = _process_data_input(data, headers)

    # Convert processed data to Google Sheets format
    if not processed_data:
        values = [[""]]  # Empty cell
    else:
        # Convert all values to strings for Google Sheets API
        values = [[str(cell) for cell in row] for row in processed_data]

    if range_address:
        # Range-specific update with auto-expansion if data doesn't fit
        import re

        # Parse the range to understand its dimensions
        def parse_range(range_str):
            """Parse range like A1:B2 to get start and end cells"""
            if ':' in range_str:
                start_cell, end_cell = range_str.split(':')
            else:
                start_cell = end_cell = range_str
            return start_cell, end_cell

        def cell_to_indices(cell):
            """Convert cell like A1 to (0, 0) (row, col)"""
            match = re.match(r'([A-Z]+)(\d+)', cell)
            if not match:
                raise ValueError(f"Invalid cell format: {cell}")
            col_str, row_str = match.groups()

            # Convert column letters to index
            col_index = 0
            for i, char in enumerate(reversed(col_str)):
                col_index += (ord(char) - ord('A') + 1) * (26 ** i)
            col_index -= 1  # Convert to 0-based

            row_index = int(row_str) - 1  # Convert to 0-based
            return row_index, col_index

        def indices_to_cell(row_index, col_index):
            """Convert (row, col) indices to cell like A1"""
            # Convert column index to letters
            col_str = ""
            col_index += 1  # Convert to 1-based
            while col_index > 0:
                col_index -= 1
                col_str = chr(65 + col_index % 26) + col_str
                col_index //= 26

            row_str = str(row_index + 1)  # Convert to 1-based
            return col_str + row_str

        try:
            start_cell, end_cell = parse_range(range_address)
            start_row, start_col = cell_to_indices(start_cell)
            end_row, end_col = cell_to_indices(end_cell)

            # Calculate current range dimensions
            range_rows = end_row - start_row + 1
            range_cols = end_col - start_col + 1

            # Calculate data dimensions
            data_rows = len(values)
            data_cols = len(values[0]) if values else 0

            # Expand range if data is larger
            if data_rows > range_rows or data_cols > range_cols:
                new_end_row = start_row + max(data_rows, range_rows) - 1
                new_end_col = start_col + max(data_cols, range_cols) - 1
                new_end_cell = indices_to_cell(new_end_row, new_end_col)
                range_address = f"{start_cell}:{new_end_cell}"
                logger.info(f"Expanded range to {range_address} to fit data dimensions ({data_rows}, {data_cols})")

        except Exception as e:
            logger.warning(f"Failed to parse/expand range {range_address}: {e}. Using original range.")

        # Determine if it's a single cell update
        single_cell_pattern = r'^[A-Z]+\d+$'

        if re.match(single_cell_pattern, range_address) and len(values) == 1 and len(values[0]) == 1:
            # Single cell update
            return await range_operations.update_cell(
                ctx=ctx,
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
            ctx=ctx,
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
    else:
        # Full sheet replacement using export_data functionality
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

        # Convert all values to strings for Google Sheets
        data_strings = [[str(cell) for cell in row] for row in values]
        headers_strings = [str(header) for header in processed_headers] if processed_headers else []

        # Use the write_sheet_structured method
        result = await GoogleSheetsService.write_sheet_structured(
            ctx=ctx,
            spreadsheet_identifier=spreadsheet_id,
            data=data_strings,
            headers=headers_strings,
            sheet_name=final_worksheet
        )

        # Add metadata to the result
        result.update({
            "export_type": "google_sheets",
            "rows_exported": len(data_strings),
            "columns_exported": len(headers_strings) if headers_strings else (len(data_strings[0]) if data_strings else 0),
            "original_uri": uri
        })

        return result


async def _handle_file_export(
    ctx: Context,
    uri: str,
    data: Any,
    headers: Optional[List[str]],
    encoding: Optional[str],
    delimiter: Optional[str]
) -> Dict[str, Any]:
    """Handle file-based exports"""
    from datatable_tools.utils import parse_export_uri
    from datatable_tools.lifecycle_tools import _process_data_input

    # Process data into standardized format
    processed_data, processed_headers = _process_data_input(data, headers)

    # Parse the URI to determine export type and parameters
    export_info = parse_export_uri(uri)
    export_type = export_info["export_type"]

    logger.info(f"Exporting data to {export_type}: {uri}")

    # Handle Google Sheets export without range (shouldn't reach here, but for safety)
    if export_type == "google_sheets":
        return await _export_data_google_sheets_full(ctx, processed_data, processed_headers, export_info)

    # Handle file-based exports
    return await _export_data_file(processed_data, processed_headers, export_info, encoding, delimiter)


async def _export_data_google_sheets_full(ctx: Context, processed_data: List[List[Any]], processed_headers: List[str], export_info: dict) -> Dict[str, Any]:
    """Internal function to export processed data to Google Sheets with authentication."""
    try:
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

        # Convert all values to strings for Google Sheets
        data = [[str(cell) for cell in row] for row in processed_data]
        headers = [str(header) for header in processed_headers]

        # Extract parameters from export_info
        spreadsheet_id = export_info.get("spreadsheet_id")
        sheet_name = export_info.get("sheet_name")

        # Use the write_sheet_structured method
        result = await GoogleSheetsService.write_sheet_structured(
            ctx=ctx,
            spreadsheet_identifier=spreadsheet_id,
            data=data,
            headers=headers,
            sheet_name=sheet_name
        )

        # Add data metadata to the result
        result.update({
            "export_type": "google_sheets",
            "rows_exported": len(data),
            "columns_exported": len(headers),
            "original_uri": export_info["original_uri"]
        })

        return result

    except Exception as e:
        logger.error(f"Error exporting data to Google Sheets: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export data to Google Sheets: {str(e)}"
        }


async def _export_data_file(processed_data: List[List[Any]], processed_headers: List[str], export_info: dict, encoding: Optional[str], delimiter: Optional[str]) -> Dict[str, Any]:
    """Internal function to export processed data to file-based formats."""
    import pandas as pd
    import os

    export_type = export_info["export_type"]
    file_path = export_info["file_path"]

    # Create DataFrame from processed data
    df = pd.DataFrame(processed_data, columns=processed_headers)

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)

    # Export based on format
    if export_type == "csv":
        csv_params = {"index": False}
        if encoding:
            csv_params["encoding"] = encoding
        if delimiter:
            csv_params["sep"] = delimiter
        df.to_csv(file_path, **csv_params)

    elif export_type == "excel":
        df.to_excel(file_path, index=False)

    elif export_type == "json":
        df.to_json(file_path, orient='records', indent=2)

    elif export_type == "parquet":
        df.to_parquet(file_path, index=False)

    else:
        # Generic file export as CSV
        df.to_csv(file_path, index=False)

    # Get file info
    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

    return {
        "success": True,
        "export_type": export_type,
        "file_path": file_path,
        "file_size": file_size,
        "rows_exported": len(processed_data),
        "columns_exported": len(processed_headers),
        "original_uri": export_info["original_uri"],
        "message": f"Exported data ({len(processed_data)} rows) to {export_type}: {file_path}"
    }

# @mcp.tool
async def export_table_to_range(
    ctx: Context,
    table_id: str,
    uri: str,
    start_cell: Optional[str] = "A1",
    include_headers: bool = True,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export DataTable content to various formats using URI-based auto-detection.
    For Google Sheets, supports precise range placement. For other formats, exports entire table.

    Args:
        table_id: DataTable ID to export
        uri: URI for the export destination. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{id}/edit or spreadsheet ID
             - CSV files: /path/to/file.csv
             - Excel files: /path/to/file.xlsx
             - JSON files: /path/to/file.json
             - Parquet files: /path/to/file.parquet
        start_cell: Starting cell for Google Sheets data (e.g., "A1", "B5"). Only used for Google Sheets.
        include_headers: Whether to include table headers (default True)
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)

    Returns:
        Dict containing export results and file/spreadsheet information

    Examples:
        # Export to specific range in Google Sheets
        export_table_to_range(ctx, "table1", "https://docs.google.com/spreadsheets/d/{id}/edit", "B5")

        # Export to CSV file
        export_table_to_range(ctx, "table1", "/path/to/data.csv")

        # Export to Excel file
        export_table_to_range(ctx, "table1", "/path/to/workbook.xlsx")
    """
    try:
        from datatable_tools.utils import parse_export_uri, parse_google_sheets_url
        from datatable_tools.table_manager import table_manager

        # Get the table
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Source table does not exist"
            }

        # Check if it's a Google Sheets URL first (for range-specific export)
        spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
        if spreadsheet_id and start_cell:
            # Google Sheets range-specific export
            final_worksheet = sheet_name or "Sheet1"
            return await range_operations.update_table_range(
                table_id=table_id,
                spreadsheet_id=spreadsheet_id,
                worksheet=final_worksheet,
                start_cell=start_cell,
                user_id="",
                include_headers=include_headers,
                ctx=ctx
            )

        # General export using parse_export_uri for other formats
        export_info = parse_export_uri(uri)
        export_type = export_info["export_type"]

        logger.info(f"Exporting table {table_id} to {export_type}: {uri}")

        # Handle Google Sheets export without range (entire sheet replacement)
        if export_type == "google_sheets":
            return await _export_google_sheets_full(ctx, table, export_info, include_headers)

        # Handle file-based exports
        return await _export_file_with_headers(table, export_info, encoding, delimiter, include_headers)

    except Exception as e:
        logger.error(f"Error exporting table {table_id} to {uri}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export table {table_id} to {uri}"
        }


async def _export_google_sheets_full(ctx: Context, table, export_info: dict, include_headers: bool) -> Dict[str, Any]:
    """
    Internal function to export entire table to Google Sheets with authentication.
    """
    try:
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

        # Get table data
        df = table.df

        # Convert to lists for Google Sheets
        data = df.values.tolist()

        if include_headers:
            headers = df.columns.tolist()
        else:
            headers = []

        # Convert all values to strings for Google Sheets
        data = [[str(cell) for cell in row] for row in data]

        # Extract parameters from export_info
        spreadsheet_id = export_info.get("spreadsheet_id")
        sheet_name = export_info.get("sheet_name")

        # Use the write_sheet_structured method
        result = await GoogleSheetsService.write_sheet_structured(
            ctx=ctx,
            spreadsheet_identifier=spreadsheet_id,
            data=data,
            headers=[str(h) for h in headers] if headers else [],
            sheet_name=sheet_name
        )

        # Add table metadata to the result
        result.update({
            "table_id": table.table_id,
            "table_name": table.metadata.name,
            "export_type": "google_sheets",
            "rows_exported": len(data),
            "columns_exported": len(df.columns),
            "original_uri": export_info["original_uri"],
            "include_headers": include_headers
        })

        return result

    except Exception as e:
        logger.error(f"Error exporting to Google Sheets: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export table to Google Sheets: {str(e)}"
        }


async def _export_file_with_headers(table, export_info: dict, encoding: Optional[str], delimiter: Optional[str], include_headers: bool) -> Dict[str, Any]:
    """
    Internal function to export to file-based formats with header control.
    """
    import pandas as pd
    import os

    export_type = export_info["export_type"]
    file_path = export_info["file_path"]
    df = table.df

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)

    # Export based on format
    if export_type == "csv":
        csv_params = {"index": False, "header": include_headers}
        if encoding:
            csv_params["encoding"] = encoding
        if delimiter:
            csv_params["sep"] = delimiter
        df.to_csv(file_path, **csv_params)

    elif export_type == "excel":
        df.to_excel(file_path, index=False, header=include_headers)

    elif export_type == "json":
        df.to_json(file_path, orient='records', indent=2)

    elif export_type == "parquet":
        df.to_parquet(file_path, index=False)

    else:
        # Generic file export as CSV
        df.to_csv(file_path, index=False, header=include_headers)

    # Get file info
    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

    return {
        "success": True,
        "table_id": table.table_id,
        "export_type": export_type,
        "file_path": file_path,
        "file_size": file_size,
        "rows_exported": len(df),
        "columns_exported": len(df.columns),
        "table_name": table.metadata.name,
        "original_uri": export_info["original_uri"],
        "include_headers": include_headers,
        "message": f"Exported table '{table.metadata.name}' ({len(df)} rows) to {export_type}: {file_path}"
    }