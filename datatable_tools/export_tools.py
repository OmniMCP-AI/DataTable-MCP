from typing import Dict, List, Optional, Any
import logging
import io
import os
import pandas as pd
from pathlib import Path
from fastmcp import Context
from core.server import mcp
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

@mcp.tool
async def export_table(
    ctx: Context,
    table_id: str,
    uri: str,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export table to various formats using URI-based auto-detection.

    Args:
        table_id: ID of the table to export
        uri: URI for the export destination. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{id}/edit or spreadsheet ID
             - CSV files: /path/to/file.csv
             - Excel files: /path/to/file.xlsx
             - JSON files: /path/to/file.json
             - Parquet files: /path/to/file.parquet
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)

    Returns:
        Dict containing export results and file information

    Examples:
        # Export to CSV
        uri = "/path/to/data.csv"

        # Export to Google Sheets
        uri = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"

        # Export to Excel
        uri = "/path/to/workbook.xlsx"
    """
    try:
        from datatable_tools.utils import parse_export_uri

        # Get the table
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Source table does not exist"
            }

        # Parse the URI to determine export type and parameters
        export_info = parse_export_uri(uri)
        export_type = export_info["export_type"]

        logger.info(f"Exporting table {table_id} to {export_type}: {uri}")

        # Handle Google Sheets export with authentication
        if export_type == "google_sheets":
            return await _export_google_sheets(ctx, table, export_info)

        # Handle file-based exports
        return await _export_file(table, export_info, encoding, delimiter)

    except Exception as e:
        logger.error(f"Error exporting table {table_id} to {uri}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export table {table_id} to {uri}"
        }


@mcp.tool
async def export_data(
    ctx: Context,
    data: Any,
    uri: str,
    headers: Optional[List[str]] = None,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export data directly to various formats using URI-based auto-detection.
    Accepts flexible data types like create_table.

    Args:
        data: Data in various formats:
              - List[List[Any]]: 2D array of table data (rows x columns)
              - Dict[str, List]: Dictionary with column names as keys and column data as values
              - Dict[str, Any]: Dictionary with column names as keys and scalar/sequence values
              - List[Dict]: List of dictionaries (records format)
              - pandas.DataFrame: Existing DataFrame
              - pandas.Series: Single column data
              - List[Any]: Single column or row data
              - scalar: Single value
        uri: URI for the export destination. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{id}/edit or spreadsheet ID
             - CSV files: /path/to/file.csv
             - Excel files: /path/to/file.xlsx
             - JSON files: /path/to/file.json
             - Parquet files: /path/to/file.parquet
        headers: Optional column headers. If not provided, will auto-generate or use data keys
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)

    Returns:
        Dict containing export results and file information

    Examples:
        # Export DataFrame to Google Sheets
        data = pd.DataFrame({"A": [1,2], "B": [3,4]})
        uri = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"

        # Export dictionary to CSV
        data = {"Name": ["Alice", "Bob"], "Age": [25, 30]}
        uri = "/path/to/data.csv"

        # Export single value to Excel
        data = 42
        headers = ["Value"]
        uri = "/path/to/value.xlsx"
    """
    try:
        from datatable_tools.utils import parse_export_uri
        from datatable_tools.lifecycle_tools import _process_data_input

        # Process data into standardized format
        processed_data, processed_headers = _process_data_input(data, headers)

        # Parse the URI to determine export type and parameters
        export_info = parse_export_uri(uri)
        export_type = export_info["export_type"]

        logger.info(f"Exporting data to {export_type}: {uri}")

        # Handle Google Sheets export with authentication
        if export_type == "google_sheets":
            return await _export_data_google_sheets(ctx, processed_data, processed_headers, export_info)

        # Handle file-based exports
        return await _export_data_file(processed_data, processed_headers, export_info, encoding, delimiter)

    except Exception as e:
        logger.error(f"Error exporting data to {uri}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export data to {uri}"
        }


async def _export_data_google_sheets(ctx: Context, processed_data: List[List[Any]], processed_headers: List[str], export_info: dict) -> Dict[str, Any]:
    """
    Internal function to export processed data to Google Sheets with authentication.
    """
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
            service=None,  # Will be injected by decorator
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
    """
    Internal function to export processed data to file-based formats.
    """
    import pandas as pd

    export_type = export_info["export_type"]
    file_path = export_info["file_path"]

    # Create DataFrame from processed data
    df = pd.DataFrame(processed_data, columns=processed_headers)

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)

    # Export based on format
    if export_type == "csv":
        csv_params = {}
        if encoding:
            csv_params["encoding"] = encoding
        if delimiter:
            csv_params["sep"] = delimiter
        df.to_csv(file_path, index=False, **csv_params)

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


async def _export_google_sheets(ctx: Context, table, export_info: dict) -> Dict[str, Any]:
    """
    Internal function to export to Google Sheets with authentication.
    """
    try:
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

        # Get table data
        df = table.df

        # Convert to lists for Google Sheets
        headers = df.columns.tolist()
        data = df.values.tolist()

        # Convert all values to strings for Google Sheets
        data = [[str(cell) for cell in row] for row in data]

        # Extract parameters from export_info
        spreadsheet_id = export_info.get("spreadsheet_id")
        sheet_name = export_info.get("sheet_name")

        # Use the write_sheet_structured method
        result = await GoogleSheetsService.write_sheet_structured(
            service=None,  # Will be injected by decorator
            ctx=ctx,
            spreadsheet_identifier=spreadsheet_id,
            data=data,
            headers=headers,
            sheet_name=sheet_name
        )

        # Add table metadata to the result
        result.update({
            "table_id": table.table_id,
            "table_name": table.metadata.name,
            "export_type": "google_sheets",
            "rows_exported": len(data),
            "columns_exported": len(headers),
            "original_uri": export_info["original_uri"]
        })

        return result

    except Exception as e:
        logger.error(f"Error exporting to Google Sheets: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export table to Google Sheets: {str(e)}"
        }


async def _export_file(table, export_info: dict, encoding: Optional[str], delimiter: Optional[str]) -> Dict[str, Any]:
    """
    Internal function to export to file-based formats.
    """
    export_type = export_info["export_type"]
    file_path = export_info["file_path"]
    df = table.df

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)

    # Export based on format
    if export_type == "csv":
        csv_params = {}
        if encoding:
            csv_params["encoding"] = encoding
        if delimiter:
            csv_params["sep"] = delimiter
        df.to_csv(file_path, index=False, **csv_params)

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
        "table_id": table.table_id,
        "export_type": export_type,
        "file_path": file_path,
        "file_size": file_size,
        "rows_exported": len(df),
        "columns_exported": len(df.columns),
        "table_name": table.metadata.name,
        "original_uri": export_info["original_uri"],
        "message": f"Exported table '{table.metadata.name}' ({len(df)} rows) to {export_type}: {file_path}"
    }

