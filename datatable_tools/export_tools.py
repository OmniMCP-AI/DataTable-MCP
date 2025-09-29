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

@mcp.tool()
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


async def _export_google_sheets(ctx: Context, table, export_info: dict) -> Dict[str, Any]:
    """
    Internal function to export to Google Sheets with authentication.
    """
    # This would need the Google Sheets authentication decorator
    # For now, return a placeholder
    return {
        "success": False,
        "error": "Google Sheets export not yet implemented with new URI system",
        "message": "Google Sheets export is being refactored to use the new URI-based system"
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

