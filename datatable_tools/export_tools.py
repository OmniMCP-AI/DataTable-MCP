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
    export_format: str,
    file_path: Optional[str] = None,
    return_content: bool = False,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    spreadsheet_name: Optional[str] = None,
    worksheet_id: Optional[str] = None,
    user_id: Optional[str] = None,
    columns_name: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Export table to multiple formats (CSV, JSON, Excel, Parquet, Google Sheets).

    Args:
        table_id: ID of the table to export
        export_format: Export format ("csv", "json", "excel", "parquet", "google_sheets")
        file_path: Optional file path to save to (if None and return_content=False, generates temp file)
        return_content: If True, returns content in response instead of saving to file
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)
        spreadsheet_id: Google Sheets spreadsheet ID (optional for google_sheets format)
        spreadsheet_name: Name for new spreadsheet (optional for google_sheets format)
        worksheet_id: Google Sheets worksheet ID (optional)
        user_id: User ID for Google Sheets authentication (required for google_sheets)
        columns_name: Column headers to match against for Google Sheets (optional)

    Returns:
        Dict containing export results and file information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Source table does not exist"
            }

        # Validate required parameters for Google Sheets
        if export_format == "google_sheets" and not user_id:
            return {
                "success": False,
                "error": "user_id is required for google_sheets export",
                "message": "Please provide a user_id for Google Sheets authentication"
            }

        # Use new exporter system for supported formats
        if export_format in ["google_sheets", "excel", "csv", "json"]:
            from datatable_tools.data_exporters import create_exporter

            export_params = {
                "ctx": ctx,
                "user_id": user_id,
                "file_path": file_path,
                "return_content": return_content,
                "encoding": encoding,
                "delimiter": delimiter,
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_name": spreadsheet_name,
                "worksheet": worksheet_id,
                "columns_name": columns_name
            }

            exporter = create_exporter(export_format, **export_params)
            return await exporter.export_data(table, **export_params)

        # Legacy code for parquet format
        elif export_format == "parquet":
            df = table.df
            content = None
            actual_file_path = file_path

            # Generate file path if not provided and not returning content
            if not file_path and not return_content:
                import tempfile
                temp_dir = tempfile.gettempdir()
                safe_name = table.metadata.name.replace(" ", "_").replace("/", "_")
                actual_file_path = os.path.join(temp_dir, f"{safe_name}_{table_id}.{export_format}")

            if return_content:
                # Return Parquet content as base64 for binary data
                import base64
                buffer = io.BytesIO()
                df.to_parquet(buffer)
                content = base64.b64encode(buffer.getvalue()).decode()
            else:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(actual_file_path), exist_ok=True)
                df.to_parquet(actual_file_path)

            result = {
                "success": True,
                "table_id": table_id,
                "export_format": export_format,
                "return_content": return_content,
                "rows_exported": len(df),
                "columns_exported": len(df.columns),
                "table_name": table.metadata.name,
            }

            if return_content:
                result["content"] = content
                result["content_type"] = "base64"
                result["message"] = f"Exported table {table_id} as {export_format} content"
            else:
                result["file_path"] = actual_file_path
                result["file_size"] = os.path.getsize(actual_file_path) if os.path.exists(actual_file_path) else 0
                result["message"] = f"Exported table {table_id} to {export_format} file: {actual_file_path}"

            return result

        else:
            return {
                "success": False,
                "error": f"Unsupported export format: {export_format}",
                "message": "export_format must be one of: csv, json, excel, parquet, google_sheets"
            }

    except Exception as e:
        logger.error(f"Error exporting table {table_id} as {export_format}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export table {table_id} as {export_format}"
        }

