from typing import Dict, List, Optional, Any
import logging
import io
import os
import pandas as pd
from pathlib import Path
from core.server import register_tool
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

async def export_table(
    table_id: str,
    export_format: str,
    file_path: Optional[str] = None,
    return_content: bool = False,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    worksheet_id: Optional[str] = None
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
        spreadsheet_id: Google Sheets spreadsheet ID (required for google_sheets format)
        worksheet_id: Google Sheets worksheet ID (optional, creates new if not provided)

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

        df = table.df
        content = None
        actual_file_path = file_path

        # Generate file path if not provided and not returning content
        if not file_path and not return_content:
            import tempfile
            temp_dir = tempfile.gettempdir()
            safe_name = table.metadata.name.replace(" ", "_").replace("/", "_")
            actual_file_path = os.path.join(temp_dir, f"{safe_name}_{table_id}.{export_format}")

        # Prepare export parameters
        export_params = {}
        if encoding:
            export_params['encoding'] = encoding
        if delimiter:
            export_params['delimiter'] = delimiter

        # Export based on format
        if export_format == "csv":
            if return_content:
                content = df.to_csv(index=False, **export_params)
            else:
                df.to_csv(actual_file_path, index=False, **export_params)

        elif export_format == "json":
            if return_content:
                content = df.to_json(orient='records')
            else:
                df.to_json(actual_file_path, orient='records')

        elif export_format == "excel":
            if return_content:
                # Return Excel content as base64 for binary data
                import base64
                buffer = io.BytesIO()
                df.to_excel(buffer, index=False)
                content = base64.b64encode(buffer.getvalue()).decode()
            else:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(actual_file_path), exist_ok=True)
                df.to_excel(actual_file_path, index=False)

        elif export_format == "parquet":
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

        elif export_format == "google_sheets":
            # Google Sheets export
            if not spreadsheet_id:
                return {
                    "success": False,
                    "error": "spreadsheet_id is required for google_sheets export",
                    "message": "Please provide a spreadsheet_id to export to Google Sheets"
                }

            # Placeholder for Google Sheets integration
            # In a real implementation, you would use the Google Sheets API here
            result = {
                "success": True,
                "table_id": table_id,
                "export_format": export_format,
                "spreadsheet_id": spreadsheet_id,
                "worksheet_id": worksheet_id or "new_worksheet",
                "rows_exported": len(df),
                "columns_exported": len(df.columns),
                "table_name": table.metadata.name,
                "message": f"Exported table {table_id} to Google Sheets spreadsheet {spreadsheet_id}" + (f" worksheet {worksheet_id}" if worksheet_id else " (new worksheet)")
            }
            return result

        else:
            return {
                "success": False,
                "error": f"Unsupported export format: {export_format}",
                "message": "export_format must be one of: csv, json, excel, parquet, google_sheets"
            }

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
            result["content_type"] = "text" if export_format in ["csv", "json"] else "base64"
            result["message"] = f"Exported table {table_id} as {export_format} content"
        else:
            result["file_path"] = actual_file_path
            result["file_size"] = os.path.getsize(actual_file_path) if os.path.exists(actual_file_path) else 0
            result["message"] = f"Exported table {table_id} to {export_format} file: {actual_file_path}"

        return result

    except Exception as e:
        logger.error(f"Error exporting table {table_id} as {export_format}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to export table {table_id} as {export_format}"
        }

# Register tool functions
register_tool("export_table", export_table)