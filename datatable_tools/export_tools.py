from typing import Dict, List, Optional, Any
import logging
import io
import os
import pandas as pd
from pathlib import Path
from core.server import register_tool
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

async def save_table(
    table_id: str,
    destination_type: str,
    destination_path: str,
    sheet_name: Optional[str] = None,
    if_exists: str = "replace",
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Save table to external destinations (spreadsheet/excel/database).

    Args:
        table_id: ID of the table to save
        destination_type: Type of destination ("google_sheets", "excel", "csv", "database")
        destination_path: Path/URL to save to (file path, spreadsheet ID, database connection string)
        sheet_name: Sheet name for Excel/Google Sheets (optional)
        if_exists: How to handle existing files/sheets ("replace", "append", "error")
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)

    Returns:
        Dict containing save operation status and information
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

        # Prepare save parameters
        save_params = {}
        if encoding:
            save_params['encoding'] = encoding
        if delimiter:
            save_params['delimiter'] = delimiter

        # Save based on destination type
        if destination_type == "csv":
            df.to_csv(destination_path, index=False, **save_params)

        elif destination_type == "excel":
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            if sheet_name:
                # Handle existing Excel file with multiple sheets
                if os.path.exists(destination_path) and if_exists != "replace":
                    with pd.ExcelWriter(destination_path, mode='a', if_sheet_exists=if_exists) as writer:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    with pd.ExcelWriter(destination_path, mode='w') as writer:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                df.to_excel(destination_path, index=False)

        elif destination_type == "google_sheets":
            # Placeholder for Google Sheets integration
            return {
                "success": False,
                "error": "Google Sheets integration not yet implemented",
                "message": "Google Sheets save functionality is under development"
            }

        elif destination_type == "database":
            # Placeholder for database integration
            return {
                "success": False,
                "error": "Database integration not yet implemented",
                "message": "Database save functionality is under development"
            }

        else:
            return {
                "success": False,
                "error": f"Unsupported destination type: {destination_type}",
                "message": "destination_type must be one of: csv, excel, google_sheets, database"
            }

        return {
            "success": True,
            "table_id": table_id,
            "destination_type": destination_type,
            "destination_path": destination_path,
            "sheet_name": sheet_name,
            "rows_saved": len(df),
            "columns_saved": len(df.columns),
            "if_exists": if_exists,
            "message": f"Successfully saved table {table_id} to {destination_type} at {destination_path}"
        }

    except Exception as e:
        logger.error(f"Error saving table {table_id} to {destination_type}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to save table {table_id} to {destination_type}"
        }

async def export_table(
    table_id: str,
    export_format: str,
    file_path: Optional[str] = None,
    return_content: bool = False,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export table to multiple formats (CSV, JSON, Excel, Parquet).

    Args:
        table_id: ID of the table to export
        export_format: Export format ("csv", "json", "excel", "parquet")
        file_path: Optional file path to save to (if None and return_content=False, generates temp file)
        return_content: If True, returns content in response instead of saving to file
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)

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

        else:
            return {
                "success": False,
                "error": f"Unsupported export format: {export_format}",
                "message": "export_format must be one of: csv, json, excel, parquet"
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