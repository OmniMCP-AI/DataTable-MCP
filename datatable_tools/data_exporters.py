from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import logging
import os
import io
from datatable_tools.spreadsheet_client import spreadsheet_client
from datatable_tools.spreadsheet_models import WriteSheetRequest
from datatable_tools.table_manager import DataTable

logger = logging.getLogger(__name__)


class DataExporter(ABC):
    """Abstract base class for data exporters"""

    @abstractmethod
    async def export_data(
        self,
        table: DataTable,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Export table data to target

        Args:
            table: DataTable to export
            **kwargs: Export-specific parameters

        Returns:
            Dict with export results
        """
        pass


class SpreadsheetExporter(DataExporter):
    """Exporter for Google Spreadsheets via SPREADSHEET_API"""

    def __init__(self, user_id: str):
        self.user_id = user_id

    async def export_data(
        self,
        table: DataTable,
        spreadsheet_id: Optional[str] = None,
        spreadsheet_name: Optional[str] = None,
        worksheet: Optional[str] = None,
        columns_name: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Export data to Google Spreadsheet"""
        try:
            # Prepare data for export - include headers as first row
            export_data = []
            if table.headers:
                export_data.append(table.headers)

            # Add table data
            for row in table.data:
                export_data.append([str(cell) for cell in row])

            # Create request
            request = WriteSheetRequest(
                spreadsheet_id=spreadsheet_id,
                spreadsheet_name=spreadsheet_name or f"Export_{table.metadata.name}",
                worksheet=worksheet,
                columns_name=columns_name,
                values=export_data
            )

            # Call spreadsheet API
            response = await spreadsheet_client.write_sheet(request, self.user_id)

            if not response.success:
                raise Exception(f"Failed to write to spreadsheet: {response.message}")

            return {
                "success": True,
                "export_format": "google_sheets",
                "table_id": table.table_id,
                "spreadsheet_id": response.spreadsheet_id,
                "worksheet": response.worksheet.name,
                "updated_range": response.updated_range,
                "updated_cells": response.updated_cells,
                "matched_columns": response.matched_columns,
                "worksheet_url": response.worksheet_url,
                "rows_exported": len(table.data),
                "columns_exported": len(table.headers),
                "table_name": table.metadata.name,
                "message": f"Exported table {table.table_id} to Google Sheets: {response.message}"
            }

        except Exception as e:
            logger.error(f"Error exporting to spreadsheet: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to export table {table.table_id} to Google Sheets"
            }


class ExcelExporter(DataExporter):
    """Exporter for Excel files"""

    async def export_data(
        self,
        table: DataTable,
        file_path: Optional[str] = None,
        return_content: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """Export data to Excel file"""
        try:
            df = table.df
            actual_file_path = file_path

            # Generate file path if not provided and not returning content
            if not file_path and not return_content:
                import tempfile
                temp_dir = tempfile.gettempdir()
                safe_name = table.metadata.name.replace(" ", "_").replace("/", "_")
                actual_file_path = os.path.join(temp_dir, f"{safe_name}_{table.table_id}.xlsx")

            if return_content:
                # Return Excel content as base64 for binary data
                import base64
                buffer = io.BytesIO()
                df.to_excel(buffer, index=False)
                content = base64.b64encode(buffer.getvalue()).decode()
                return {
                    "success": True,
                    "export_format": "excel",
                    "table_id": table.table_id,
                    "return_content": True,
                    "content": content,
                    "content_type": "base64",
                    "rows_exported": len(df),
                    "columns_exported": len(df.columns),
                    "table_name": table.metadata.name,
                    "message": f"Exported table {table.table_id} as Excel content"
                }
            else:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(actual_file_path), exist_ok=True)
                df.to_excel(actual_file_path, index=False)
                return {
                    "success": True,
                    "export_format": "excel",
                    "table_id": table.table_id,
                    "file_path": actual_file_path,
                    "file_size": os.path.getsize(actual_file_path),
                    "rows_exported": len(df),
                    "columns_exported": len(df.columns),
                    "table_name": table.metadata.name,
                    "message": f"Exported table {table.table_id} to Excel file: {actual_file_path}"
                }

        except Exception as e:
            logger.error(f"Error exporting to Excel: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to export table {table.table_id} to Excel"
            }


class CSVExporter(DataExporter):
    """Exporter for CSV files"""

    async def export_data(
        self,
        table: DataTable,
        file_path: Optional[str] = None,
        return_content: bool = False,
        encoding: Optional[str] = None,
        delimiter: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Export data to CSV file"""
        try:
            df = table.df
            actual_file_path = file_path

            # Set defaults
            encoding = encoding or "utf-8"
            delimiter = delimiter or ","

            # Generate file path if not provided and not returning content
            if not file_path and not return_content:
                import tempfile
                temp_dir = tempfile.gettempdir()
                safe_name = table.metadata.name.replace(" ", "_").replace("/", "_")
                actual_file_path = os.path.join(temp_dir, f"{safe_name}_{table.table_id}.csv")

            export_params = {"index": False, "encoding": encoding, "sep": delimiter}

            if return_content:
                content = df.to_csv(**export_params)
                return {
                    "success": True,
                    "export_format": "csv",
                    "table_id": table.table_id,
                    "return_content": True,
                    "content": content,
                    "content_type": "text",
                    "rows_exported": len(df),
                    "columns_exported": len(df.columns),
                    "table_name": table.metadata.name,
                    "message": f"Exported table {table.table_id} as CSV content"
                }
            else:
                df.to_csv(actual_file_path, **export_params)
                return {
                    "success": True,
                    "export_format": "csv",
                    "table_id": table.table_id,
                    "file_path": actual_file_path,
                    "file_size": os.path.getsize(actual_file_path),
                    "rows_exported": len(df),
                    "columns_exported": len(df.columns),
                    "table_name": table.metadata.name,
                    "message": f"Exported table {table.table_id} to CSV file: {actual_file_path}"
                }

        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to export table {table.table_id} to CSV"
            }


class JSONExporter(DataExporter):
    """Exporter for JSON files"""

    async def export_data(
        self,
        table: DataTable,
        file_path: Optional[str] = None,
        return_content: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """Export data to JSON file"""
        try:
            df = table.df
            actual_file_path = file_path

            # Generate file path if not provided and not returning content
            if not file_path and not return_content:
                import tempfile
                temp_dir = tempfile.gettempdir()
                safe_name = table.metadata.name.replace(" ", "_").replace("/", "_")
                actual_file_path = os.path.join(temp_dir, f"{safe_name}_{table.table_id}.json")

            if return_content:
                content = df.to_json(orient='records')
                return {
                    "success": True,
                    "export_format": "json",
                    "table_id": table.table_id,
                    "return_content": True,
                    "content": content,
                    "content_type": "text",
                    "rows_exported": len(df),
                    "columns_exported": len(df.columns),
                    "table_name": table.metadata.name,
                    "message": f"Exported table {table.table_id} as JSON content"
                }
            else:
                df.to_json(actual_file_path, orient='records')
                return {
                    "success": True,
                    "export_format": "json",
                    "table_id": table.table_id,
                    "file_path": actual_file_path,
                    "file_size": os.path.getsize(actual_file_path),
                    "rows_exported": len(df),
                    "columns_exported": len(df.columns),
                    "table_name": table.metadata.name,
                    "message": f"Exported table {table.table_id} to JSON file: {actual_file_path}"
                }

        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to export table {table.table_id} to JSON"
            }


def create_exporter(export_format: str, **kwargs) -> DataExporter:
    """Factory function to create appropriate exporter"""

    if export_format == "google_sheets":
        return SpreadsheetExporter(user_id=kwargs.get("user_id"))
    elif export_format == "excel":
        return ExcelExporter()
    elif export_format == "csv":
        return CSVExporter()
    elif export_format == "json":
        return JSONExporter()
    else:
        raise ValueError(f"Unsupported export format: {export_format}")