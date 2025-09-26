"""
Worksheet Service for DataTable MCP
Simplified implementation based on fastestai/tools/sheet/worksheet/service.py
"""
import logging
from typing import List, Tuple, Union, Optional

from datatable_tools.third_party.spreadsheet.spreadsheet_models import (
    ReadSheetRequest,
    ReadSheetResponse,
    WriteSheetRequest,
    WriteSheetResponse,
    WorkSheetInfo
)
from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

logger = logging.getLogger(__name__)


class WorksheetService:
    """Worksheet service that provides high-level read/write operations"""

    def __init__(self):
        self.google_sheets_service = GoogleSheetsService()

    async def read_sheet(self, request: ReadSheetRequest, user_id: str) -> ReadSheetResponse:
        """
        High-level read operation that reads worksheet content and returns structured data.
        """
        try:
            # Use the Google Sheets service to read data
            data = await self.google_sheets_service.read_sheet(
                user_id=user_id,
                spreadsheet_id=request.spreadsheet_id,
                sheet_name=request.worksheet if isinstance(request.worksheet, str) else None
            )

            if not data:
                return ReadSheetResponse(
                    success=True,
                    message="No data found in worksheet",
                    spreadsheet_id=request.spreadsheet_id,
                    worksheet=WorkSheetInfo(name=request.worksheet if isinstance(request.worksheet, str) else "Sheet1"),
                    used_range="A1:A1",
                    values=[],
                    headers=None,
                    row_count=0,
                    column_count=0,
                    worksheet_url=f"https://docs.google.com/spreadsheets/d/{request.spreadsheet_id}/edit"
                )

            # Process the data
            headers = data[0] if data else []
            rows = data[1:] if len(data) > 1 else []

            # Calculate used range
            if data and headers:
                last_col = len(headers)
                last_row = len(data)
                end_col_letter = self._column_index_to_letter(last_col - 1)
                used_range = f"A1:{end_col_letter}{last_row}"
            else:
                used_range = "A1:A1"

            return ReadSheetResponse(
                success=True,
                message=f"Successfully read {len(rows)} rows and {len(headers)} columns",
                spreadsheet_id=request.spreadsheet_id,
                worksheet=WorkSheetInfo(name=request.worksheet if isinstance(request.worksheet, str) else "Sheet1"),
                used_range=used_range,
                values=data,  # Include all data including headers
                headers=headers,
                row_count=len(rows),
                column_count=len(headers),
                worksheet_url=f"https://docs.google.com/spreadsheets/d/{request.spreadsheet_id}/edit"
            )

        except Exception as e:
            logger.error(f"Error reading sheet: {e}")
            raise

    async def write_sheet(self, request: WriteSheetRequest, user_id: str) -> WriteSheetResponse:
        """
        High-level write operation for updating Google Sheets.
        """
        try:
            # Prepare data for writing
            data_rows = []

            # Add headers if provided
            if hasattr(request, 'headers') and request.headers:
                data_rows.append(request.headers)

            # Add data values
            if hasattr(request, 'values') and request.values:
                data_rows.extend(request.values)
            elif hasattr(request, 'data') and request.data:
                data_rows.extend(request.data)

            # Use the Google Sheets service to write data
            success = await self.google_sheets_service.write_sheet(
                user_id=user_id,
                spreadsheet_id=request.spreadsheet_id,
                data=data_rows,
                sheet_name=request.worksheet if isinstance(request.worksheet, str) else request.spreadsheet_name
            )

            if success:
                total_cells = sum(len(row) for row in data_rows)
                updated_range = f"A1:{self._column_index_to_letter(len(data_rows[0])-1)}{len(data_rows)}" if data_rows and data_rows[0] else "A1:A1"

                return WriteSheetResponse(
                    success=True,
                    message=f"Successfully updated {len(data_rows)} rows",
                    spreadsheet_id=request.spreadsheet_id,
                    worksheet=WorkSheetInfo(name=request.worksheet if isinstance(request.worksheet, str) else request.spreadsheet_name or "Sheet1"),
                    updated_range=updated_range,
                    updated_cells=total_cells,
                    matched_columns=None,
                    worksheet_url=f"https://docs.google.com/spreadsheets/d/{request.spreadsheet_id}/edit"
                )
            else:
                raise ValueError("Write operation failed")

        except Exception as e:
            logger.error(f"Error writing sheet: {e}")
            raise

    def _column_index_to_letter(self, index: int) -> str:
        """Convert column index (0-based) to letter (A, B, C, ..., AA, AB, ...)"""
        result = ""
        while index >= 0:
            result = chr(index % 26 + ord('A')) + result
            index = index // 26 - 1
        return result