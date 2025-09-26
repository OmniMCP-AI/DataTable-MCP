import os
import logging
from typing import Dict, Any
from .spreadsheet_models import (
    ReadSheetRequest,
    ReadSheetResponse,
    WriteSheetRequest,
    WriteSheetResponse,
    UpdateRangeRequest,
    UpdateRangeResponse,
    WorkSheetInfo
)
from .api import LocalSpreadsheetAPI
from ..google_sheets.service import GoogleSheetsService

logger = logging.getLogger(__name__)


class SpreadsheetClient:
    """Client that can use either Google Sheets API or local spreadsheet operations"""

    def __init__(self):
        self.local_api = LocalSpreadsheetAPI()
        self.google_sheets_service = GoogleSheetsService()

    async def read_sheet(self, request: ReadSheetRequest, user_id: str) -> ReadSheetResponse:
        """
        Read data from Google Sheets or local spreadsheet file

        Args:
            request: ReadSheetRequest containing spreadsheet and worksheet info
            user_id: User ID for tracking

        Returns:
            ReadSheetResponse with spreadsheet data
        """
        try:
            # Try Google Sheets first if spreadsheet_id is provided
            if request.spreadsheet_id and user_id:
                try:
                    # Use Google Sheets service
                    data = await self.google_sheets_service.read_sheet(
                        user_id=user_id,
                        spreadsheet_id=request.spreadsheet_id,
                        sheet_name=request.worksheet if hasattr(request, 'worksheet') and request.worksheet else None
                    )

                    # Return success response with Google Sheets data
                    headers = data[0] if data else []
                    rows = data[1:] if len(data) > 1 else []

                    # Combine headers and data for the 'values' field (as expected by data_sources.py)
                    all_values = [headers] + rows if headers else rows

                    return ReadSheetResponse(
                        success=True,
                        message=f"Successfully read data from Google Sheets",
                        spreadsheet_id=request.spreadsheet_id,
                        worksheet=WorkSheetInfo(name=request.worksheet if hasattr(request, 'worksheet') and request.worksheet else "Sheet1"),
                        headers=headers,
                        values=all_values,
                        used_range=f"A1:{chr(65+len(headers)-1)}{len(all_values)}" if headers and all_values else "A1:A1",
                        row_count=len(rows),
                        column_count=len(headers),
                        worksheet_url=f"https://docs.google.com/spreadsheets/d/{request.spreadsheet_id}/edit"
                    )
                except Exception as google_error:
                    logger.warning(f"Google Sheets read failed, falling back to local: {google_error}")
                    # Fall through to local API

            # Fallback to local spreadsheet
            return await self.local_api.read_sheet(request, user_id)
        except Exception as e:
            logger.error(f"Error reading spreadsheet: {e}")
            raise

    async def write_sheet(self, request: WriteSheetRequest, user_id: str) -> WriteSheetResponse:
        """
        Write data to Google Sheets or local spreadsheet file

        Args:
            request: WriteSheetRequest containing data to write
            user_id: User ID for tracking

        Returns:
            WriteSheetResponse with write operation results
        """
        try:
            # Try Google Sheets first if spreadsheet_id is provided
            if request.spreadsheet_id and user_id:
                try:
                    # Prepare data for Google Sheets
                    data_rows = []
                    if hasattr(request, 'headers') and request.headers:
                        data_rows.append(request.headers)
                    if hasattr(request, 'data') and request.data:
                        data_rows.extend(request.data)

                    # Use Google Sheets service
                    await self.google_sheets_service.write_sheet(
                        user_id=user_id,
                        spreadsheet_id=request.spreadsheet_id,
                        data=data_rows,
                        sheet_name=request.worksheet if hasattr(request, 'worksheet') and request.worksheet else request.spreadsheet_name
                    )

                    # Return success response with Google Sheets URL
                    return WriteSheetResponse(
                        success=True,
                        message=f"Successfully wrote data to Google Sheets",
                        spreadsheet_id=request.spreadsheet_id,
                        worksheet=WorkSheetInfo(name=request.worksheet if hasattr(request, 'worksheet') and request.worksheet else request.spreadsheet_name or "Sheet1"),
                        rows_written=len(data_rows),
                        worksheet_url=f"https://docs.google.com/spreadsheets/d/{request.spreadsheet_id}/edit"
                    )
                except Exception as google_error:
                    logger.warning(f"Google Sheets write failed, falling back to local: {google_error}")
                    # Fall through to local API

            # Fallback to local spreadsheet
            return await self.local_api.write_sheet(request, user_id)
        except Exception as e:
            logger.error(f"Error writing to spreadsheet: {e}")
            raise

    async def update_range(self, request: UpdateRangeRequest, user_id: str) -> UpdateRangeResponse:
        """
        Update a specific range in Google Sheets or local spreadsheet

        Args:
            request: UpdateRangeRequest containing range and data to update
            user_id: User ID for tracking

        Returns:
            UpdateRangeResponse with update operation results
        """
        try:
            # Try Google Sheets first if spreadsheet_id is provided
            if request.spreadsheet_id and user_id:
                try:
                    # Prepare range notation for Google Sheets
                    worksheet_name = request.worksheet if hasattr(request, 'worksheet') and request.worksheet else "Sheet1"
                    if isinstance(worksheet_name, WorkSheetInfo):
                        worksheet_name = worksheet_name.name
                    range_notation = f"{worksheet_name}!{request.range}"

                    # Use Google Sheets service
                    success = await self.google_sheets_service.update_range(
                        user_id=user_id,
                        spreadsheet_id=request.spreadsheet_id,
                        range_notation=range_notation,
                        values=request.values
                    )

                    if success:
                        total_cells = sum(len(row) for row in request.values)
                        return UpdateRangeResponse(
                            success=True,
                            message=f"Successfully updated range {request.range} in Google Sheets",
                            spreadsheet_id=request.spreadsheet_id,
                            worksheet=WorkSheetInfo(name=worksheet_name),
                            updated_range=request.range,
                            updated_cells=total_cells,
                            worksheet_url=f"https://docs.google.com/spreadsheets/d/{request.spreadsheet_id}/edit"
                        )
                    else:
                        raise ValueError("Google Sheets update operation failed")
                except Exception as google_error:
                    logger.warning(f"Google Sheets update failed, falling back to local: {google_error}")
                    # Fall through to local API

            # Fallback to local spreadsheet
            return await self.local_api.update_range(request, user_id)
        except Exception as e:
            logger.error(f"Error updating range in spreadsheet: {e}")
            raise


# Global instance
spreadsheet_client = SpreadsheetClient()