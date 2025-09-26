import logging
from typing import List, Union

from .spreadsheet_models import (
    ReadSheetRequest,
    ReadSheetResponse,
    WriteSheetRequest,
    WriteSheetResponse,
    UpdateRangeRequest,
    UpdateRangeResponse,
    WorkSheetInfo
)
from .worksheet_service import LocalWorksheetService
from .range_service import RangeService


class LocalSpreadsheetAPI:
    """
    Local implementation of spreadsheet API functionality
    Replaces external Google Sheets API calls with local file operations
    """

    def __init__(self):
        self.worksheet_service = LocalWorksheetService()
        self.range_service = RangeService()
        self.logger = logging.getLogger(__name__)

    async def read_sheet(self, request: ReadSheetRequest, user_id: str) -> ReadSheetResponse:
        """
        High-level operation that reads worksheet content and returns structured data.
        """
        try:
            return await self.worksheet_service.read_sheet(request, user_id)
        except Exception as e:
            self.logger.error(f"Error in read_sheet: {e}")
            raise

    async def write_sheet(self, request: WriteSheetRequest, user_id: str) -> WriteSheetResponse:
        """
        High-level operation for writing data using column names or position-based approach.
        """
        try:
            return await self.worksheet_service.write_sheet(request, user_id)
        except Exception as e:
            self.logger.error(f"Error in write_sheet: {e}")
            raise

    async def update_range(self, request: UpdateRangeRequest, user_id: str) -> UpdateRangeResponse:
        """
        Update a specific range in the spreadsheet
        """
        try:
            success = await self.range_service.update_range(
                request.spreadsheet_id,
                request.worksheet if isinstance(request.worksheet, WorkSheetInfo) else WorkSheetInfo(name=str(request.worksheet)),
                request.range,
                request.values
            )

            if success:
                total_cells = sum(len(row) for row in request.values)
                return UpdateRangeResponse(
                    success=True,
                    message=f"Successfully updated range {request.range}",
                    spreadsheet_id=request.spreadsheet_id,
                    worksheet=WorkSheetInfo(name=str(request.worksheet)),
                    updated_range=request.range,
                    updated_cells=total_cells,
                    worksheet_url=f"file:///tmp/datatable_spreadsheets/{request.spreadsheet_id}.xlsx"
                )
            else:
                raise ValueError("Update operation failed")

        except Exception as e:
            self.logger.error(f"Error in update_range: {e}")
            raise

    async def get_range_values(self, spreadsheet_id: str, worksheet: Union[str, WorkSheetInfo],
                              range_str: str, user_id: str) -> List[List[str]]:
        """
        Get values from a specific range
        """
        try:
            worksheet_info = worksheet if isinstance(worksheet, WorkSheetInfo) else WorkSheetInfo(name=str(worksheet))
            return await self.range_service.get_range_values(spreadsheet_id, worksheet_info, range_str)
        except Exception as e:
            self.logger.error(f"Error in get_range_values: {e}")
            raise

    async def get_used_range(self, spreadsheet_id: str, worksheet: Union[str, WorkSheetInfo],
                           user_id: str) -> tuple:
        """
        Get the used range of the worksheet
        """
        try:
            worksheet_info = worksheet if isinstance(worksheet, WorkSheetInfo) else WorkSheetInfo(name=str(worksheet))
            return await self.range_service.get_used_range(spreadsheet_id, worksheet_info)
        except Exception as e:
            self.logger.error(f"Error in get_used_range: {e}")
            raise

    async def clear_range(self, spreadsheet_id: str, worksheet: Union[str, WorkSheetInfo],
                         range_str: str, user_id: str) -> bool:
        """
        Clear values in a specific range
        """
        try:
            worksheet_info = worksheet if isinstance(worksheet, WorkSheetInfo) else WorkSheetInfo(name=str(worksheet))
            return await self.range_service.clear_range(spreadsheet_id, worksheet_info, range_str)
        except Exception as e:
            self.logger.error(f"Error in clear_range: {e}")
            raise