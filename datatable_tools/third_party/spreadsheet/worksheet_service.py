import logging
from typing import List, Tuple, Union
import openpyxl
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from pathlib import Path

from .spreadsheet_models import (
    ReadSheetRequest,
    ReadSheetResponse,
    WriteSheetRequest,
    WriteSheetResponse,
    WorkSheetInfo
)

# Import our new Google Sheets integration services
try:
    from ..worksheet.service import WorksheetService as GoogleWorksheetService
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False


class LocalWorksheetService:
    """
    Hybrid worksheet service that uses Google Sheets when possible,
    falls back to local Excel files when needed
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.temp_dir = Path("/tmp/datatable_spreadsheets")
        self.temp_dir.mkdir(exist_ok=True)

        # Initialize Google Sheets service if available
        if GOOGLE_SHEETS_AVAILABLE:
            self.google_service = GoogleWorksheetService()
        else:
            self.google_service = None

    def _get_workbook_path(self, spreadsheet_id: str) -> Path:
        """Get the file path for a spreadsheet based on its ID"""
        return self.temp_dir / f"{spreadsheet_id}.xlsx"

    def _load_or_create_workbook(self, spreadsheet_id: str) -> Tuple[Workbook, Path]:
        """Load existing workbook or create new one"""
        file_path = self._get_workbook_path(spreadsheet_id)

        if file_path.exists():
            return openpyxl.load_workbook(file_path), file_path
        else:
            workbook = Workbook()
            return workbook, file_path

    def _get_worksheet(self, workbook: Workbook, worksheet_info: WorkSheetInfo):
        """Get worksheet from workbook based on WorkSheetInfo"""
        if worksheet_info.name:
            if worksheet_info.name in workbook.sheetnames:
                return workbook[worksheet_info.name]
            else:
                # Create new worksheet with the specified name
                return workbook.create_sheet(worksheet_info.name)
        else:
            # Use first sheet if no name specified
            if workbook.worksheets:
                return workbook.worksheets[0]
            else:
                return workbook.create_sheet("Sheet1")

    def _normalize_worksheet_info(self, worksheet: Union[str, int, WorkSheetInfo, None]) -> WorkSheetInfo:
        """Convert different worksheet parameter types to WorkSheetInfo"""
        if worksheet is None:
            return WorkSheetInfo(name="Sheet1")
        elif isinstance(worksheet, WorkSheetInfo):
            return worksheet
        elif isinstance(worksheet, str):
            return WorkSheetInfo(name=worksheet)
        elif isinstance(worksheet, int):
            return WorkSheetInfo(name=f"Sheet{worksheet}")
        else:
            raise ValueError(f"Invalid worksheet parameter type: {type(worksheet)}")

    def _column_index_to_letter(self, col_idx: int) -> str:
        """Convert 0-based column index to Excel column letter"""
        return get_column_letter(col_idx + 1)

    def _find_data_boundaries(self, worksheet) -> Tuple[int, int]:
        """Find the last row and column with data"""
        last_row = 0
        last_col = 0

        for row in worksheet.iter_rows():
            for cell in row:
                if cell.value is not None and str(cell.value).strip():
                    last_row = max(last_row, cell.row)
                    last_col = max(last_col, cell.column)

        return last_row, last_col

    async def read_sheet(
        self, request: ReadSheetRequest, user_id: str
    ) -> ReadSheetResponse:
        """
        Read worksheet data and return structured response
        """
        try:
            workbook, file_path = self._load_or_create_workbook(request.spreadsheet_id)
            worksheet_info = self._normalize_worksheet_info(request.worksheet)
            worksheet = self._get_worksheet(workbook, worksheet_info)

            # Find data boundaries
            last_row, last_col = self._find_data_boundaries(worksheet)

            if last_row == 0 or last_col == 0:
                return ReadSheetResponse(
                    success=True,
                    message="No data found in worksheet",
                    spreadsheet_id=request.spreadsheet_id,
                    worksheet=WorkSheetInfo(name=worksheet.title),
                    used_range="A1:A1",
                    values=[],
                    headers=None,
                    row_count=0,
                    column_count=0,
                    worksheet_url=f"file://{file_path}"
                )

            # Extract values
            values = []
            for row in worksheet.iter_rows(min_row=1, max_row=last_row,
                                         min_col=1, max_col=last_col, values_only=True):
                row_values = [str(cell) if cell is not None else "" for cell in row]
                values.append(row_values)

            # Detect headers (first row if it looks like headers)
            headers = None
            if values and len(values) > 1:
                first_row = values[0]
                if all(isinstance(cell, str) and cell.strip() for cell in first_row):
                    headers = first_row

            end_col_letter = get_column_letter(last_col)
            used_range = f"A1:{end_col_letter}{last_row}"

            return ReadSheetResponse(
                success=True,
                message=f"Successfully read {last_row} rows and {last_col} columns",
                spreadsheet_id=request.spreadsheet_id,
                worksheet=WorkSheetInfo(name=worksheet.title),
                used_range=used_range,
                values=values,
                headers=headers,
                row_count=last_row,
                column_count=last_col,
                worksheet_url=f"file://{file_path}"
            )

        except Exception as e:
            self.logger.error(f"Error reading sheet: {e}")
            raise ValueError(f"Error reading sheet: {e}")

    async def write_sheet(
        self, request: WriteSheetRequest, user_id: str
    ) -> WriteSheetResponse:
        """
        Write data to worksheet - try Google Sheets first, fallback to local
        """
        # Try Google Sheets first if available and we have valid spreadsheet_id and user_id
        if (self.google_service and
            request.spreadsheet_id and
            request.spreadsheet_id.strip() and
            user_id and user_id.strip()):
            try:
                self.logger.info(f"Attempting Google Sheets write for spreadsheet {request.spreadsheet_id}")
                return await self.google_service.write_sheet(request, user_id)
            except Exception as google_error:
                self.logger.warning(f"Google Sheets write failed, falling back to local: {google_error}")
                # Fall through to local implementation

        # Local implementation (fallback)
        self.logger.info(f"Using local write for spreadsheet {request.spreadsheet_id or 'new'}")
        try:
            # Create new spreadsheet if needed
            if not request.spreadsheet_id or request.spreadsheet_id.strip() == "":
                import uuid
                spreadsheet_id = str(uuid.uuid4())
            else:
                spreadsheet_id = request.spreadsheet_id

            workbook, file_path = self._load_or_create_workbook(spreadsheet_id)
            worksheet_info = self._normalize_worksheet_info(request.worksheet)
            worksheet = self._get_worksheet(workbook, worksheet_info)

            # Determine start position
            start_row = request.start_row or 1
            start_col = 1

            if request.columns_name:
                # Find columns by header names
                start_row, matched_columns = await self._find_columns_by_names(
                    worksheet, request.columns_name
                )
            else:
                matched_columns = None

            # Write data
            total_cells = 0
            for row_idx, row_data in enumerate(request.values):
                for col_idx, cell_value in enumerate(row_data):
                    cell = worksheet.cell(
                        row=start_row + row_idx,
                        column=start_col + col_idx,
                        value=str(cell_value) if cell_value is not None else ""
                    )
                    total_cells += 1

            # Calculate updated range
            end_row = start_row + len(request.values) - 1
            end_col = start_col + max(len(row) for row in request.values) - 1
            end_col_letter = get_column_letter(end_col)
            updated_range = f"{get_column_letter(start_col)}{start_row}:{end_col_letter}{end_row}"

            # Save workbook
            workbook.save(file_path)

            return WriteSheetResponse(
                success=True,
                message=f"Successfully updated {len(request.values)} rows",
                spreadsheet_id=spreadsheet_id,
                worksheet=WorkSheetInfo(name=worksheet.title),
                updated_range=updated_range,
                updated_cells=total_cells,
                matched_columns=matched_columns,
                worksheet_url=f"file://{file_path}"
            )

        except Exception as e:
            self.logger.error(f"Error writing to local worksheet: {e}")
            raise

    async def _find_columns_by_names(
        self, worksheet, column_names: List[str]
    ) -> Tuple[int, List[str]]:
        """
        Find column positions by matching header names
        Returns start_row and matched column names
        """
        # Check first 3 rows for headers
        header_row = None
        header_row_num = 1

        for row_num in range(1, 4):
            row_values = []
            for col_num in range(1, worksheet.max_column + 1):
                cell_value = worksheet.cell(row_num, col_num).value
                row_values.append(str(cell_value) if cell_value else "")

            # Check if any column names match this row
            if any(col_name.lower().strip() in cell_value.lower().strip()
                  for cell_value in row_values for col_name in column_names):
                header_row = row_values
                header_row_num = row_num
                break

        if not header_row:
            return 2, column_names  # Start at row 2, return original names

        # Find exact matches
        matched_columns = []
        for col_name in column_names:
            for cell_value in header_row:
                if cell_value.lower().strip() == col_name.lower().strip():
                    matched_columns.append(col_name)
                    break

        return header_row_num + 1, matched_columns or column_names