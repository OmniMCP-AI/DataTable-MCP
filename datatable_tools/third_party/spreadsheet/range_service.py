import logging
import openpyxl
from openpyxl import Workbook
from openpyxl.utils import get_column_letter, column_index_from_string
from openpyxl.utils.cell import coordinate_from_string
from typing import List, Union, Tuple
import re
from pathlib import Path

from .spreadsheet_models import WorkSheetInfo


class RangeService:
    """Local implementation of range operations without relying on external API"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.temp_dir = Path("/tmp/datatable_spreadsheets")
        self.temp_dir.mkdir(exist_ok=True)

    def _get_workbook_path(self, spreadsheet_id: str) -> Path:
        """Get the file path for a spreadsheet based on its ID"""
        return self.temp_dir / f"{spreadsheet_id}.xlsx"

    def _load_workbook(self, spreadsheet_id: str) -> Tuple[Workbook, Path]:
        """Load existing workbook"""
        file_path = self._get_workbook_path(spreadsheet_id)
        if not file_path.exists():
            raise FileNotFoundError(f"Spreadsheet {spreadsheet_id} not found")
        workbook = openpyxl.load_workbook(file_path)
        return workbook, file_path

    def _get_worksheet(self, workbook: Workbook, worksheet_info: WorkSheetInfo):
        """Get worksheet from workbook"""
        if worksheet_info.name and worksheet_info.name in workbook.sheetnames:
            return workbook[worksheet_info.name]
        elif workbook.worksheets:
            return workbook.worksheets[0]
        else:
            raise ValueError("No worksheets found")

    def _parse_range(self, range_str: str) -> Tuple[int, int, int, int]:
        """Parse Excel range like 'A1:C10' to (start_row, start_col, end_row, end_col)"""
        if ':' not in range_str:
            # Single cell
            col, row = coordinate_from_string(range_str)
            col_idx = column_index_from_string(col)
            return row, col_idx, row, col_idx

        start_cell, end_cell = range_str.split(':')
        start_col, start_row = coordinate_from_string(start_cell)
        end_col, end_row = coordinate_from_string(end_cell)

        start_col_idx = column_index_from_string(start_col)
        end_col_idx = column_index_from_string(end_col)

        return start_row, start_col_idx, end_row, end_col_idx

    async def get_range_values(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo, range_str: str) -> List[List[str]]:
        """Get values from a specific range"""
        workbook, _ = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        start_row, start_col, end_row, end_col = self._parse_range(range_str)

        values = []
        for row in worksheet.iter_rows(min_row=start_row, max_row=end_row,
                                     min_col=start_col, max_col=end_col, values_only=True):
            row_values = [str(cell) if cell is not None else "" for cell in row]
            values.append(row_values)

        return values

    async def update_range(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo,
                          range_str: str, values: List[List[str]]) -> bool:
        """Update values in a specific range"""
        workbook, file_path = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        start_row, start_col, _, _ = self._parse_range(range_str)

        for row_idx, row_data in enumerate(values):
            for col_idx, cell_value in enumerate(row_data):
                cell = worksheet.cell(
                    row=start_row + row_idx,
                    column=start_col + col_idx,
                    value=str(cell_value) if cell_value is not None else ""
                )

        workbook.save(file_path)
        return True

    async def get_used_range(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo) -> Tuple[str, int, int]:
        """Get the used range of the worksheet"""
        workbook, _ = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        last_row = 0
        last_col = 0

        for row in worksheet.iter_rows():
            for cell in row:
                if cell.value is not None and str(cell.value).strip():
                    last_row = max(last_row, cell.row)
                    last_col = max(last_col, cell.column)

        if last_row == 0 or last_col == 0:
            return "A1:A1", 0, 0

        end_col_letter = get_column_letter(last_col)
        used_range = f"A1:{end_col_letter}{last_row}"

        return used_range, last_row, last_col

    async def clear_range(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo, range_str: str) -> bool:
        """Clear values in a specific range"""
        workbook, file_path = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        start_row, start_col, end_row, end_col = self._parse_range(range_str)

        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                worksheet.cell(row=row, column=col).value = None

        workbook.save(file_path)
        return True

    async def get_entire_row_data(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo, row_number: int) -> List[str]:
        """Get all data from a specific row"""
        workbook, _ = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        row_values = []
        for cell in worksheet[row_number]:
            row_values.append(str(cell.value) if cell.value is not None else "")

        # Remove trailing empty cells
        while row_values and not row_values[-1]:
            row_values.pop()

        return row_values

    async def get_entire_column_data(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo, column: str) -> List[str]:
        """Get all data from a specific column"""
        workbook, _ = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        col_idx = column_index_from_string(column)
        column_values = []

        for row in worksheet.iter_rows(min_col=col_idx, max_col=col_idx):
            cell = row[0]
            column_values.append(str(cell.value) if cell.value is not None else "")

        # Remove trailing empty cells
        while column_values and not column_values[-1]:
            column_values.pop()

        return column_values

    async def insert_rows(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo,
                         start_index: int, count: int) -> bool:
        """Insert empty rows"""
        workbook, file_path = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        worksheet.insert_rows(start_index + 1, count)
        workbook.save(file_path)
        return True

    async def insert_columns(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo,
                           start_index: int, count: int) -> bool:
        """Insert empty columns"""
        workbook, file_path = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        worksheet.insert_cols(start_index + 1, count)
        workbook.save(file_path)
        return True

    async def delete_rows(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo,
                         start_index: int, count: int) -> bool:
        """Delete rows"""
        workbook, file_path = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        worksheet.delete_rows(start_index + 1, count)
        workbook.save(file_path)
        return True

    async def delete_columns(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo,
                           start_index: int, count: int) -> bool:
        """Delete columns"""
        workbook, file_path = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        worksheet.delete_cols(start_index + 1, count)
        workbook.save(file_path)
        return True

    async def find_cells_by_keyword(self, spreadsheet_id: str, worksheet_info: WorkSheetInfo,
                                  keyword: str, search_range: str = None) -> List[Tuple[str, str]]:
        """Find cells containing keyword"""
        workbook, _ = self._load_workbook(spreadsheet_id)
        worksheet = self._get_worksheet(workbook, worksheet_info)

        found_cells = []

        if search_range:
            start_row, start_col, end_row, end_col = self._parse_range(search_range)
            cells_to_search = worksheet.iter_rows(min_row=start_row, max_row=end_row,
                                                min_col=start_col, max_col=end_col)
        else:
            cells_to_search = worksheet.iter_rows()

        for row in cells_to_search:
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    if re.search(keyword, str(cell.value), re.IGNORECASE):
                        cell_address = f"{get_column_letter(cell.column)}{cell.row}"
                        found_cells.append((cell_address, str(cell.value)))

        return found_cells