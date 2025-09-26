"""
Range Service for DataTable MCP
Simplified implementation based on fastestai/tools/sheet/range/service.py
"""
import logging
from typing import List, Union, Optional

from datatable_tools.third_party.spreadsheet.spreadsheet_models import (
    UpdateRangeRequest,
    UpdateRangeResponse,
    WorkSheetInfo
)
from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

logger = logging.getLogger(__name__)


class RangeService:
    """Range service that provides range-based operations on spreadsheets"""

    def __init__(self):
        self.google_sheets_service = GoogleSheetsService()

    async def update_range(self, spreadsheet_id: str, worksheet: Union[str, WorkSheetInfo],
                          range_str: str, values: List[List[str]]) -> bool:
        """
        Update a specific range in the spreadsheet.

        Args:
            spreadsheet_id: The spreadsheet ID
            worksheet: Worksheet name or info object
            range_str: Range in A1 notation (e.g., 'A1:C5')
            values: 2D array of values to write

        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract worksheet name
            worksheet_name = worksheet.name if isinstance(worksheet, WorkSheetInfo) else str(worksheet)

            # Create full range notation (e.g., "Sheet1!A1:C5")
            if '!' not in range_str:
                full_range = f"{worksheet_name}!{range_str}"
            else:
                full_range = range_str

            # For now, we'll use a simplified approach by reading the entire sheet,
            # updating the specific range, and writing it back
            # This could be optimized in the future to use proper range updates

            # Get current sheet data
            current_data = await self.google_sheets_service.read_sheet(
                user_id="system",  # This should be passed properly
                spreadsheet_id=spreadsheet_id,
                sheet_name=worksheet_name
            )

            # Parse range to determine position
            range_parts = range_str.replace(f"{worksheet_name}!", "").split(":")
            start_cell = range_parts[0]
            end_cell = range_parts[1] if len(range_parts) > 1 else start_cell

            # Simple range parsing (this could be enhanced)
            start_row, start_col = self._parse_cell_reference(start_cell)

            # Update the data at the specified range
            updated_data = current_data[:]  # Copy current data

            # Ensure we have enough rows
            while len(updated_data) < start_row + len(values):
                updated_data.append([""] * (len(updated_data[0]) if updated_data else 1))

            # Update the specific range
            for i, row in enumerate(values):
                target_row = start_row + i
                if target_row < len(updated_data):
                    # Ensure row has enough columns
                    while len(updated_data[target_row]) < start_col + len(row):
                        updated_data[target_row].append("")

                    # Update the cells
                    for j, cell_value in enumerate(row):
                        updated_data[target_row][start_col + j] = str(cell_value)

            # Write the updated data back
            success = await self.google_sheets_service.write_sheet(
                user_id="system",  # This should be passed properly
                spreadsheet_id=spreadsheet_id,
                data=updated_data,
                sheet_name=worksheet_name
            )

            return success

        except Exception as e:
            logger.error(f"Error updating range {range_str} in spreadsheet {spreadsheet_id}: {e}")
            return False

    async def get_range_values(self, spreadsheet_id: str, worksheet: Union[str, WorkSheetInfo],
                              range_str: str) -> List[List[str]]:
        """
        Get values from a specific range.

        Args:
            spreadsheet_id: The spreadsheet ID
            worksheet: Worksheet name or info object
            range_str: Range in A1 notation

        Returns:
            2D array of cell values
        """
        try:
            # Extract worksheet name
            worksheet_name = worksheet.name if isinstance(worksheet, WorkSheetInfo) else str(worksheet)

            # For now, read the entire sheet and extract the range
            # This could be optimized to use proper range reading
            all_data = await self.google_sheets_service.read_sheet(
                user_id="system",  # This should be passed properly
                spreadsheet_id=spreadsheet_id,
                sheet_name=worksheet_name
            )

            # Parse and extract the specific range
            range_parts = range_str.replace(f"{worksheet_name}!", "").split(":")
            start_cell = range_parts[0]
            end_cell = range_parts[1] if len(range_parts) > 1 else start_cell

            start_row, start_col = self._parse_cell_reference(start_cell)
            end_row, end_col = self._parse_cell_reference(end_cell)

            # Extract the range data
            result = []
            for row_idx in range(start_row, min(end_row + 1, len(all_data))):
                row_data = []
                for col_idx in range(start_col, end_col + 1):
                    if row_idx < len(all_data) and col_idx < len(all_data[row_idx]):
                        row_data.append(all_data[row_idx][col_idx])
                    else:
                        row_data.append("")
                result.append(row_data)

            return result

        except Exception as e:
            logger.error(f"Error getting range {range_str} from spreadsheet {spreadsheet_id}: {e}")
            return []

    async def get_used_range(self, spreadsheet_id: str, worksheet: Union[str, WorkSheetInfo]) -> tuple:
        """
        Get the used range of the worksheet.

        Returns:
            Tuple of (last_row, last_col) indices (0-based)
        """
        try:
            worksheet_name = worksheet.name if isinstance(worksheet, WorkSheetInfo) else str(worksheet)

            all_data = await self.google_sheets_service.read_sheet(
                user_id="system",  # This should be passed properly
                spreadsheet_id=spreadsheet_id,
                sheet_name=worksheet_name
            )

            if not all_data:
                return (0, 0)

            # Find last row and column with data
            last_row = 0
            last_col = 0

            for row_idx, row in enumerate(all_data):
                for col_idx, cell in enumerate(row):
                    if cell and str(cell).strip():
                        last_row = max(last_row, row_idx)
                        last_col = max(last_col, col_idx)

            return (last_row, last_col)

        except Exception as e:
            logger.error(f"Error getting used range for spreadsheet {spreadsheet_id}: {e}")
            return (0, 0)

    async def clear_range(self, spreadsheet_id: str, worksheet: Union[str, WorkSheetInfo],
                         range_str: str) -> bool:
        """
        Clear values in a specific range.

        Args:
            spreadsheet_id: The spreadsheet ID
            worksheet: Worksheet name or info object
            range_str: Range in A1 notation

        Returns:
            True if successful, False otherwise
        """
        try:
            # Parse range to determine dimensions
            range_parts = range_str.split(":")
            start_cell = range_parts[0]
            end_cell = range_parts[1] if len(range_parts) > 1 else start_cell

            start_row, start_col = self._parse_cell_reference(start_cell)
            end_row, end_col = self._parse_cell_reference(end_cell)

            # Create empty values for the range
            empty_values = []
            for _ in range(end_row - start_row + 1):
                empty_values.append([""] * (end_col - start_col + 1))

            # Update the range with empty values
            return await self.update_range(spreadsheet_id, worksheet, range_str, empty_values)

        except Exception as e:
            logger.error(f"Error clearing range {range_str} in spreadsheet {spreadsheet_id}: {e}")
            return False

    def _parse_cell_reference(self, cell_ref: str) -> tuple:
        """
        Parse a cell reference like 'A1' into (row, col) indices (0-based).

        Args:
            cell_ref: Cell reference like 'A1', 'B5', 'AA10'

        Returns:
            Tuple of (row_index, col_index) both 0-based
        """
        import re

        match = re.match(r'([A-Z]+)(\d+)', cell_ref.upper())
        if not match:
            raise ValueError(f"Invalid cell reference: {cell_ref}")

        col_letters = match.group(1)
        row_number = int(match.group(2))

        # Convert column letters to index (0-based)
        col_index = 0
        for char in col_letters:
            col_index = col_index * 26 + (ord(char) - ord('A') + 1)
        col_index -= 1  # Convert to 0-based

        # Convert row number to index (0-based)
        row_index = row_number - 1

        return (row_index, col_index)