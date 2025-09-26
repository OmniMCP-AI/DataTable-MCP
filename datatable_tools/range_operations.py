from typing import Dict, List, Optional, Any
import logging
from datatable_tools.spreadsheet_client import spreadsheet_client
from datatable_tools.spreadsheet_models import UpdateRangeRequest, WorkSheetInfo
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)


class DataTableRangeOperations:
    """Detailed spreadsheet operations using /range/update endpoint"""

    @staticmethod
    def _column_index_to_letter(index: int) -> str:
        """Convert column index (0-based) to Excel column letter"""
        result = ""
        while index >= 0:
            result = chr(index % 26 + ord('A')) + result
            index = index // 26 - 1
        return result

    @staticmethod
    def _parse_worksheet_info(worksheet: Any) -> WorkSheetInfo:
        """Convert worksheet parameter to WorkSheetInfo"""
        if isinstance(worksheet, str):
            return WorkSheetInfo(name=worksheet)
        elif isinstance(worksheet, int):
            return WorkSheetInfo(id=worksheet)
        elif isinstance(worksheet, dict):
            return WorkSheetInfo(**worksheet)
        elif hasattr(worksheet, 'name') or hasattr(worksheet, 'id'):
            return worksheet
        else:
            return WorkSheetInfo(name="Sheet1")

    async def update_cell(
        self,
        spreadsheet_id: str,
        worksheet: Any,
        cell_address: str,
        value: Any,
        user_id: str
    ) -> Dict[str, Any]:
        """
        Update a single cell in the spreadsheet

        Args:
            spreadsheet_id: Target spreadsheet ID
            worksheet: Worksheet identifier (name, id, or WorkSheetInfo)
            cell_address: Cell address in A1 notation (e.g., "B5")
            value: Value to set in the cell
            user_id: User ID for authentication

        Returns:
            Dict with operation results
        """
        try:
            request = UpdateRangeRequest(
                spreadsheet_id=spreadsheet_id,
                worksheet=self._parse_worksheet_info(worksheet),
                range=cell_address,
                values=[[str(value)]]
            )

            response = await spreadsheet_client.update_range(request, user_id)

            return {
                "success": response.success,
                "message": response.message,
                "spreadsheet_id": response.spreadsheet_id,
                "worksheet": response.worksheet.name,
                "updated_range": response.updated_range,
                "updated_cells": response.updated_cells,
                "worksheet_url": response.worksheet_url
            }

        except Exception as e:
            logger.error(f"Error updating cell {cell_address}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to update cell {cell_address}"
            }

    async def update_row(
        self,
        spreadsheet_id: str,
        worksheet: Any,
        row_number: int,
        row_data: List[Any],
        user_id: str,
        start_column: str = "A"
    ) -> Dict[str, Any]:
        """
        Update an entire row in the spreadsheet

        Args:
            spreadsheet_id: Target spreadsheet ID
            worksheet: Worksheet identifier
            row_number: Row number (1-based)
            row_data: List of values for the row
            user_id: User ID for authentication
            start_column: Starting column letter (default "A")

        Returns:
            Dict with operation results
        """
        try:
            # Calculate end column based on data length
            start_col_index = ord(start_column.upper()) - ord('A')
            end_col_index = start_col_index + len(row_data) - 1
            end_column = self._column_index_to_letter(end_col_index)

            range_str = f"{start_column}{row_number}:{end_column}{row_number}"

            request = UpdateRangeRequest(
                spreadsheet_id=spreadsheet_id,
                worksheet=self._parse_worksheet_info(worksheet),
                range=range_str,
                values=[[str(val) for val in row_data]]
            )

            response = await spreadsheet_client.update_range(request, user_id)

            return {
                "success": response.success,
                "message": response.message,
                "spreadsheet_id": response.spreadsheet_id,
                "worksheet": response.worksheet.name,
                "updated_range": response.updated_range,
                "updated_cells": response.updated_cells,
                "row_number": row_number,
                "columns_updated": len(row_data),
                "worksheet_url": response.worksheet_url
            }

        except Exception as e:
            logger.error(f"Error updating row {row_number}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to update row {row_number}"
            }

    async def update_column(
        self,
        spreadsheet_id: str,
        worksheet: Any,
        column: str,
        column_data: List[Any],
        user_id: str,
        start_row: int = 1
    ) -> Dict[str, Any]:
        """
        Update an entire column in the spreadsheet

        Args:
            spreadsheet_id: Target spreadsheet ID
            worksheet: Worksheet identifier
            column: Column letter (e.g., "B")
            column_data: List of values for the column
            user_id: User ID for authentication
            start_row: Starting row number (1-based, default 1)

        Returns:
            Dict with operation results
        """
        try:
            end_row = start_row + len(column_data) - 1
            range_str = f"{column.upper()}{start_row}:{column.upper()}{end_row}"

            # Convert column data to 2D array (each value in its own row)
            values = [[str(val)] for val in column_data]

            request = UpdateRangeRequest(
                spreadsheet_id=spreadsheet_id,
                worksheet=self._parse_worksheet_info(worksheet),
                range=range_str,
                values=values
            )

            response = await spreadsheet_client.update_range(request, user_id)

            return {
                "success": response.success,
                "message": response.message,
                "spreadsheet_id": response.spreadsheet_id,
                "worksheet": response.worksheet.name,
                "updated_range": response.updated_range,
                "updated_cells": response.updated_cells,
                "column": column.upper(),
                "rows_updated": len(column_data),
                "worksheet_url": response.worksheet_url
            }

        except Exception as e:
            logger.error(f"Error updating column {column}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to update column {column}"
            }

    async def update_table_range(
        self,
        table_id: str,
        spreadsheet_id: str,
        worksheet: Any,
        start_cell: str,
        user_id: str,
        include_headers: bool = True
    ) -> Dict[str, Any]:
        """
        Update spreadsheet with entire DataTable content using range update

        Args:
            table_id: DataTable ID to export
            spreadsheet_id: Target spreadsheet ID
            worksheet: Worksheet identifier
            start_cell: Starting cell (e.g., "A1")
            user_id: User ID for authentication
            include_headers: Whether to include table headers

        Returns:
            Dict with operation results
        """
        try:
            table = table_manager.get_table(table_id)
            if not table:
                return {
                    "success": False,
                    "error": f"Table {table_id} not found",
                    "message": "Source table does not exist"
                }

            # Prepare data
            export_data = []
            if include_headers and table.headers:
                export_data.append(table.headers)

            # Add table data (convert all values to strings)
            for row in table.data:
                export_data.append([str(cell) for cell in row])

            if not export_data:
                return {
                    "success": False,
                    "error": "No data to export",
                    "message": "Table is empty"
                }

            # Calculate range based on data dimensions
            num_rows = len(export_data)
            num_cols = len(export_data[0])

            # Parse start cell (e.g., "A1" -> column A, row 1)
            import re
            match = re.match(r'([A-Z]+)(\d+)', start_cell.upper())
            if not match:
                raise ValueError(f"Invalid start_cell format: {start_cell}")

            start_col = match.group(1)
            start_row = int(match.group(2))

            # Calculate end position
            start_col_index = sum((ord(c) - ord('A') + 1) * (26 ** i) for i, c in enumerate(reversed(start_col))) - 1
            end_col_index = start_col_index + num_cols - 1
            end_col = self._column_index_to_letter(end_col_index)
            end_row = start_row + num_rows - 1

            range_str = f"{start_cell.upper()}:{end_col}{end_row}"

            request = UpdateRangeRequest(
                spreadsheet_id=spreadsheet_id,
                worksheet=self._parse_worksheet_info(worksheet),
                range=range_str,
                values=export_data
            )

            response = await spreadsheet_client.update_range(request, user_id)

            return {
                "success": response.success,
                "message": response.message,
                "table_id": table_id,
                "spreadsheet_id": response.spreadsheet_id,
                "worksheet": response.worksheet.name,
                "updated_range": response.updated_range,
                "updated_cells": response.updated_cells,
                "rows_exported": len(table.data),
                "columns_exported": len(table.headers) if table.headers else 0,
                "included_headers": include_headers,
                "table_name": table.metadata.name,
                "worksheet_url": response.worksheet_url
            }

        except Exception as e:
            logger.error(f"Error updating table range for table {table_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to update table range for table {table_id}"
            }


# Global instance
range_operations = DataTableRangeOperations()