from typing import Dict, List, Optional, Any
import logging
from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

logger = logging.getLogger(__name__)


class DataTableRangeOperations:
    """Detailed spreadsheet operations using GoogleSheetsService"""

    def __init__(self):
        self.google_sheets_service = GoogleSheetsService()

    @staticmethod
    def _column_index_to_letter(index: int) -> str:
        """Convert column index (0-based) to Excel column letter"""
        result = ""
        while index >= 0:
            result = chr(index % 26 + ord('A')) + result
            index = index // 26 - 1
        return result

    async def update_cell(
        self,
        ctx: Any,  # Context parameter needed for Google Sheets API calls
        spreadsheet_id: str,
        worksheet: Any,
        cell_address: str,
        value: Any,
        user_id: str
    ) -> Dict[str, Any]:
        """
        Update a single cell in the spreadsheet

        Args:
            ctx: Context for the API call
            spreadsheet_id: Target spreadsheet ID
            worksheet: Worksheet identifier (name, id, or WorkSheetInfo)
            cell_address: Cell address in A1 notation (e.g., "B5")
            value: Value to set in the cell
            user_id: User ID for authentication

        Returns:
            Dict with update results
        """
        try:
            # Convert worksheet to name
            worksheet_name = worksheet if isinstance(worksheet, str) else str(worksheet)

            # Create range notation with worksheet
            range_notation = f"{worksheet_name}!{cell_address}"

            # Prepare values as 2D array
            values = [[str(value)]]

            # Update the range using GoogleSheetsService
            success = await self.google_sheets_service.update_range(
                ctx=ctx,
                spreadsheet_id=spreadsheet_id,
                range_notation=range_notation,
                values=values
            )

            if success:
                return {
                    "success": True,
                    "spreadsheet_id": spreadsheet_id,
                    "worksheet": worksheet_name,
                    "updated_range": cell_address,
                    "updated_cells": 1,
                    "message": f"Successfully updated cell {cell_address} in worksheet '{worksheet_name}'"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update cell",
                    "message": f"Failed to update cell {cell_address} in worksheet '{worksheet_name}'"
                }

        except Exception as e:
            logger.error(f"Error updating cell {cell_address}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Error updating cell {cell_address}: {e}"
            }

    async def update_row(
        self,
        ctx: Any,  # Context parameter needed for Google Sheets API calls
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
            # Convert worksheet to name
            worksheet_name = worksheet if isinstance(worksheet, str) else str(worksheet)

            # Calculate end column based on data length
            start_col_index = ord(start_column.upper()) - ord('A')
            end_col_index = start_col_index + len(row_data) - 1
            end_column = self._column_index_to_letter(end_col_index)

            range_str = f"{start_column}{row_number}:{end_column}{row_number}"
            range_notation = f"{worksheet_name}!{range_str}"

            # Prepare values as 2D array
            values = [[str(val) for val in row_data]]

            # Update the range using GoogleSheetsService
            success = await self.google_sheets_service.update_range(
                ctx=ctx,
                spreadsheet_id=spreadsheet_id,
                range_notation=range_notation,
                values=values
            )

            if success:
                return {
                    "success": True,
                    "spreadsheet_id": spreadsheet_id,
                    "worksheet": worksheet_name,
                    "updated_range": range_str,
                    "updated_cells": len(row_data),
                    "row_number": row_number,
                    "columns_updated": len(row_data),
                    "message": f"Successfully updated row {row_number} in worksheet '{worksheet_name}'"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update row",
                    "message": f"Failed to update row {row_number} in worksheet '{worksheet_name}'"
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
        ctx: Any,  # Context parameter needed for Google Sheets API calls
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
            # Convert worksheet to name
            worksheet_name = worksheet if isinstance(worksheet, str) else str(worksheet)

            end_row = start_row + len(column_data) - 1
            range_str = f"{column.upper()}{start_row}:{column.upper()}{end_row}"
            range_notation = f"{worksheet_name}!{range_str}"

            # Convert column data to 2D array (each value in its own row)
            values = [[str(val)] for val in column_data]

            # Update the range using GoogleSheetsService
            success = await self.google_sheets_service.update_range(
                ctx=ctx,
                spreadsheet_id=spreadsheet_id,
                range_notation=range_notation,
                values=values
            )

            if success:
                return {
                    "success": True,
                    "spreadsheet_id": spreadsheet_id,
                    "worksheet": worksheet_name,
                    "updated_range": range_str,
                    "updated_cells": len(column_data),
                    "column": column.upper(),
                    "rows_updated": len(column_data),
                    "message": f"Successfully updated column {column} in worksheet '{worksheet_name}'"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update column",
                    "message": f"Failed to update column {column} in worksheet '{worksheet_name}'"
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
        include_headers: bool = True,
        ctx: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        DEPRECATED: This method relied on table_manager which has been removed.
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
        raise NotImplementedError("update_table_range is deprecated - table_manager has been removed. Use update_range directly instead.")

        # OLD CODE - kept for reference but not functional
        """
        try:
            table = table_manager.get_table(table_id)
            if not table:
                return {
                    "success": False,
                    "error": f"Table {table_id} not found",
                    "message": "Source table does not exist"
                }

            # Convert worksheet to name
            worksheet_name = worksheet if isinstance(worksheet, str) else str(worksheet)

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
            range_notation = f"{worksheet_name}!{range_str}"

            # Update the range using GoogleSheetsService
            success = await self.google_sheets_service.update_range(
                ctx=ctx,
                spreadsheet_id=spreadsheet_id,
                range_notation=range_notation,
                values=export_data
            )

            if success:
                return {
                    "success": True,
                    "table_id": table_id,
                    "spreadsheet_id": spreadsheet_id,
                    "worksheet": worksheet_name,
                    "updated_range": range_str,
                    "updated_cells": num_rows * num_cols,
                    "rows_exported": len(table.data),
                    "columns_exported": len(table.headers) if table.headers else 0,
                    "included_headers": include_headers,
                    "table_name": table.metadata.name,
                    "message": f"Successfully updated table range for table {table_id} in worksheet '{worksheet_name}'"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update table range",
                    "message": f"Failed to update table range for table {table_id} in worksheet '{worksheet_name}'"
                }

        except Exception as e:
            logger.error(f"Error updating table range for table {table_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to update table range for table {table_id}"
            }
        """


# Global instance
range_operations = DataTableRangeOperations()