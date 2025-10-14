"""
GoogleSheetDataTable - Google Sheets Implementation of DataTable Interface

This module provides the Google Sheets-specific implementation of the DataTableInterface.
All Google Sheets operations go through this class, which inherits from DataTableInterface.
"""

from typing import Dict, List, Optional, Any
import logging
from fastmcp import Context

from datatable_tools.interfaces.datatable import DataTableInterface
from datatable_tools.utils import parse_google_sheets_url, _process_data_input
from datatable_tools.range_operations import range_operations

logger = logging.getLogger(__name__)


def _col_letter_to_num(col: str) -> int:
    """Convert column letter to number (A=1, Z=26, AA=27)"""
    num = 0
    for char in col:
        num = num * 26 + (ord(char) - ord('A') + 1)
    return num


def _col_num_to_letter(num: int) -> str:
    """Convert column number to letter (1=A, 26=Z, 27=AA)"""
    letter = ''
    while num > 0:
        num -= 1
        letter = chr(num % 26 + ord('A')) + letter
        num //= 26
    return letter


def _auto_expand_range(range_address: str, data: list[list]) -> str:
    """
    Auto-expand single cell range to full range based on data dimensions.

    Args:
        range_address: Range in A1 notation (e.g., "A23", "B5:D10")
        data: 2D array of data to be written

    Returns:
        Expanded range address if single cell provided, otherwise original range
    """
    # If already a range (contains ':'), return as-is
    if ':' in range_address:
        return range_address

    # Calculate data dimensions
    rows = len(data)
    cols = max(len(row) for row in data) if data else 0

    # If no data or single cell with single value, return as-is
    if rows == 0 or cols == 0:
        return range_address

    # Parse start cell (e.g., "A23" -> col="A", row=23)
    import re
    match = re.match(r'^([A-Z]+)(\d+)$', range_address)
    if not match:
        return range_address

    start_col = match.group(1)
    start_row = int(match.group(2))

    # Calculate end cell
    end_col_num = _col_letter_to_num(start_col) + cols - 1
    end_col = _col_num_to_letter(end_col_num)
    end_row = start_row + rows - 1

    # Return expanded range
    expanded_range = f"{start_col}{start_row}:{end_col}{end_row}"
    logger.info(f"Auto-expanded range from '{range_address}' to '{expanded_range}' for data shape ({rows}x{cols})")
    return expanded_range


class GoogleSheetDataTable(DataTableInterface):
    """
    Google Sheets implementation of the DataTable interface.

    This class implements all DataTable operations for Google Sheets,
    using the GoogleSheetsService from third_party/google_sheets/service.py
    for low-level API operations.
    """

    async def write_new_sheet(
        self,
        ctx: Context,
        data: List[List[Any]],
        headers: Optional[List[str]] = None,
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new Google Sheets spreadsheet with the provided data.

        Implementation of DataTableInterface.write_new_sheet() for Google Sheets.
        """
        try:
            from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

            # Process data input using the same logic as other tools
            processed_data, processed_headers = _process_data_input(data, headers)

            # Convert processed data to Google Sheets format
            if not processed_data:
                values = [[""]]  # Empty cell
            else:
                # Convert all values to strings for Google Sheets API
                values = [[str(cell) for cell in row] for row in processed_data]

            # Convert headers to strings (empty list if no headers)
            headers_strings = [str(header) for header in processed_headers] if processed_headers else []

            # Use default sheet name if not provided
            final_sheet_name = sheet_name or "New DataTable"

            # Create new spreadsheet
            result = await GoogleSheetsService.create_new_spreadsheet(
                ctx=ctx,
                title=final_sheet_name,
                data=values,
                headers=headers_strings
            )

            if result.get('success'):
                spreadsheet_id = result.get('spreadsheet_id', '')
                sheet_id = result.get('sheet_id', 0)
                # Include gid in URL so subsequent update_range calls can identify the worksheet
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

                return {
                    "success": True,
                    "spreadsheet_url": spreadsheet_url,
                    "rows_created": len(values),
                    "columns_created": len(values[0]) if values else 0,
                    "data_shape": (len(values), len(values[0]) if values else 0),
                    "error": None,
                    "message": f"Successfully created new spreadsheet '{final_sheet_name}' with {len(values)} rows"
                }
            else:
                error_msg = result.get('error', 'Unknown error')
                return {
                    "success": False,
                    "spreadsheet_url": '',
                    "rows_created": 0,
                    "columns_created": 0,
                    "data_shape": (0, 0),
                    "error": error_msg,
                    "message": f"Failed to create new spreadsheet: {error_msg}"
                }

        except Exception as e:
            logger.error(f"Error creating new spreadsheet: {e}")
            return {
                "success": False,
                "spreadsheet_url": '',
                "rows_created": 0,
                "columns_created": 0,
                "data_shape": (0, 0),
                "error": str(e),
                "message": f"Failed to create new spreadsheet: {str(e)}"
            }

    async def append_rows(
        self,
        ctx: Context,
        uri: str,
        data: List[List[Any]]
    ) -> Dict[str, Any]:
        """
        Append data as new rows below existing data in Google Sheets.

        Implementation of DataTableInterface.append_rows() for Google Sheets.
        """
        try:
            spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
            if not spreadsheet_id:
                raise ValueError(f"Invalid Google Sheets URI: {uri}")

            return await self._handle_google_sheets_append(
                ctx, uri, data, None, spreadsheet_id, sheet_name, append_mode="rows"
            )

        except Exception as e:
            logger.error(f"Error appending rows to {uri}: {e}")
            raise

    async def append_columns(
        self,
        ctx: Context,
        uri: str,
        data: List[List[Any]],
        headers: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Append data as new columns to the right of existing data in Google Sheets.

        Implementation of DataTableInterface.append_columns() for Google Sheets.
        """
        try:
            spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
            if not spreadsheet_id:
                raise ValueError(f"Invalid Google Sheets URI: {uri}")

            return await self._handle_google_sheets_append(
                ctx, uri, data, headers, spreadsheet_id, sheet_name, append_mode="columns"
            )

        except Exception as e:
            logger.error(f"Error appending columns to {uri}: {e}")
            raise

    async def update_range(
        self,
        ctx: Context,
        uri: str,
        data: List[List[Any]],
        range_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Writes cell values to a Google Sheets range, replacing existing content.

        Implementation of DataTableInterface.update_range() for Google Sheets.
        """
        try:
            spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
            if not spreadsheet_id:
                raise ValueError(f"Invalid Google Sheets URI: {uri}")

            return await self._handle_google_sheets_update(
                ctx, uri, data, range_address, None, spreadsheet_id, sheet_name
            )

        except Exception as e:
            logger.error(f"Error updating data to {uri}: {e}")
            raise

    async def load_data_table(
        self,
        ctx: Context,
        uri: str
    ) -> Dict[str, Any]:
        """
        Load a table from Google Sheets.

        Implementation of DataTableInterface.load_data_table() for Google Sheets.
        """
        from datatable_tools.auth.service_decorator import require_google_service
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

        # Parse the URI to get Google Sheets info
        spreadsheet_id, sheet_name = parse_google_sheets_url(uri)

        if not spreadsheet_id:
            raise ValueError(f"Invalid Google Sheets URI: Could not parse Google Sheets ID from URI: {uri}")

        logger.info(f"Loading table from Google Sheets: {spreadsheet_id}")

        # Create decorated function for authentication
        @require_google_service("sheets", "sheets_read")
        async def _load_with_auth(service, ctx_inner, spreadsheet_id_inner, sheet_name_inner, uri_inner):
            # Load data from Google Sheets
            response = await GoogleSheetsService.read_sheet_structured(
                service, ctx_inner, spreadsheet_id_inner, sheet_name_inner
            )

            if not response.get("success"):
                raise Exception(f"Failed to read spreadsheet: {response.get('message', 'Unknown error')}")

            # Extract data and headers
            headers = response.get("headers", [])
            data = response.get("data", [])

            # Create metadata
            metadata = {
                "type": "google_sheets",
                "spreadsheet_id": spreadsheet_id_inner,
                "original_uri": uri_inner,
                "worksheet": response["worksheet"]["title"],
                "used_range": response.get("used_range"),
                "worksheet_url": response.get("worksheet_url"),
                "row_count": response.get("row_count", len(data)),
                "column_count": response.get("column_count", len(headers))
            }

            return {
                "success": True,
                "table_id": f"gs_{spreadsheet_id_inner}_{sheet_name_inner}",  # Virtual table ID
                "name": f"Sheet: {response['worksheet']['title']}",
                "shape": (len(data), len(headers)),
                "headers": headers,
                "data": data,
                "source_info": metadata,
                "error": None,
                "message": f"Loaded table from Google Sheets with {len(data)} rows and {len(headers)} columns"
            }

        return await _load_with_auth(ctx, spreadsheet_id, sheet_name, uri)

    # Private helper methods

    async def _handle_google_sheets_append(
        self,
        ctx: Context,
        uri: str,
        data: Any,
        headers: Optional[List[str]],
        spreadsheet_id: str,
        sheet_name: Optional[str],
        append_mode: str  # "rows" or "columns"
    ) -> Dict[str, Any]:
        """Handle Google Sheets append operations (rows or columns)"""
        if not spreadsheet_id:
            return {
                "success": False,
                "error": "Invalid Google Sheets URI",
                "message": f"Could not parse spreadsheet ID from URI: {uri}"
            }

        # Process data input
        processed_data, processed_headers = _process_data_input(data, headers)

        # Convert processed data to Google Sheets format
        if not processed_data:
            values = [[""]]
        else:
            # If headers were provided or detected, include them in the output
            if (processed_headers and
                len(processed_headers) > 0 and
                not processed_headers[0].startswith("Column_") and
                headers is not None):

                # Include headers as the first row, followed by the data
                values = [[str(cell) for cell in processed_headers]]
                values.extend([[str(cell) for cell in row] for row in processed_data])
                logger.info(f"Including provided headers in append output: {processed_headers}")
            else:
                # No headers provided or auto-generated headers, just use the data
                values = [[str(cell) for cell in row] for row in processed_data]

        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService
        try:
            # Get current worksheet info to determine used range
            worksheet_info = await GoogleSheetsService.get_worksheet_info(
                ctx=ctx,
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name
            )

            # Use the resolved sheet title
            final_worksheet = worksheet_info["title"]

            # Calculate append position based on mode
            if append_mode == "rows":
                start_row = worksheet_info["row_count"] + 1
                start_col_index = 0
            elif append_mode == "columns":
                start_row = 1
                start_col_index = worksheet_info["col_count"]
            else:
                start_row = 1
                start_col_index = 0

            # Convert column index to letter
            def col_index_to_letter(index):
                result = ""
                while index >= 0:
                    result = chr(65 + index % 26) + result
                    index = index // 26 - 1
                    if index < 0:
                        break
                return result

            start_col = col_index_to_letter(start_col_index)

            # Calculate end position
            end_row = start_row + len(values) - 1
            end_col_index = start_col_index + (len(values[0]) if values else 1) - 1
            end_col = col_index_to_letter(end_col_index)

            # Create the range address
            range_address = f"{start_col}{start_row}:{end_col}{end_row}"

        except Exception as e:
            logger.error(f"Failed to auto-detect append position: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to determine append position for {append_mode}"
            }

        # Use the range update logic
        full_range = f"{final_worksheet}!{range_address}"

        # Update the range
        success = await range_operations.google_sheets_service.update_range(
            ctx=ctx,
            spreadsheet_id=spreadsheet_id,
            range_notation=full_range,
            values=values
        )

        if success:
            return {
                "success": True,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": final_worksheet,
                "range": range_address,
                "append_mode": append_mode,
                "updated_cells": sum(len(row) for row in values),
                "data_shape": (len(values), len(values[0]) if values else 0),
                "message": f"Successfully appended {append_mode} at {range_address} in worksheet '{final_worksheet}'"
            }
        else:
            return {
                "success": False,
                "error": "Failed to append data",
                "message": f"Failed to append {append_mode} at {range_address} in worksheet '{final_worksheet}'"
            }

    async def _handle_google_sheets_update(
        self,
        ctx: Context,
        uri: str,
        data: Any,
        range_address: Optional[str],
        headers: Optional[List[str]],
        spreadsheet_id: str,
        sheet_name: Optional[str]
    ) -> Dict[str, Any]:
        """Handle Google Sheets updates with range support"""
        if not spreadsheet_id:
            return {
                "success": False,
                "error": "Invalid Google Sheets URI",
                "message": f"Could not parse spreadsheet ID from URI: {uri}"
            }

        # Process data input with automatic header detection
        processed_data, processed_headers = _process_data_input(data, headers=None)
        logger.info(f"_process_data_input in update_range: processed_headers = {processed_headers}")

        # Convert processed data to Google Sheets format
        if not processed_data:
            values = [[""]]
        else:
            # If headers were detected and extracted, include them back in the output
            if (processed_headers and
                len(processed_headers) > 0 and
                not processed_headers[0].startswith("Column_")):

                # Include headers as the first row, followed by the data
                values = [[str(cell) for cell in processed_headers]]
                values.extend([[str(cell) for cell in row] for row in processed_data])
                logger.info(f"Including detected headers in output: {processed_headers}")
            else:
                # No headers detected or auto-generated headers, just use the data
                values = [[str(cell) for cell in row] for row in processed_data]

        # Resolve gid: format to actual sheet name if needed
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService
        if sheet_name and sheet_name.startswith("gid:"):
            try:
                worksheet_info = await GoogleSheetsService.get_worksheet_info(
                    ctx=ctx,
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name
                )
                final_worksheet = worksheet_info["title"]
            except Exception as e:
                logger.error(f"Failed to resolve sheet gid: {e}")
                return {
                    "success": False,
                    "error": str(e),
                    "message": f"Failed to resolve sheet identifier: {sheet_name}"
                }
        elif not sheet_name:
            # No sheet specified, default to first sheet (gid=0)
            try:
                worksheet_info = await GoogleSheetsService.get_worksheet_info(
                    ctx=ctx,
                    spreadsheet_id=spreadsheet_id,
                    sheet_name="gid:0"
                )
                final_worksheet = worksheet_info["title"]
                logger.info(f"No sheet specified, using first sheet: '{final_worksheet}'")
            except Exception as e:
                logger.warning(f"Failed to get first sheet info, falling back to 'Sheet1': {e}")
                final_worksheet = "Sheet1"
        else:
            final_worksheet = sheet_name

        if range_address:
            # Range-specific update
            import re

            # Parse worksheet name from range_address if present (e.g., "Sheet1!A1:J6")
            worksheet_from_range = None
            if '!' in range_address:
                worksheet_from_range, range_address = range_address.split('!', 1)
                logger.info(f"Parsed worksheet '{worksheet_from_range}' from range_address")

                # Validate if worksheet_from_range exists
                try:
                    service_result = await GoogleSheetsService.get_worksheet_info(
                        ctx=ctx,
                        spreadsheet_id=spreadsheet_id,
                        sheet_name=worksheet_from_range
                    )
                    final_worksheet = service_result["title"]
                    logger.info(f"Validated worksheet '{worksheet_from_range}' exists in spreadsheet")
                except Exception as e:
                    logger.warning(
                        f"Worksheet '{worksheet_from_range}' from range_address not found. "
                        f"Falling back to worksheet from URI: '{final_worksheet}'. Error: {e}"
                    )

            # Auto-expand range if single cell provided
            range_address = _auto_expand_range(range_address, values)

            # Determine if it's a single cell update
            single_cell_pattern = r'^[A-Z]+\d+$'

            if re.match(single_cell_pattern, range_address) and len(values) == 1 and len(values[0]) == 1:
                # Single cell update
                return await range_operations.update_cell(
                    ctx=ctx,
                    spreadsheet_id=spreadsheet_id,
                    worksheet=final_worksheet,
                    cell_address=range_address,
                    value=values[0][0],
                    user_id=""
                )

            # For ranges, use the GoogleSheetsService directly
            full_range = f"{final_worksheet}!{range_address}"

            success = await range_operations.google_sheets_service.update_range(
                ctx=ctx,
                spreadsheet_id=spreadsheet_id,
                range_notation=full_range,
                values=values
            )

            if success:
                return {
                    "success": True,
                    "spreadsheet_id": spreadsheet_id,
                    "worksheet": final_worksheet,
                    "range": range_address,
                    "updated_cells": sum(len(row) for row in values),
                    "data_shape": (len(values), len(values[0]) if values else 0),
                    "message": f"Successfully updated range {range_address} in worksheet '{final_worksheet}'"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update range",
                    "message": f"Failed to update range {range_address} in worksheet '{final_worksheet}'"
                }
        else:
            # Full sheet replacement
            data_strings = [[str(cell) for cell in row] for row in values]
            headers_strings = [str(header) for header in processed_headers] if processed_headers else []

            result = await GoogleSheetsService.write_sheet_structured(
                ctx=ctx,
                spreadsheet_identifier=spreadsheet_id,
                data=data_strings,
                headers=headers_strings,
                sheet_name=final_worksheet
            )

            result.update({
                "export_type": "google_sheets",
                "rows_exported": len(data_strings),
                "columns_exported": len(headers_strings) if headers_strings else (len(data_strings[0]) if data_strings else 0),
                "original_uri": uri
            })

            return result
