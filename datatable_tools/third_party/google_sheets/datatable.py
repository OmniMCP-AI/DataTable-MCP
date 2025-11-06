"""
GoogleSheetDataTable - Google Sheets Implementation of DataTable Interface

This module provides the Google Sheets-specific implementation of the DataTableInterface.
All Google Sheets operations go through this class, which inherits from DataTableInterface.

Stage 4.2: Framework-agnostic implementation with NO FastMCP dependency.
Decorators moved to MCP layer (mcp_tools.py).
"""

from typing import Dict, List, Optional, Any
import logging
import asyncio
import re

from datatable_tools.interfaces.datatable import DataTableInterface
from datatable_tools.models import TableResponse, SpreadsheetResponse, UpdateResponse
from datatable_tools.google_sheets_helpers import (
    parse_google_sheets_uri,
    get_sheet_by_gid,
    auto_detect_headers,
    column_index_to_letter,
    column_letter_to_index,
    process_data_input
)

logger = logging.getLogger(__name__)


class GoogleSheetDataTable(DataTableInterface):
    """
    Google Sheets implementation of the DataTable interface.

    This class implements all DataTable operations for Google Sheets.
    Uses stacked decorators and direct Google Sheets API calls for clean architecture.
    """

    async def load_data_table(
        self,
        service,  # Authenticated Google Sheets service
        uri: str
    ) -> Dict[str, Any]:
        """
        Load a table from Google Sheets.

        Implementation of DataTableInterface.load_data_table() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
        """
        # Parse URI to extract spreadsheet_id and gid
        spreadsheet_id, gid = parse_google_sheets_uri(uri)

        logger.info(f"Loading table from Google Sheets: {spreadsheet_id}, gid={gid}")

        # Get sheet properties by gid (or first sheet if no gid)
        sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
        sheet_title = sheet_props['title']
        sheet_id = sheet_props['sheetId']

        # Read data from sheet using Google API directly
        range_name = f"'{sheet_title}'!A:ZZ"
        result = await asyncio.to_thread(
            service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute
        )

        all_data = result.get('values', [])

        # Calculate sheet dimensions
        if all_data:
            row_count = len(all_data)
            col_count = max(len(row) for row in all_data) if all_data else 0
        else:
            row_count = 0
            col_count = 0

        # Process headers and data (first row = headers)
        headers = []
        data_rows = []

        if all_data:
            headers = all_data[0] if all_data else []
            data_rows = all_data[1:] if len(all_data) > 1 else []

            # Ensure consistent column count
            if headers:
                max_cols = len(headers)
                for row in data_rows:
                    # Pad short rows
                    while len(row) < max_cols:
                        row.append("")
                    # Truncate long rows
                    if len(row) > max_cols:
                        row[:] = row[:max_cols]

        # Convert data from list of lists to list of dicts
        data = []
        if headers and data_rows:
            for row in data_rows:
                row_dict = {}
                for i, header in enumerate(headers):
                    # Use the header as key, and the corresponding cell value
                    row_dict[header] = row[i] if i < len(row) else ""
                data.append(row_dict)

        # Build metadata
        metadata = {
            "type": "google_sheets",
            "spreadsheet_id": spreadsheet_id,
            "original_uri": uri,
            "worksheet": sheet_title,
            "used_range": f"A1:{chr(65 + col_count - 1)}{row_count}" if row_count > 0 and col_count > 0 else "A1:A1",
            "worksheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
            "row_count": row_count,
            "column_count": col_count
        }

        return TableResponse(
            success=True,
            table_id=f"gs_{spreadsheet_id}_{gid or '0'}",
            name=f"Sheet: {sheet_title}",
            shape=f"({len(data)},{len(headers)})",
            data=data,
            source_info=metadata,
            error=None,
            message=f"Loaded table from Google Sheets with {len(data)} rows and {len(headers)} columns"
        )

    async def write_new_sheet(
        self,
        service,  # Authenticated Google Sheets service
        data: List[List[Any]],
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new Google Sheets spreadsheet with the provided data.

        Implementation of DataTableInterface.write_new_sheet() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            data: 2D array of table data or list of dicts (DataFrame-like)
            sheet_name: Optional name for the spreadsheet
        """
        try:
            # Process input data (handles both 2D array and list of dicts)
            extracted_headers, data_rows = process_data_input(data)

            # If data was list of dicts, use extracted headers
            if extracted_headers:
                final_headers = extracted_headers
                final_data = data_rows
            else:
                # Auto-detect headers if data_rows is 2D array
                # Use data_rows (already processed by process_data_input)
                detected_headers, processed_rows = auto_detect_headers(data_rows)

                # Use detected headers
                final_headers = detected_headers
                final_data = processed_rows if detected_headers else data_rows

            # Use default sheet name if not provided
            title = sheet_name or "New DataTable"

            # Prepare data for Google Sheets API
            # Serialize nested structures (lists/dicts) to JSON strings
            # Convert other types to strings (Google Sheets API requirement)
            from datatable_tools.google_sheets_helpers import serialize_row

            values = [serialize_row(row) for row in final_data]

            # Convert to strings after serialization
            values = [[str(cell) if cell is not None else "" for cell in row] for row in values]

            # Prepare write data with headers
            write_data = []
            if final_headers:
                write_data.append([str(h) for h in final_headers])
            write_data.extend(values)

            # Create new spreadsheet
            spreadsheet_body = {'properties': {'title': title}}
            result = await asyncio.to_thread(
                service.spreadsheets().create(body=spreadsheet_body).execute
            )

            spreadsheet_id = result['spreadsheetId']
            sheet_id = result['sheets'][0]['properties']['sheetId']
            sheet_title = result['sheets'][0]['properties']['title']

            # Write data to the new spreadsheet
            if write_data:
                range_name = f"'{sheet_title}'!A1"
                body = {'values': write_data}
                await asyncio.to_thread(
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption='RAW',
                        body=body
                    ).execute
                )

            # Include gid in URL for subsequent operations
            spreadsheet_url_with_gid = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

            total_cols = len(write_data[0]) if write_data else 0

            return SpreadsheetResponse(
                success=True,
                spreadsheet_url=spreadsheet_url_with_gid,
                rows_created=len(values),
                columns_created=total_cols,
                shape=f"({len(values)},{total_cols})",
                error=None,
                message=f"Successfully created new spreadsheet '{title}' with {len(values)} rows"
            )

        except Exception as e:
            logger.error(f"Error creating new spreadsheet: {e}")
            return SpreadsheetResponse(
                success=False,
                spreadsheet_url='',
                rows_created=0,
                columns_created=0,
                shape="(0,0)",
                error=str(e),
                message=f"Failed to create new spreadsheet: {e}"
            )

    async def append_rows(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[List[Any]]
    ) -> Dict[str, Any]:
        """
        Append data as new rows below existing data in Google Sheets.

        Implementation of DataTableInterface.append_rows() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            data: 2D array of row data to append or list of dicts (DataFrame-like)
        """
        try:
            # Parse URI to extract spreadsheet_id and gid
            spreadsheet_id, gid = parse_google_sheets_uri(uri)

            # Get sheet properties by gid (or first sheet if no gid)
            sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_title = sheet_props['title']
            sheet_id = sheet_props['sheetId']

            # Process input data (handles both 2D array and list of dicts)
            extracted_headers, data_rows = process_data_input(data)

            # If data is already processed (list of dicts), use the converted data
            if extracted_headers:
                values_to_write = data_rows
            else:
                # Auto-detect headers in data_rows (already processed, but don't write headers when appending rows)
                detected_headers, processed_rows = auto_detect_headers(data_rows)
                # For append_rows, we only write data rows, not headers
                values_to_write = processed_rows if detected_headers else data_rows

            # Convert to strings for Google Sheets API
            values = [[str(cell) if cell is not None else "" for cell in row] for row in values_to_write]

            if not values:
                values = [[""]]

            # Get current data to determine last row
            range_name = f"'{sheet_title}'!A:ZZ"
            result = await asyncio.to_thread(
                service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_name
                ).execute
            )

            all_data = result.get('values', [])
            row_count = len(all_data)

            # Calculate append position (start from next row after last data)
            start_row = row_count + 1
            start_col = "A"
            end_row = start_row + len(values) - 1
            end_col_index = len(values[0]) - 1 if values else 0
            end_col = column_index_to_letter(end_col_index)

            # Create range address
            range_address = f"{start_col}{start_row}:{end_col}{end_row}"
            full_range = f"'{sheet_title}'!{range_address}"

            # Append data
            body = {'values': values}
            await asyncio.to_thread(
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=full_range,
                    valueInputOption='RAW',
                    body=body
                ).execute
            )

            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
            
            return UpdateResponse(
                success=True,
                spreadsheet_url=spreadsheet_url,
                spreadsheet_id=spreadsheet_id,
                worksheet=sheet_title,
                range=range_address,
                updated_cells=sum(len(row) for row in values),
                shape=f"({len(values)},{len(values[0]) if values else 0})",
                error=None,
                message=f"Successfully appended rows at {range_address} in worksheet '{sheet_title}'"
            )

        except Exception as e:
            logger.error(f"Error appending rows to {uri}: {e}")
            return UpdateResponse(
                success=False,
                spreadsheet_url="",
                spreadsheet_id="",
                worksheet="",
                range="",
                updated_cells=0,
                shape="(0,0)",
                error=str(e),
                message=f"Failed to append rows: {e}"
            )

    async def append_columns(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[List[Any]]
    ) -> Dict[str, Any]:
        """
        Append data as new columns to the right of existing data in Google Sheets.

        Implementation of DataTableInterface.append_columns() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            data: 2D array of column data to append or list of dicts (DataFrame-like)
        """
        try:
            # Parse URI to extract spreadsheet_id and gid
            spreadsheet_id, gid = parse_google_sheets_uri(uri)

            # Get sheet properties by gid (or first sheet if no gid)
            sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_title = sheet_props['title']
            sheet_id = sheet_props['sheetId']

            # Process input data (handles both 2D array and list of dicts)
            extracted_headers, data_rows = process_data_input(data)

            # If data was list of dicts, use extracted headers
            if extracted_headers:
                final_headers = extracted_headers
                final_data = data_rows
            else:
                # Auto-detect headers if data_rows is 2D array
                detected_headers, processed_rows = auto_detect_headers(data_rows)

                # Use detected headers
                final_headers = detected_headers
                final_data = processed_rows if detected_headers else data_rows

            # Convert to strings for Google Sheets API
            values_only = [[str(cell) if cell is not None else "" for cell in row] for row in final_data]

            # Prepare write data with headers
            values = []
            if final_headers:
                values.append([str(h) for h in final_headers])
            values.extend(values_only)

            if not values:
                values = [[""]]

            # Get current data to determine last column
            range_name = f"'{sheet_title}'!A:ZZ"
            result = await asyncio.to_thread(
                service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_name
                ).execute
            )

            all_data = result.get('values', [])
            col_count = max(len(row) for row in all_data) if all_data else 0

            # Calculate append position (start from next column after last data)
            start_row = 1
            start_col_index = col_count
            start_col = column_index_to_letter(start_col_index)

            end_row = start_row + len(values) - 1
            end_col_index = start_col_index + (len(values[0]) - 1 if values else 0)
            end_col = column_index_to_letter(end_col_index)

            # Create range address
            range_address = f"{start_col}{start_row}:{end_col}{end_row}"
            full_range = f"'{sheet_title}'!{range_address}"

            # Append data
            body = {'values': values}
            await asyncio.to_thread(
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=full_range,
                    valueInputOption='RAW',
                    body=body
                ).execute
            )

            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
            
            return UpdateResponse(
                success=True,
                spreadsheet_url=spreadsheet_url,
                spreadsheet_id=spreadsheet_id,
                worksheet=sheet_title,
                range=range_address,
                updated_cells=sum(len(row) for row in values),
                shape=f"({len(values)},{len(values[0]) if values else 0})",
                error=None,
                message=f"Successfully appended columns at {range_address} in worksheet '{sheet_title}'"
            )

        except Exception as e:
            logger.error(f"Error appending columns to {uri}: {e}")
            return UpdateResponse(
                success=False,
                spreadsheet_url="",
                spreadsheet_id="",
                worksheet="",
                range="",
                updated_cells=0,
                shape="(0,0)",
                error=str(e),
                message=f"Failed to append columns: {e}"
            )

    async def update_range(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[List[Any]],
        range_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Writes cell values to a Google Sheets range, replacing existing content.

        Implementation of DataTableInterface.update_range() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            data: 2D array of cell values or list of dicts (DataFrame-like)
            range_address: A1 notation range address
        """
        try:
            # Parse URI to extract spreadsheet_id and gid
            spreadsheet_id, gid = parse_google_sheets_uri(uri)

            # Get sheet properties by gid (or first sheet if no gid)
            sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_title = sheet_props['title']
            sheet_id = sheet_props['sheetId']

            # Process input data (handles both 2D array and list of dicts)
            extracted_headers, data_rows = process_data_input(data)

            # Special handling for 1D array with single-column range:
            # If range_address is a single column (e.g., "B", "C:C") and data_rows is a single row,
            # transpose it to column format (multiple rows, single column)
            from datatable_tools.google_sheets_helpers import is_single_column_range
            if (range_address and is_single_column_range(range_address) and
                len(data_rows) == 1 and len(data_rows[0]) > 1):
                # Transpose: single row with multiple values -> multiple rows with single value
                data_rows = [[value] for value in data_rows[0]]
                logger.debug(f"Transposed 1D array to column format for single-column range '{range_address}': {len(data_rows)} rows x 1 column")

            # If data was list of dicts, ALWAYS include extracted headers
            # (list of dicts format implies DataFrame-like structure with headers)
            if extracted_headers:
                values = [[str(h) for h in extracted_headers]]
                values.extend([[str(cell) if cell is not None else "" for cell in row] for row in data_rows])
                logger.info(f"Including extracted headers from list of dicts: {extracted_headers}")
            else:
                # Auto-detect headers in data_rows (already processed)
                detected_headers, processed_rows = auto_detect_headers(data_rows)

                # If headers were detected, include them in the output ONLY if no explicit range_address
                if detected_headers and not range_address:
                    values = [[str(h) for h in detected_headers]]
                    values.extend([[str(cell) if cell is not None else "" for cell in row] for row in processed_rows])
                    logger.info(f"Including detected headers in output: {detected_headers}")
                elif detected_headers:
                    # Explicit range_address provided - only use processed data rows
                    values = [[str(cell) if cell is not None else "" for cell in row] for row in processed_rows]
                    logger.info(f"Using processed data rows only (no headers) for explicit range_address: {range_address}")
                else:
                    # Use data_rows (already processed by process_data_input)
                    values = [[str(cell) if cell is not None else "" for cell in row] for row in data_rows]

            if not values:
                values = [[""]]

            # Parse worksheet name from range_address if present (e.g., "Sheet1!A1:J6")
            final_range = range_address
            if range_address and '!' in range_address:
                worksheet_from_range, final_range = range_address.split('!', 1)
                worksheet_from_range = worksheet_from_range.strip("'\"")
                logger.info(f"Parsed worksheet '{worksheet_from_range}' from range_address")

                # Validate if worksheet exists
                try:
                    metadata = await asyncio.to_thread(
                        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute
                    )
                    sheets = metadata.get('sheets', [])
                    found = False
                    for sheet in sheets:
                        if sheet.get('properties', {}).get('title') == worksheet_from_range:
                            sheet_title = worksheet_from_range
                            found = True
                            break

                    if not found:
                        logger.warning(
                            f"Worksheet '{worksheet_from_range}' from range_address not found. "
                            f"Falling back to worksheet from URI: '{sheet_title}'."
                        )
                except Exception as e:
                    logger.warning(
                        f"Error validating worksheet '{worksheet_from_range}': {e}. "
                        f"Falling back to worksheet from URI: '{sheet_title}'."
                    )

            # Auto-expand range if single cell provided
            def auto_expand_range(range_addr: str, data_values: list[list]) -> str:
                """Auto-expand range to match data dimensions."""
                if not range_addr:
                    return "A1"

                rows = len(data_values)
                cols = max(len(row) for row in data_values) if data_values else 0

                if rows == 0 or cols == 0:
                    return range_addr

                # Case 1: Range with colon (e.g., "F2:F5")
                if ':' in range_addr:
                    # Parse start cell from range
                    start_cell = range_addr.split(':')[0]
                    match = re.match(r'^([A-Z]+)(\d+)$', start_cell)
                    if not match:
                        return range_addr

                    start_col = match.group(1)
                    start_row = int(match.group(2))

                    # Calculate end cell based on data dimensions
                    start_col_index = column_letter_to_index(start_col)
                    end_col_index = start_col_index + cols - 1
                    end_col = column_index_to_letter(end_col_index)
                    end_row = start_row + rows - 1

                    expanded = f"{start_col}{start_row}:{end_col}{end_row}"
                    logger.info(f"Auto-expanded range from '{range_addr}' to '{expanded}' for data shape ({rows}x{cols})")
                    return expanded

                # Case 2: Just a column letter (e.g., "B", "AA")
                if range_addr.isalpha():
                    end_row = rows
                    expanded = f"{range_addr}1:{range_addr}{end_row}"
                    logger.info(f"Auto-expanded column '{range_addr}' to '{expanded}' for data shape ({rows}x{cols})")
                    return expanded

                # Case 3: Cell address (e.g., "A23")
                match = re.match(r'^([A-Z]+)(\d+)$', range_addr)
                if not match:
                    return range_addr

                start_col = match.group(1)
                start_row = int(match.group(2))

                # Calculate end cell
                start_col_index = column_letter_to_index(start_col)
                end_col_index = start_col_index + cols - 1
                end_col = column_index_to_letter(end_col_index)
                end_row = start_row + rows - 1

                expanded = f"{start_col}{start_row}:{end_col}{end_row}"
                logger.info(f"Auto-expanded range from '{range_addr}' to '{expanded}' for data shape ({rows}x{cols})")
                return expanded

            final_range = auto_expand_range(final_range, values) if final_range else "A1"

            # Create full range notation
            full_range = f"'{sheet_title}'!{final_range}"

            # Update the range
            body = {'values': values}
            await asyncio.to_thread(
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=full_range,
                    valueInputOption='RAW',
                    body=body
                ).execute
            )

            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
            
            return UpdateResponse(
                success=True,
                spreadsheet_url=spreadsheet_url,
                spreadsheet_id=spreadsheet_id,
                worksheet=sheet_title,
                range=final_range,
                updated_cells=sum(len(row) for row in values),
                shape=f"({len(values)},{len(values[0]) if values else 0})",
                error=None,
                message=f"Successfully updated range {final_range} in worksheet '{sheet_title}'"
            )

        except Exception as e:
            logger.error(f"Error updating data to {uri}: {e}")
            return UpdateResponse(
                success=False,
                spreadsheet_url="",
                spreadsheet_id="",
                worksheet="",
                range="",
                updated_cells=0,
                shape="(0,0)",
                error=str(e),
                message=f"Failed to update range: {e}"
            )
