"""
GoogleSheetDataTable - Google Sheets Implementation of DataTable Interface

This module provides the Google Sheets-specific implementation of the DataTableInterface.
All Google Sheets operations go through this class, which inherits from DataTableInterface.

Stage 4.2: Framework-agnostic implementation with NO FastMCP dependency.
Decorators moved to MCP layer (detailed_tools.py).
"""

from typing import Dict, List, Optional, Any
import logging
import asyncio
import re

from datatable_tools.interfaces.datatable import DataTableInterface
from datatable_tools.google_sheets_helpers import (
    parse_google_sheets_uri,
    get_sheet_by_gid,
    auto_detect_headers,
    column_index_to_letter,
    column_letter_to_index
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
        data = []

        if all_data:
            headers = all_data[0] if all_data else []
            data = all_data[1:] if len(all_data) > 1 else []

            # Ensure consistent column count
            if headers:
                max_cols = len(headers)
                for row in data:
                    # Pad short rows
                    while len(row) < max_cols:
                        row.append("")
                    # Truncate long rows
                    if len(row) > max_cols:
                        row[:] = row[:max_cols]

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

        return {
            "success": True,
            "table_id": f"gs_{spreadsheet_id}_{gid or '0'}",
            "name": f"Sheet: {sheet_title}",
            "shape": (len(data), len(headers)),
            "headers": headers,
            "data": data,
            "source_info": metadata,
            "error": None,
            "message": f"Loaded table from Google Sheets with {len(data)} rows and {len(headers)} columns"
        }

    async def write_new_sheet(
        self,
        service,  # Authenticated Google Sheets service
        data: List[List[Any]],
        headers: Optional[List[str]] = None,
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new Google Sheets spreadsheet with the provided data.

        Implementation of DataTableInterface.write_new_sheet() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            data: 2D array of table data
            headers: Optional column headers
            sheet_name: Optional name for the spreadsheet
        """
        try:
            # Auto-detect headers if not provided
            detected_headers, data_rows = auto_detect_headers(data)

            # Use provided headers if given, otherwise use detected headers
            final_headers = headers if headers is not None else detected_headers
            final_data = data_rows if detected_headers else data

            # Use default sheet name if not provided
            title = sheet_name or "New DataTable"

            # Prepare data for Google Sheets API
            # Convert data to strings (Google Sheets API requirement)
            values = [[str(cell) if cell is not None else "" for cell in row] for row in final_data]

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

            return {
                "success": True,
                "spreadsheet_url": spreadsheet_url_with_gid,
                "rows_created": len(values),
                "columns_created": total_cols,
                "data_shape": (len(values), total_cols),
                "error": None,
                "message": f"Successfully created new spreadsheet '{title}' with {len(values)} rows"
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
                "message": f"Failed to create new spreadsheet: {e}"
            }

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
            data: 2D array of row data to append
        """
        try:
            # Parse URI to extract spreadsheet_id and gid
            spreadsheet_id, gid = parse_google_sheets_uri(uri)

            # Get sheet properties by gid (or first sheet if no gid)
            sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_title = sheet_props['title']

            # Auto-detect headers in data (but don't write headers when appending rows)
            detected_headers, data_rows = auto_detect_headers(data)
            # For append_rows, we only write data rows, not headers
            values_to_write = data_rows if detected_headers else data

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

            return {
                "success": True,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": sheet_title,
                "range": range_address,
                "append_mode": "rows",
                "updated_cells": sum(len(row) for row in values),
                "data_shape": [len(values), len(values[0]) if values else 0],
                "message": f"Successfully appended rows at {range_address} in worksheet '{sheet_title}'"
            }

        except Exception as e:
            logger.error(f"Error appending rows to {uri}: {e}")
            raise

    async def append_columns(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[List[Any]],
        headers: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Append data as new columns to the right of existing data in Google Sheets.

        Implementation of DataTableInterface.append_columns() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            data: 2D array of column data to append
            headers: Optional column headers
        """
        try:
            # Parse URI to extract spreadsheet_id and gid
            spreadsheet_id, gid = parse_google_sheets_uri(uri)

            # Get sheet properties by gid (or first sheet if no gid)
            sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_title = sheet_props['title']

            # Process headers - auto-detect if not provided
            detected_headers, data_rows = auto_detect_headers(data)

            # Determine final headers and data
            if headers is not None:
                # User provided headers explicitly
                final_headers = headers
                final_data = data
            elif detected_headers:
                # Headers auto-detected
                final_headers = detected_headers
                final_data = data_rows
            else:
                # No headers
                final_headers = []
                final_data = data

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

            return {
                "success": True,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": sheet_title,
                "range": range_address,
                "append_mode": "columns",
                "updated_cells": sum(len(row) for row in values),
                "data_shape": [len(values), len(values[0]) if values else 0],
                "message": f"Successfully appended columns at {range_address} in worksheet '{sheet_title}'"
            }

        except Exception as e:
            logger.error(f"Error appending columns to {uri}: {e}")
            raise

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
            data: 2D array of cell values
            range_address: A1 notation range address
        """
        try:
            # Parse URI to extract spreadsheet_id and gid
            spreadsheet_id, gid = parse_google_sheets_uri(uri)

            # Get sheet properties by gid (or first sheet if no gid)
            sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_title = sheet_props['title']

            # Auto-detect headers in data and include them in output
            detected_headers, data_rows = auto_detect_headers(data)

            # If headers were detected, include them in the output
            if detected_headers:
                values = [[str(h) for h in detected_headers]]
                values.extend([[str(cell) if cell is not None else "" for cell in row] for row in data_rows])
                logger.info(f"Including detected headers in output: {detected_headers}")
            else:
                values = [[str(cell) if cell is not None else "" for cell in row] for row in data]

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
                """Auto-expand single cell range to full range based on data dimensions."""
                if not range_addr or ':' in range_addr:
                    return range_addr  # Already a range or None

                rows = len(data_values)
                cols = max(len(row) for row in data_values) if data_values else 0

                if rows == 0 or cols == 0:
                    return range_addr

                # Parse start cell (e.g., "A23" -> col="A", row=23)
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

            return {
                "success": True,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": sheet_title,
                "range": final_range,
                "updated_cells": sum(len(row) for row in values),
                "data_shape": [len(values), len(values[0]) if values else 0],
                "message": f"Successfully updated range {final_range} in worksheet '{sheet_title}'"
            }

        except Exception as e:
            logger.error(f"Error updating data to {uri}: {e}")
            raise
