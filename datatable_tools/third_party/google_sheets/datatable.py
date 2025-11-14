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

    async def read_worksheet_with_formulas(
        self,
        service,  # Authenticated Google Sheets service
        uri: str
    ) -> Dict[str, Any]:
        """
        Read a worksheet from Google Sheets with formulas instead of calculated values.

        Similar to load_data_table, but returns the raw formulas from cells
        instead of the computed values. For cells without formulas, returns the plain value.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI

        Returns:
            TableResponse with cell formulas (e.g., "=SUM(A1:A10)" instead of "100")
        """
        # Parse URI to extract spreadsheet_id and gid
        spreadsheet_id, gid = parse_google_sheets_uri(uri)

        logger.info(f"Loading table with formulas from Google Sheets: {spreadsheet_id}, gid={gid}")

        # Get sheet properties by gid (or first sheet if no gid)
        sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
        sheet_title = sheet_props['title']
        sheet_id = sheet_props['sheetId']

        # Read data from sheet using FORMULA mode to get raw formulas
        range_name = f"'{sheet_title}'!A:ZZ"
        result = await asyncio.to_thread(
            service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueRenderOption='FORMULA'  # KEY DIFFERENCE: Get formulas instead of values
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
                    # Use the header as key, and the corresponding cell value (which is now a formula if present)
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
            "column_count": col_count,
            "value_render_option": "FORMULA"  # Indicate that this contains formulas
        }

        return TableResponse(
            success=True,
            table_id=f"gs_{spreadsheet_id}_{gid or '0'}_formulas",
            name=f"Sheet (Formulas): {sheet_title}",
            shape=f"({len(data)},{len(headers)})",
            data=data,
            source_info=metadata,
            error=None,
            message=f"Loaded table with formulas from Google Sheets with {len(data)} rows and {len(headers)} columns"
        )

    async def preview_worksheet_with_formulas(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        limit: int = 5
    ) -> Dict[str, Any]:
        """
        Preview the first N rows of a worksheet with formulas (quick preview).

        Specifically returns formula strings instead of calculated values.
        Useful for quickly inspecting the beginning of a large sheet without loading all data.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            limit: Number of data rows to preview (default: 5, max: 100)

        Returns:
            TableResponse with limited rows containing formulas
        """
        # Validate and cap limit
        limit = max(1, min(limit, 100))  # Between 1 and 100

        # Parse URI to extract spreadsheet_id and gid
        spreadsheet_id, gid = parse_google_sheets_uri(uri)

        logger.info(f"Previewing worksheet formulas: {spreadsheet_id}, gid={gid}, limit={limit}")

        # Get sheet properties by gid (or first sheet if no gid)
        sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
        sheet_title = sheet_props['title']
        sheet_id = sheet_props['sheetId']

        # Read only the first N+1 rows (N data rows + 1 header row)
        # Use A1 notation to limit the range: A1:ZZ{limit+1}
        range_name = f"'{sheet_title}'!A1:ZZ{limit + 1}"

        result = await asyncio.to_thread(
            service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueRenderOption='FORMULA'  # Get formulas instead of values
            ).execute
        )

        all_data = result.get('values', [])

        # Calculate dimensions
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
                    row_dict[header] = row[i] if i < len(row) else ""
                data.append(row_dict)

        # Build metadata
        metadata = {
            "type": "google_sheets",
            "spreadsheet_id": spreadsheet_id,
            "original_uri": uri,
            "worksheet": sheet_title,
            "worksheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
            "preview_limit": limit,
            "is_preview": True,
            "value_render_option": "FORMULA",
            "row_count": len(data),
            "column_count": len(headers)
        }

        return TableResponse(
            success=True,
            table_id=f"gs_{spreadsheet_id}_{gid or '0'}_preview_formulas",
            name=f"Preview (Formulas): {sheet_title}",
            shape=f"({len(data)},{len(headers)})",
            data=data,
            source_info=metadata,
            error=None,
            message=f"Preview loaded {len(data)} row(s) with formulas from Google Sheets (limit: {limit})"
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
            raise Exception(f"Failed to create new spreadsheet: {e}") from e

    async def write_new_worksheet(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[List[Any]],
        worksheet_name: str
    ) -> Dict[str, Any]:
        """
        Create a new worksheet in an existing Google Sheets spreadsheet and write data to it.

        If a worksheet with the same name already exists, it will return the existing worksheet
        information without creating a duplicate.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI pointing to the target spreadsheet
            data: 2D array of table data or list of dicts (DataFrame-like)
            worksheet_name: Name for the new worksheet

        Returns:
            UpdateResponse containing worksheet URL and write information
        """
        try:
            # Parse URI to extract spreadsheet_id
            spreadsheet_id, _ = parse_google_sheets_uri(uri)

            logger.info(f"Creating worksheet '{worksheet_name}' in spreadsheet {spreadsheet_id}")

            # Get current worksheets
            metadata = await asyncio.to_thread(
                service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute
            )
            sheets = metadata.get('sheets', [])

            # Check if worksheet with this name already exists
            existing_worksheet = None
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == worksheet_name:
                    existing_worksheet = sheet
                    break

            # Create worksheet if it doesn't exist
            if existing_worksheet:
                logger.info(f"Worksheet '{worksheet_name}' already exists")
                worksheet_id = existing_worksheet['properties']['sheetId']
            else:
                # Create new worksheet
                add_sheet_request = {
                    "requests": [{
                        "addSheet": {
                            "properties": {
                                "title": worksheet_name,
                                "gridProperties": {
                                    "rowCount": 1000,
                                    "columnCount": 26
                                }
                            }
                        }
                    }]
                }

                add_result = await asyncio.to_thread(
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=add_sheet_request
                    ).execute
                )

                worksheet_id = add_result['replies'][0]['addSheet']['properties']['sheetId']
                logger.info(f"Successfully created worksheet '{worksheet_name}' with ID {worksheet_id}")

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

            # Prepare data for Google Sheets API
            from datatable_tools.google_sheets_helpers import serialize_row

            values = [serialize_row(row) for row in final_data]

            # Convert to strings after serialization
            values = [[str(cell) if cell is not None else "" for cell in row] for row in values]

            # Prepare write data with headers
            write_data = []
            if final_headers:
                write_data.append([str(h) for h in final_headers])
            write_data.extend(values)

            # Write data to the worksheet
            if write_data:
                range_name = f"'{worksheet_name}'!A1"
                body = {'values': write_data}
                await asyncio.to_thread(
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption='RAW',
                        body=body
                    ).execute
                )

            # Build worksheet URL
            worksheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={worksheet_id}"

            total_cols = len(write_data[0]) if write_data else 0
            total_rows = len(write_data)  # Total rows including headers

            return UpdateResponse(
                success=True,
                spreadsheet_url=worksheet_url,
                spreadsheet_id=spreadsheet_id,
                worksheet=worksheet_name,
                range=f"A1:{column_index_to_letter(total_cols - 1)}{total_rows}" if write_data else "A1",
                updated_cells=sum(len(row) for row in write_data),
                shape=f"({total_rows},{total_cols})",
                error=None,
                message=f"Successfully created worksheet '{worksheet_name}' and wrote {len(values)} data rows"
            )

        except Exception as e:
            logger.error(f"Error creating worksheet in {uri}: {e}")
            raise Exception(f"Failed to create worksheet in {uri}: {e}") from e

    async def append_rows(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[List[Any]]
    ) -> Dict[str, Any]:
        """
        Append data as new rows below existing data in Google Sheets.

        Implementation of DataTableInterface.append_rows() for Google Sheets.

        Automatically resizes the sheet if the append operation would exceed grid limits.

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

            # Get current grid dimensions from sheet properties
            metadata = await asyncio.to_thread(
                service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute
            )
            sheets = metadata.get('sheets', [])
            current_grid_row_count = 1000  # Default fallback
            current_grid_col_count = 26    # Default fallback

            for sheet in sheets:
                if sheet.get('properties', {}).get('sheetId') == sheet_id:
                    grid_props = sheet.get('properties', {}).get('gridProperties', {})
                    current_grid_row_count = grid_props.get('rowCount', 1000)
                    current_grid_col_count = grid_props.get('columnCount', 26)
                    logger.info(f"Current grid dimensions: {current_grid_row_count} rows x {current_grid_col_count} columns")
                    break

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

            # Check if we need to resize the sheet
            rows_to_append = len(values)
            required_row_count = row_count + rows_to_append
            required_col_count = end_col_index + 1

            need_resize = False
            new_row_count = current_grid_row_count
            new_col_count = current_grid_col_count

            if required_row_count > current_grid_row_count:
                # Add buffer for future operations (1000 rows)
                new_row_count = required_row_count + 1000
                need_resize = True
                logger.info(f"Sheet resize needed: rows {current_grid_row_count} -> {new_row_count}")

            if required_col_count > current_grid_col_count:
                # Add buffer for future operations (10 columns)
                new_col_count = required_col_count + 10
                need_resize = True
                logger.info(f"Sheet resize needed: columns {current_grid_col_count} -> {new_col_count}")

            # Resize the sheet if needed
            if need_resize:
                resize_request = {
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": sheet_id,
                                    "gridProperties": {
                                        "rowCount": new_row_count,
                                        "columnCount": new_col_count
                                    }
                                },
                                "fields": "gridProperties.rowCount,gridProperties.columnCount"
                            }
                        }
                    ]
                }

                logger.info(f"Resizing sheet '{sheet_title}' to {new_row_count} rows x {new_col_count} columns")
                await asyncio.to_thread(
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=resize_request
                    ).execute
                )
                logger.info("Sheet successfully resized")

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

            message = f"Successfully appended rows at {range_address} in worksheet '{sheet_title}'"
            if need_resize:
                message += f" (sheet auto-resized to {new_row_count} rows x {new_col_count} columns)"

            return UpdateResponse(
                success=True,
                spreadsheet_url=spreadsheet_url,
                spreadsheet_id=spreadsheet_id,
                worksheet=sheet_title,
                range=range_address,
                updated_cells=sum(len(row) for row in values),
                shape=f"({len(values)},{len(values[0]) if values else 0})",
                error=None,
                message=message
            )

        except Exception as e:
            logger.error(f"Error appending rows to {uri}: {e}")
            raise Exception(f"Failed to append rows to {uri}: {e}") from e

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
            raise Exception(f"Failed to append columns to {uri}: {e}") from e

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

        Automatically detects if the original URI data has headers and handles updates accordingly:
        - If original data has headers and new data has headers: skips header and updates only data rows
        - If original data has no headers: updates all data including first row

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

            # Load original data to detect if it has headers
            range_name = f"'{sheet_title}'!A:ZZ"
            result = await asyncio.to_thread(
                service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_name
                ).execute
            )
            original_data = result.get('values', [])

            # Detect if original data has headers
            original_has_headers = False
            if original_data:
                detected_headers, _ = auto_detect_headers(original_data)
                original_has_headers = bool(detected_headers)
                logger.info(f"Original data header detection: {original_has_headers}")
                if original_has_headers:
                    logger.info(f"Detected original headers: {detected_headers}")

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

            # If data was list of dicts, include/skip extracted headers based on original data
            # (list of dicts format implies DataFrame-like structure with headers)
            if extracted_headers:
                # Determine if we should skip headers:
                # Skip headers if original data has headers AND new data has headers
                skip_header_for_update = original_has_headers and extracted_headers

                if skip_header_for_update:
                    # Skip headers - only write data rows
                    values = [[str(cell) if cell is not None else "" for cell in row] for row in data_rows]
                    logger.info(f"[Auto-detected] Original data has headers. Skipping extracted headers from list of dicts: {extracted_headers}")
                    logger.info(f"[Auto-detected] Writing {len(data_rows)} data rows only (no header row)")
                else:
                    # Include headers
                    values = [[str(h) for h in extracted_headers]]
                    values.extend([[str(cell) if cell is not None else "" for cell in row] for row in data_rows])
                    logger.info(f"[Auto-detected] Original data has no headers. Including extracted headers from list of dicts: {extracted_headers}")
                    logger.info(f"[Auto-detected] Writing {len(data_rows)} data rows + 1 header row = {len(values)} total rows")
            else:
                # Auto-detect headers in data_rows (already processed)
                detected_headers, processed_rows = auto_detect_headers(data_rows)

                # Determine if we should skip headers:
                # Skip headers if original data has headers AND new data has detected headers
                skip_header_for_update = original_has_headers and detected_headers

                # If headers were detected in new data, decide whether to include them
                if detected_headers and not skip_header_for_update:
                    # Original has NO headers, so include detected headers
                    values = [[str(h) for h in detected_headers]]
                    values.extend([[str(cell) if cell is not None else "" for cell in row] for row in processed_rows])
                    logger.info(f"[Auto-detected] Original data has no headers. Including detected headers in output: {detected_headers}")
                elif detected_headers and skip_header_for_update:
                    # Original has headers, new data has headers - skip header row
                    values = [[str(cell) if cell is not None else "" for cell in row] for row in processed_rows]
                    logger.info(f"[Auto-detected] Original data has headers. Skipping detected headers in new data: {detected_headers}")
                else:
                    # No headers detected in new data - use all data rows as-is
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

            # Log success with spreadsheet URL
            logger.info(f"Successfully updated range {final_range} in worksheet '{sheet_title}'")
            logger.info(f"Spreadsheet URL: {spreadsheet_url}")

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
            raise Exception(f"Failed to update range in {uri}: {e}") from e

    async def update_by_lookup(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[Dict[str, Any]],
        on: str,
        override: bool = False
    ) -> Dict[str, Any]:
        """
        Update Google Sheets data by looking up rows using a key column.

        Similar to SQL UPDATE with JOIN - matches rows by a lookup key and updates
        only the columns present in the new data, preserving other columns.
        New columns are automatically added at the end if they don't exist.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            data: List of dicts containing update data (DataFrame-like)
            on: Column name to use as lookup key (must exist in both sheet and data)
            override: If True, empty/null values in data will clear existing cells;
                     If False, empty/null values will preserve existing values

        Returns:
            UpdateResponse with success status, updated cell count, and metadata

        Behavior:
            - Lookup matching: Case-insensitive
            - Duplicate keys: Updates first match only
            - Unmatched rows: Ignored (skipped silently)
            - New columns: Automatically added at the end
            - Empty values: Clears cell if override=True, preserves if override=False

        Examples:
            # Basic update by username
            await update_by_lookup(
                service, uri,
                data=[
                    {"username": "@user1", "status": "active"},
                    {"username": "@user2", "status": "inactive"}
                ],
                on="username"
            )

            # Update with new columns (automatically added) and override empty values
            await update_by_lookup(
                service, uri,
                data=[{"username": "@user1", "new_col": "value", "old_col": ""}],
                on="username",
                override=True  # Empty "old_col" will clear the existing cell
            )
        """
        try:
            # Validate input
            if not isinstance(data, list) or not data:
                raise ValueError("Invalid data: must be non-empty list of dicts")

            if not all(isinstance(row, dict) for row in data):
                raise ValueError("Invalid data: all items must be dicts")

            # Check if lookup column exists in update data
            if not all(on in row for row in data):
                raise ValueError(f"Lookup column '{on}' not found in all rows of update data")

            # Load existing sheet data
            logger.info(f"Loading existing sheet data from {uri}")
            load_response = await self.load_data_table(service, uri)

            if not load_response.success:
                raise Exception(f"Failed to load sheet for update by lookup: {load_response.error}")

            existing_data = load_response.data  # List of dicts
            existing_headers = list(existing_data[0].keys()) if existing_data else []
            metadata = load_response.source_info
            spreadsheet_id = metadata['spreadsheet_id']
            sheet_title = metadata['worksheet']
            sheet_id = metadata.get('worksheet_url', '').split('gid=')[-1] if 'worksheet_url' in metadata else '0'

            # Validate lookup column exists in sheet
            if on not in existing_headers:
                raise ValueError(
                    f"Lookup column '{on}' not found in sheet. Available columns: {existing_headers}"
                )

            # Build case-insensitive lookup index: {lookup_value.lower(): row_index}
            logger.info(f"Building lookup index on column '{on}' (case-insensitive)")
            lookup_index = {}
            for idx, row in enumerate(existing_data):
                lookup_value = str(row.get(on, "")).lower()
                if lookup_value and lookup_value not in lookup_index:
                    # Store first occurrence only (handle duplicates)
                    lookup_index[lookup_value] = idx

            logger.info(f"Built lookup index with {len(lookup_index)} unique keys")

            # Identify columns in update data
            update_columns = set()
            for row in data:
                update_columns.update(row.keys())

            # Identify new columns (columns in data but not in sheet)
            new_columns = [col for col in update_columns if col not in existing_headers]

            # Determine final headers - automatically add new columns at the end
            final_headers = existing_headers.copy()
            if new_columns:
                final_headers.extend(new_columns)
                logger.info(f"Adding new columns at the end: {new_columns}")

            # Initialize result data with existing data
            # Convert to list of lists for easier manipulation
            result_data = []
            for row in existing_data:
                row_list = [row.get(header, "") for header in final_headers]
                result_data.append(row_list)

            # If new columns were added, ensure all existing rows have placeholders
            if new_columns:
                for row_list in result_data:
                    # Pad rows with empty strings for new columns
                    while len(row_list) < len(final_headers):
                        row_list.append("")

            # Perform lookup and update
            matched_count = 0
            unmatched_count = 0
            updated_cells = 0

            for update_row in data:
                lookup_value = str(update_row.get(on, "")).lower()

                if lookup_value not in lookup_index:
                    unmatched_count += 1
                    continue

                # Get row index in existing data
                row_idx = lookup_index[lookup_value]
                matched_count += 1

                # Update columns in this row
                for col_name, new_value in update_row.items():
                    if col_name not in final_headers:
                        # Column not in final headers (should not happen as we add all columns)
                        continue

                    col_idx = final_headers.index(col_name)

                    # Handle empty/null values based on override flag
                    if new_value is None or new_value == "":
                        if override:
                            # Clear the cell
                            result_data[row_idx][col_idx] = ""
                            updated_cells += 1
                        # else: preserve existing value (do nothing)
                    else:
                        # Update with new value
                        result_data[row_idx][col_idx] = new_value
                        updated_cells += 1

            logger.info(f"Lookup results: {matched_count} matched, {unmatched_count} unmatched")
            logger.info(f"Updated {updated_cells} cells")

            if matched_count == 0:
                logger.warning("No matching rows found")

            # Prepare data for writing back to sheet (headers + data)
            write_data = [final_headers] + result_data

            # Write back to sheet starting from A1
            range_address = "A1"
            response = await self.update_range(service, uri, write_data, range_address)

            # Enhance response message with lookup statistics
            if response.success:
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

                # Create new UpdateResponse with enhanced message
                return UpdateResponse(
                    success=True,
                    spreadsheet_url=spreadsheet_url,
                    spreadsheet_id=response.spreadsheet_id,
                    worksheet=response.worksheet,
                    range=response.range,
                    updated_cells=response.updated_cells,
                    shape=response.shape,
                    error=None,
                    message=(
                        f"Successfully updated by lookup on '{on}': "
                        f"{matched_count} rows matched, {unmatched_count} unmatched, "
                        f"{updated_cells} cells updated"
                    )
                )

            return response

        except Exception as e:
            logger.error(f"Error updating by lookup: {e}")
            raise Exception(f"Failed to update by lookup on '{on}': {e}") from e

    async def list_worksheets(
        self,
        service,  # Authenticated Google Sheets service
        uri: str
    ) -> Dict[str, Any]:
        """
        List all worksheets (sheets/tabs) in a Google Sheets spreadsheet.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI (spreadsheet ID or full URL)

        Returns:
            WorksheetsListResponse containing:
                - success: Whether the operation succeeded
                - spreadsheet_id: The spreadsheet ID
                - spreadsheet_url: Full URL to the spreadsheet
                - spreadsheet_title: The title of the spreadsheet
                - worksheets: List of WorksheetInfo objects
                - total_worksheets: Total number of worksheets
                - error: Error message if failed, None otherwise
                - message: Human-readable result message
        """
        try:
            # Import response model
            from datatable_tools.models import WorksheetsListResponse, WorksheetInfo

            # Parse URI to extract spreadsheet_id
            spreadsheet_id, _ = parse_google_sheets_uri(uri)

            logger.info(f"Listing worksheets for spreadsheet: {spreadsheet_id}")

            # Get spreadsheet metadata
            result = await asyncio.to_thread(
                service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id
                ).execute
            )

            spreadsheet_title = result.get('properties', {}).get('title', 'Untitled')
            sheets = result.get('sheets', [])

            # Extract worksheet information
            worksheets = []
            for sheet in sheets:
                props = sheet.get('properties', {})
                grid_props = props.get('gridProperties', {})
                sheet_id = props.get('sheetId', 0)

                worksheet_info = WorksheetInfo(
                    sheet_id=sheet_id,
                    title=props.get('title', 'Untitled'),
                    index=props.get('index', 0),
                    row_count=grid_props.get('rowCount', 0),
                    column_count=grid_props.get('columnCount', 0),
                    worksheet_url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
                )
                worksheets.append(worksheet_info)

            # Sort by index
            worksheets.sort(key=lambda x: x.index)

            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

            return WorksheetsListResponse(
                success=True,
                spreadsheet_id=spreadsheet_id,
                spreadsheet_url=spreadsheet_url,
                spreadsheet_title=spreadsheet_title,
                worksheets=worksheets,
                total_worksheets=len(worksheets),
                error=None,
                message=f"Found {len(worksheets)} worksheet(s) in '{spreadsheet_title}'"
            )

        except Exception as e:
            logger.error(f"Error listing worksheets: {e}")
            from datatable_tools.models import WorksheetsListResponse
            return WorksheetsListResponse(
                success=False,
                spreadsheet_id="",
                spreadsheet_url="",
                spreadsheet_title="",
                worksheets=[],
                total_worksheets=0,
                error=str(e),
                message=f"Failed to list worksheets: {e}"
            )
