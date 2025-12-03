"""
GoogleSheetDataTable - Google Sheets Implementation of DataTable Interface

This module provides the Google Sheets-specific implementation of the DataTableInterface.
All Google Sheets operations go through this class, which inherits from DataTableInterface.

Stage 4.2: Framework-agnostic implementation with NO FastMCP dependency.
Decorators moved to MCP layer (mcp_tools.py).
"""

from typing import Dict, List, Optional, Any, Union
import logging
import asyncio
import re

from datatable_tools.interfaces.datatable import DataTableInterface
from datatable_tools.models import TableResponse, SpreadsheetResponse, UpdateResponse, ValueRenderOption, ValueInputOption
from datatable_tools.google_sheets_helpers import (
    parse_google_sheets_uri,
    get_sheet_by_gid,
    auto_detect_headers,
    detect_header_row,
    column_index_to_letter,
    column_letter_to_index,
    process_data_input,
    parse_range_address
)

logger = logging.getLogger(__name__)


def align_dict_data_to_headers(data: List[Dict[str, Any]], headers: List[str]) -> List[List[Any]]:
    """
    Align dict data to match existing sheet headers.

    This function reorders dict keys to match the header order and fills missing columns with empty strings.
    Essential for maintaining column alignment when appending data to sheets.
    Uses case-insensitive matching for header names.

    Args:
        data: List of dictionaries with data to align
        headers: List of header names in the order they appear in the sheet

    Returns:
        List of lists with values aligned to header order

    Example:
        >>> headers = ["A", "B", "C"]
        >>> data = [{"C": 3, "A": 1, "B": 2}]  # Different order
        >>> align_dict_data_to_headers(data, headers)
        [[1, 2, 3]]  # Aligned to match headers order
    """
    aligned_rows = []
    for row_dict in data:
        aligned_row = []
        # Build case-insensitive lookup for this row's keys
        row_dict_lower = {k.lower(): v for k, v in row_dict.items()}
        for header in headers:
            # Case-insensitive lookup
            value = row_dict_lower.get(header.lower(), "")
            aligned_row.append(value)
        aligned_rows.append(aligned_row)

    logger.debug(f"Aligned {len(data)} rows to {len(headers)} columns")
    return aligned_rows


class GoogleSheetDataTable(DataTableInterface):
    """
    Google Sheets implementation of the DataTable interface.

    This class implements all DataTable operations for Google Sheets.
    Uses stacked decorators and direct Google Sheets API calls for clean architecture.
    """

    async def load_data_table(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        range_address: Optional[str] = None,
        auto_detect_header_row: bool = True,
        value_render_option: str = 'FORMATTED_VALUE'
    ) -> Dict[str, Any]:
        """
        Load a table from Google Sheets.

        Implementation of DataTableInterface.load_data_table() for Google Sheets.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            range_address: Optional range in A1 notation (e.g., "A2:M1000", "2:1000", "B:Z")
            auto_detect_header_row: Automatically detect header row by analyzing first 5 rows (default: True)
            value_render_option: How values should be represented in the output:
                - 'FORMATTED_VALUE': Values formatted as they appear in the UI (default)
                - 'UNFORMATTED_VALUE': Values with no formatting applied
                - 'FORMULA': Returns formulas for cells with formulas, values for others
        """
        # Parse URI to extract spreadsheet_id and gid
        spreadsheet_id, gid = parse_google_sheets_uri(uri)

        logger.info(f"Loading table from Google Sheets: {spreadsheet_id}, gid={gid}, render_option={value_render_option}")

        # Get sheet properties by gid (or first sheet if no gid)
        sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
        sheet_title = sheet_props['title']
        sheet_id = sheet_props['sheetId']

        # Parse range address to handle worksheet!range format
        range_name, sheet_title, sheet_id = await parse_range_address(
            service, spreadsheet_id, range_address, sheet_title, sheet_id
        )

        # Read data from sheet using Google API directly
        result = await asyncio.to_thread(
            service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueRenderOption=value_render_option
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

        # Process headers and data with smart detection
        headers = []
        data_rows = []

        if all_data:
            if auto_detect_header_row:
                # Use smart header detection (analyzes first 5 rows)
                header_row_idx, headers, data_rows = detect_header_row(all_data)
                logger.info(f"Smart detection: header at row {header_row_idx}, {len(headers)} columns, {len(data_rows)} data rows")
            else:
                # Old behavior: assume row 0 is header
                headers = [str(h) if h is not None else "" for h in all_data[0]] if all_data else []
                data_rows = all_data[1:] if len(all_data) > 1 else []
                logger.info(f"Manual mode: using row 0 as header")

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
            for row_idx, row in enumerate(data_rows):
                row_dict = {}
                for i, header in enumerate(headers):
                    # Use the header as key, and the corresponding cell value
                    row_dict[header] = row[i] if i < len(row) else ""
                data.append(row_dict)
                # Debug: Log types of first row
                if row_idx == 0:
                    logger.debug(f"First row data types: {[(k, type(v).__name__, repr(v)) for k, v in row_dict.items()]}")

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
                valueRenderOption=ValueRenderOption.FORMULA.value
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
            # Convert all headers to strings (in case they are numbers or other types)
            headers = [str(h) if h is not None else "" for h in all_data[0]] if all_data else []
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
            "value_render_option": ValueRenderOption.FORMULA.value
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
                valueRenderOption=ValueRenderOption.FORMULA.value
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
            # Convert all headers to strings (in case they are numbers or other types)
            headers = [str(h) if h is not None else "" for h in all_data[0]] if all_data else []
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
            "value_render_option": ValueRenderOption.FORMULA.value,
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
                    range=range_name,
                    valueRenderOption=ValueRenderOption.FORMATTED_VALUE.value
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
                    valueInputOption=ValueInputOption.USER_ENTERED.value,
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

        Enhanced logic:
        - Reads existing sheet columns first
        - Matches column names (case-insensitive)
        - Skips columns that already exist
        - Only appends new columns that don't exist yet
        - If input is empty DataFrame with columns only: won't duplicate existing headers

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

            # Read existing sheet data to get current column headers
            range_name = f"'{sheet_title}'!A:ZZ"
            result = await asyncio.to_thread(
                service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueRenderOption=ValueRenderOption.FORMATTED_VALUE.value
                ).execute
            )

            all_data = result.get('values', [])

            # Extract existing headers (first row)
            existing_headers = []
            if all_data and len(all_data) > 0:
                existing_headers = [str(h) for h in all_data[0]]

            # Create case-insensitive lookup for existing headers (filter out empty/whitespace-only headers)
            existing_headers_lower = {h.lower(): h for h in existing_headers if h.strip()}

            logger.info(f"Existing headers in sheet: {existing_headers}")

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

            # Special case for append_columns: if we have a single row with no headers detected,
            # treat that row as column headers (not data)
            if not final_headers and len(final_data) == 1 and final_data[0]:
                logger.info(f"Single row detected with no headers - treating as column headers for append_columns")
                final_headers = [str(x) for x in final_data[0]]
                final_data = []

            logger.info(f"Input headers: {final_headers}")
            logger.info(f"Input data rows count: {len(final_data)}")

            # Determine which columns are new (case-insensitive matching)
            new_column_indices = []
            new_column_headers = []
            existing_column_headers = []

            for idx, header in enumerate(final_headers):
                header_lower = str(header).lower()
                if header_lower in existing_headers_lower:
                    # Column already exists
                    existing_column_headers.append(header)
                    logger.info(f"Column '{header}' already exists in sheet (matched with '{existing_headers_lower[header_lower]}')")
                else:
                    # New column
                    new_column_indices.append(idx)
                    new_column_headers.append(header)
                    logger.info(f"Column '{header}' is new and will be appended")

            # Handle different scenarios
            if len(new_column_indices) == 0:
                # No new columns to append
                if len(final_headers) == 0:
                    # No headers provided at all - nothing to do
                    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
                    logger.info(f"No headers provided for append_columns. Skipping.")
                    return UpdateResponse(
                        success=True,
                        spreadsheet_url=spreadsheet_url,
                        spreadsheet_id=spreadsheet_id,
                        worksheet=sheet_title,
                        range="N/A",
                        updated_cells=0,
                        shape="(0,0)",
                        error=None,
                        message=f"No headers provided for append_columns. Nothing to append."
                    )
                elif len(final_data) == 0:
                    # Headers provided but all already exist, no data - skip append
                    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
                    logger.info(f"All columns already exist and input is empty DataFrame. Skipping append.")
                    return UpdateResponse(
                        success=True,
                        spreadsheet_url=spreadsheet_url,
                        spreadsheet_id=spreadsheet_id,
                        worksheet=sheet_title,
                        range="N/A",
                        updated_cells=0,
                        shape="(0,0)",
                        error=None,
                        message=f"All columns already exist in worksheet '{sheet_title}'. No append needed. Existing columns: {existing_column_headers}"
                    )
                else:
                    # Has data rows but all columns exist - this is ambiguous, skip for now
                    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
                    logger.warning(f"All columns already exist but input has {len(final_data)} data rows. Skipping append to avoid confusion.")
                    return UpdateResponse(
                        success=True,
                        spreadsheet_url=spreadsheet_url,
                        spreadsheet_id=spreadsheet_id,
                        worksheet=sheet_title,
                        range="N/A",
                        updated_cells=0,
                        shape="(0,0)",
                        error=None,
                        message=f"All columns already exist in worksheet '{sheet_title}'. Cannot append data without new columns. Existing columns: {existing_column_headers}"
                    )

            # Filter data to only include new columns
            filtered_data = []
            for row in final_data:
                filtered_row = [row[idx] if idx < len(row) else "" for idx in new_column_indices]
                filtered_data.append(filtered_row)

            # Convert to strings for Google Sheets API
            values_only = [[str(cell) if cell is not None else "" for cell in row] for row in filtered_data]

            # Prepare write data with headers (only new columns)
            values = []
            if new_column_headers:
                values.append([str(h) for h in new_column_headers])
            values.extend(values_only)

            if not values:
                values = [[""]]

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
                    valueInputOption=ValueInputOption.USER_ENTERED.value,
                    body=body
                ).execute
            )

            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

            message_parts = [f"Successfully appended {len(new_column_headers)} new column(s) at {range_address} in worksheet '{sheet_title}'"]
            if existing_column_headers:
                message_parts.append(f"Skipped {len(existing_column_headers)} existing column(s): {existing_column_headers}")

            return UpdateResponse(
                success=True,
                spreadsheet_url=spreadsheet_url,
                spreadsheet_id=spreadsheet_id,
                worksheet=sheet_title,
                range=range_address,
                updated_cells=sum(len(row) for row in values),
                shape=f"({len(values)},{len(values[0]) if values else 0})",
                error=None,
                message=". ".join(message_parts)
            )

        except Exception as e:
            logger.error(f"Error appending columns to {uri}: {e}")
            raise Exception(f"Failed to append columns to {uri}: {e}") from e

    async def update_range(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[List[Any]],
        range_address: Optional[str] = None,
        value_input_option: str = 'USER_ENTERED'
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
            value_input_option: How input data should be interpreted:
                - 'RAW': Values are stored as-is (literal text, no parsing)
                - 'USER_ENTERED': Values are parsed as if typed by user (formulas, numbers, dates parsed)
                Default is 'RAW' for backwards compatibility.
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
                    range=range_name,
                    valueRenderOption=ValueRenderOption.FORMATTED_VALUE.value
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
            # This returns the basic parsed range, we'll auto-expand it next
            _, parsed_sheet_title, parsed_sheet_id = await parse_range_address(
                service, spreadsheet_id, range_address, sheet_title, sheet_id
            )

            # Extract just the range part (without sheet name) for auto-expansion
            final_range = range_address
            if range_address and '!' in range_address:
                _, final_range = range_address.split('!', 1)

            # Update sheet_title and sheet_id with parsed values
            sheet_title = parsed_sheet_title
            sheet_id = parsed_sheet_id

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

            # Batch processing for large datasets to avoid API limits
            # Google Sheets API: 2MB recommended max, 180s timeout per request
            # For large datasets (>2000 rows), split into batches to stay under limits
            BATCH_SIZE = 2000  # rows per batch (conservative for safety)
            total_rows = len(values)

            if total_rows > BATCH_SIZE:
                logger.info(f"Large dataset detected ({total_rows} rows). Using batch processing with batch size {BATCH_SIZE}")

                # Parse the start cell from final_range
                match = re.match(r'^([A-Z]+)(\d+)(?::.*)?$', final_range)
                if not match:
                    raise ValueError(f"Invalid range format for batch processing: {final_range}")

                start_col = match.group(1)
                start_row = int(match.group(2))

                # Calculate end column based on data width
                cols = max(len(row) for row in values) if values else 0
                start_col_index = column_letter_to_index(start_col)
                end_col_index = start_col_index + cols - 1
                end_col = column_index_to_letter(end_col_index)

                # Process in batches
                for batch_idx in range(0, total_rows, BATCH_SIZE):
                    batch_end_idx = min(batch_idx + BATCH_SIZE, total_rows)
                    batch_values = values[batch_idx:batch_end_idx]
                    batch_rows = len(batch_values)

                    # Calculate batch range
                    batch_start_row = start_row + batch_idx
                    batch_end_row = batch_start_row + batch_rows - 1
                    batch_range = f"'{sheet_title}'!{start_col}{batch_start_row}:{end_col}{batch_end_row}"

                    logger.info(f"Processing batch {batch_idx//BATCH_SIZE + 1}/{(total_rows-1)//BATCH_SIZE + 1}: rows {batch_start_row}-{batch_end_row} ({batch_rows} rows)")

                    # Update batch
                    body = {'values': batch_values}
                    await asyncio.to_thread(
                        service.spreadsheets().values().update(
                            spreadsheetId=spreadsheet_id,
                            range=batch_range,
                            valueInputOption=value_input_option,
                            body=body
                        ).execute
                    )

                logger.info(f"Batch processing completed: {total_rows} rows updated in {(total_rows-1)//BATCH_SIZE + 1} batches")
            else:
                # Small dataset - single API call
                # Use value_input_option parameter to control how data is interpreted:
                # - RAW: literal text (default for backwards compatibility)
                # - USER_ENTERED: parses formulas, numbers, dates, etc.
                body = {'values': values}
                await asyncio.to_thread(
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=full_range,
                        valueInputOption=value_input_option,
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

    async def insert_image_in_cell(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        image_url: str,
        cell_address: str,
        width_pixels: int = 400,
        height_pixels: int = 300
    ) -> Dict[str, Any]:
        """
        Insert an image into a cell using IMAGE formula (mode 4) and auto-resize the cell.

        This function:
        1. Inserts =IMAGE(url, 4, height, width) formula into the specified cell
        2. Automatically resizes the row height and column width to fit the image

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            image_url: Public URL of the image (must be accessible without authentication)
            cell_address: Cell address in A1 notation (e.g., "A1", "B5", "C10")
            width_pixels: Image and cell width in pixels (default: 400)
            height_pixels: Image and cell height in pixels (default: 300)

        Returns:
            Dict with success status and details

        Example:
            # Insert 600x400 image in cell B5
            insert_image_in_cell(service, uri,
                                "https://example.com/image.jpg",
                                "B5", width_pixels=600, height_pixels=400)
        """
        try:
            # Parse URI to extract spreadsheet_id and gid
            spreadsheet_id, gid = parse_google_sheets_uri(uri)

            # Get sheet properties by gid
            sheet_props = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_id = sheet_props['sheetId']
            sheet_title = sheet_props['title']

            # Parse cell address to get row and column indices
            import re
            match = re.match(r'^([A-Z]+)(\d+)$', cell_address.upper())
            if not match:
                raise ValueError(f"Invalid cell address: {cell_address}. Expected format like 'A1', 'B5', etc.")

            col_letter = match.group(1)
            row_number = int(match.group(2))

            # Convert to 0-indexed
            from datatable_tools.google_sheets_helpers import column_letter_to_index
            col_index = column_letter_to_index(col_letter)
            row_index = row_number - 1  # Convert to 0-indexed

            # Step 1: Insert IMAGE formula with mode 4 (custom size)
            image_formula = f'=IMAGE("{image_url}", 4, {height_pixels}, {width_pixels})'
            logger.info(f"Inserting IMAGE formula: {image_formula} into cell {cell_address}")

            # Use update_range to insert the formula
            range_name = f"'{sheet_title}'!{cell_address}"
            body = {'values': [[image_formula]]}
            await asyncio.to_thread(
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',  # Parse as formula
                    body=body
                ).execute
            )

            # Step 2: Resize the row height and column width
            logger.info(f"Resizing cell: row {row_index} to {height_pixels}px, column {col_index} to {width_pixels}px")

            resize_requests = [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_index,
                            "endIndex": row_index + 1
                        },
                        "properties": {
                            "pixelSize": height_pixels
                        },
                        "fields": "pixelSize"
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_index,
                            "endIndex": col_index + 1
                        },
                        "properties": {
                            "pixelSize": width_pixels
                        },
                        "fields": "pixelSize"
                    }
                }
            ]

            # Execute batch update for resizing
            resize_body = {"requests": resize_requests}
            await asyncio.to_thread(
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=resize_body
                ).execute
            )

            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

            logger.info(f"Successfully inserted image and resized cell {cell_address}")
            logger.info(f"Spreadsheet URL: {spreadsheet_url}")

            return UpdateResponse(
                success=True,
                spreadsheet_url=spreadsheet_url,
                spreadsheet_id=spreadsheet_id,
                worksheet=sheet_title,
                range=cell_address,
                updated_cells=1,
                shape=f"{width_pixels}x{height_pixels}px",
                error=None,
                message=f"Successfully inserted {width_pixels}x{height_pixels}px image in cell {cell_address} with auto-resized dimensions"
            )

        except Exception as e:
            logger.error(f"Error inserting image in {uri}: {e}")
            raise Exception(f"Failed to insert image in {uri}: {e}") from e

    async def update_by_lookup(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        data: List[Dict[str, Any]],
        on: Union[str, List[str]],
        override: bool = False
    ) -> Dict[str, Any]:
        """
        Update Google Sheets data by looking up rows using one or more key columns.

        Similar to SQL UPDATE with JOIN - matches rows by lookup key(s) and updates
        only the columns present in the new data, preserving other columns.
        New columns are automatically added at the end if they don't exist.

        Supports both single and composite (multiple) lookup keys for flexible matching.

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            data: List of dicts containing update data (DataFrame-like)
            on: Column name(s) to use as lookup key. Can be:
                - Single string: "username"
                - List of strings: ["first_name", "last_name"]
                All specified columns must exist in both sheet and data.
            override: If True, empty/null values in data will clear existing cells;
                     If False, empty/null values will preserve existing values

        Returns:
            UpdateResponse with success status, updated cell count, and metadata

        Behavior:
            - Lookup matching: Case-insensitive
            - Multiple keys: ALL must match (AND logic)
            - Duplicate keys: Updates all matching rows
            - Unmatched rows: Ignored (skipped silently)
            - New columns: Automatically added at the end
            - Empty values: Clears cell if override=True, preserves if override=False

        Examples:
            # Basic update by single key
            await update_by_lookup(
                service, uri,
                data=[
                    {"username": "@user1", "status": "active"},
                    {"username": "@user2", "status": "inactive"}
                ],
                on="username"
            )

            # Update by composite key (multiple columns)
            await update_by_lookup(
                service, uri,
                data=[
                    {"first_name": "John", "last_name": "Doe", "status": "active"},
                    {"first_name": "Jane", "last_name": "Smith", "status": "inactive"}
                ],
                on=["first_name", "last_name"]
            )

            # Update with new columns and override empty values
            await update_by_lookup(
                service, uri,
                data=[{"username": "@user1", "new_col": "value", "old_col": ""}],
                on="username",
                override=True  # Empty "old_col" will clear the existing cell
            )
        """
        try:
            # Normalize 'on' to always be a list for consistent handling
            lookup_keys = [on] if isinstance(on, str) else on

            # Validate input
            if not isinstance(data, list) or not data:
                raise ValueError("Invalid data: must be non-empty list of dicts")

            if not all(isinstance(row, dict) for row in data):
                raise ValueError("Invalid data: all items must be dicts")

            # Check if all lookup columns exist in update data
            for key in lookup_keys:
                if not all(key in row for row in data):
                    raise ValueError(f"Lookup column '{key}' not found in all rows of update data")

            # Load existing sheet data with FORMATTED_VALUE to ensure dates/numbers match user input format
            # This prevents lookup mismatches where dates appear as serial numbers (45987) vs formatted strings (2025-11-26)
            logger.info(f"Loading existing sheet data from {uri} with FORMATTED_VALUE render option")
            load_response = await self.load_data_table(service, uri, value_render_option='FORMATTED_VALUE')

            if not load_response.success:
                raise Exception(f"Failed to load sheet for update by lookup: {load_response.error}")

            existing_data = load_response.data  # List of dicts

            # Special handling for empty sheet
            if not existing_data:
                # Check if sheet is completely empty (no rows at all) or has only headers
                row_count = load_response.source_info.get('row_count', 0)
                is_completely_empty = row_count == 0

                if is_completely_empty:
                    # Sheet is completely empty (no headers, no data rows)
                    # Use update_range to write headers + data from A1
                    logger.info(
                        f"Sheet is completely empty (no headers, no data). "
                        f"Writing headers and data from A1. "
                        f"Sheet info: {load_response.source_info.get('worksheet', 'unknown')}"
                    )

                    # Convert list of dicts to headers + rows format
                    if data and isinstance(data[0], dict):
                        headers = list(data[0].keys())
                        data_rows = [[row.get(h, "") for h in headers] for row in data]
                        write_data = [headers] + data_rows
                    else:
                        raise ValueError("Data must be a list of dicts for update_by_lookup")

                    # Write headers + data using update_range
                    response = await self.update_range(service, uri, write_data, "A1")

                    # Enhance response message
                    if response.success:
                        metadata = load_response.source_info
                        spreadsheet_id = metadata['spreadsheet_id']
                        sheet_id = metadata.get('worksheet_url', '').split('gid=')[-1] if 'worksheet_url' in metadata else '0'
                        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

                        keys_display = str(on) if isinstance(on, list) else on

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
                                f"Sheet was completely empty. Wrote headers and {len(data)} rows as new data. "
                                f"Lookup column: {keys_display}"
                            )
                        )

                    return response
                else:
                    # Sheet has headers but no data rows - append all incoming data
                    logger.info(
                        f"Sheet has only headers (no data rows). "
                        f"Converting update_by_lookup to append operation. "
                        f"Sheet info: {load_response.source_info.get('worksheet', 'unknown')}, "
                        f"Row count: {row_count}, "
                        f"Col count: {load_response.source_info.get('column_count', 0)}"
                    )

                    # Read headers from first row of sheet to align data
                    metadata = load_response.source_info
                    spreadsheet_id = metadata['spreadsheet_id']
                    sheet_title = metadata['worksheet']

                    # Read first row to get headers
                    range_name = f"'{sheet_title}'!1:1"
                    header_result = await asyncio.to_thread(
                        service.spreadsheets().values().get(
                            spreadsheetId=spreadsheet_id,
                            range=range_name,
                            valueRenderOption=ValueRenderOption.FORMATTED_VALUE.value
                        ).execute
                    )
                    header_row = header_result.get('values', [[]])[0] if header_result.get('values') else []
                    existing_headers = [str(h) if h is not None else "" for h in header_row]

                    logger.info(f"Read {len(existing_headers)} headers from sheet: {existing_headers[:10]}")

                    # Align incoming dict data to match existing headers
                    aligned_data = align_dict_data_to_headers(data, existing_headers)

                    # Use append_rows to add aligned data
                    response = await self.append_rows(service, uri, aligned_data)

                    # Enhance response message to indicate fallback behavior
                    if response.success:
                        metadata = load_response.source_info
                        spreadsheet_id = metadata['spreadsheet_id']
                        sheet_id = metadata.get('worksheet_url', '').split('gid=')[-1] if 'worksheet_url' in metadata else '0'
                        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

                        # Format lookup keys display
                        keys_display = str(on) if isinstance(on, list) else on

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
                                f"Sheet had only headers. Appended {len(data)} rows as new data. "
                                f"Lookup column: {keys_display}"
                            )
                        )

                    return response

            existing_headers = list(existing_data[0].keys())
            metadata = load_response.source_info
            spreadsheet_id = metadata['spreadsheet_id']
            sheet_title = metadata['worksheet']
            sheet_id = metadata.get('worksheet_url', '').split('gid=')[-1] if 'worksheet_url' in metadata else '0'

            # Build case-insensitive header lookup: lowercase -> original case
            headers_lower_map = {h.lower(): h for h in existing_headers}

            # Validate all lookup columns exist in sheet (case-insensitive)
            missing_keys = [key for key in lookup_keys if key.lower() not in headers_lower_map]
            if missing_keys:
                raise ValueError(
                    f"Lookup column(s) {missing_keys} not found in sheet. Available columns: {existing_headers}"
                )

            # Normalize lookup_keys to match existing header case
            lookup_keys = [headers_lower_map.get(key.lower(), key) for key in lookup_keys]

            # Build case-insensitive lookup index for composite keys
            # Format: {(key1_value.lower(), key2_value.lower(), ...): [row_indices]}
            logger.info(f"Building lookup index on column(s) {lookup_keys} (case-insensitive)")
            lookup_index = {}
            for idx, row in enumerate(existing_data):
                # Create composite key tuple from all lookup columns
                lookup_tuple = tuple(str(row.get(key, "")).lower() for key in lookup_keys)

                # Skip rows where any lookup key is empty
                if all(val for val in lookup_tuple):
                    # Store list of row indices for each unique composite key (supports duplicates)
                    if lookup_tuple not in lookup_index:
                        lookup_index[lookup_tuple] = []
                    lookup_index[lookup_tuple].append(idx)

            logger.info(f"Built lookup index with {len(lookup_index)} unique key combinations")

            # Identify columns in update data
            update_columns = set()
            for row in data:
                update_columns.update(row.keys())

            # Identify new columns (columns in data but not in sheet) - case-insensitive
            new_columns = [col for col in update_columns if col.lower() not in headers_lower_map]

            # Determine final headers - automatically add new columns at the end
            final_headers = existing_headers.copy()
            if new_columns:
                final_headers.extend(new_columns)
                # Update headers_lower_map with new columns
                for col in new_columns:
                    headers_lower_map[col.lower()] = col
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

            # Perform lookup and update with composite key support
            matched_count = 0
            matched_rows = 0
            unmatched_count = 0
            updated_cells = 0
            unmatched_rows = []  # Collect unmatched rows to append later

            for update_row in data:
                # Create composite lookup tuple from the update row
                lookup_tuple = tuple(str(update_row.get(key, "")).lower() for key in lookup_keys)

                if lookup_tuple not in lookup_index:
                    unmatched_count += 1
                    # Collect unmatched row for appending
                    unmatched_rows.append(update_row)
                    continue

                # Get all row indices that match this composite key
                row_indices = lookup_index[lookup_tuple]
                matched_count += 1
                matched_rows += len(row_indices)

                # Update all matching rows
                for row_idx in row_indices:
                    # Update columns in this row
                    for col_name, new_value in update_row.items():
                        # Case-insensitive column lookup
                        matched_header = headers_lower_map.get(col_name.lower())
                        if matched_header is None or matched_header not in final_headers:
                            # Column not in final headers (should not happen as we add all columns)
                            continue

                        col_idx = final_headers.index(matched_header)

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

            logger.info(f"Lookup results: {matched_count} lookup keys matched {matched_rows} rows, {unmatched_count} unmatched")
            logger.info(f"Updated {updated_cells} cells")

            if matched_count == 0:
                logger.warning("No matching rows found")

            # Filter out completely empty rows from result_data to avoid gaps
            # A row is considered empty if all its values are empty strings or None
            filtered_result_data = []
            removed_empty_rows = 0
            for row_list in result_data:
                is_empty = all(not cell or cell == "" for cell in row_list)
                if not is_empty:
                    filtered_result_data.append(row_list)
                else:
                    removed_empty_rows += 1
                    logger.debug(f"Removing empty row from result_data: {row_list}")

            if removed_empty_rows > 0:
                logger.info(f"Removed {removed_empty_rows} empty row(s) to prevent gaps in output")

            # Write updated data rows back to sheet (starting from A2 to preserve header formulas)
            # Note: We only write data rows, not headers, to avoid overwriting formula headers
            range_address = "A2"
            response = await self.update_range(service, uri, filtered_result_data, range_address, value_input_option='USER_ENTERED')

            # Append unmatched rows as new data if any
            appended_count = 0
            if unmatched_rows:
                logger.info(f"Appending {len(unmatched_rows)} unmatched rows as new data")

                # Align unmatched rows to match existing sheet headers
                aligned_unmatched_rows = align_dict_data_to_headers(unmatched_rows, existing_headers)

                append_response = await self.append_rows(service, uri, aligned_unmatched_rows)
                if append_response.success:
                    appended_count = len(unmatched_rows)
                    logger.info(f"Successfully appended {appended_count} unmatched rows")
                else:
                    logger.warning(f"Failed to append unmatched rows: {append_response.error}")

            # Enhance response message with lookup statistics
            if response.success:
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"

                # Format lookup keys display
                keys_display = str(lookup_keys) if len(lookup_keys) > 1 else lookup_keys[0]

                # Build message with appended rows info if applicable
                message_parts = [
                    f"Successfully updated by lookup on {keys_display}: "
                    f"{matched_count} unique lookup keys matched {matched_rows} rows, "
                    f"{unmatched_count} unmatched, {updated_cells} cells updated"
                ]
                if appended_count > 0:
                    message_parts.append(f", {appended_count} new rows appended")

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
                    message="".join(message_parts)
                )

            return response

        except Exception as e:
            logger.error(f"Error updating by lookup: {e}")
            # Format lookup keys display for error message
            keys_display = str(on) if isinstance(on, list) else on
            raise Exception(f"Failed to update by lookup on {keys_display}: {e}") from e

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

    async def copy_range_with_formulas(
        self,
        service,  # Authenticated Google Sheets service
        uri: str,
        from_range: str,
        to_range: Optional[str] = None,
        auto_fill: bool = False,
        lookup_column: str = "A",
        skip_if_exists: bool = True,
        value_input_option: str = "USER_ENTERED"
    ) -> UpdateResponse:
        """
        Copy a range with formulas, adapting cell references based on position change.

        Supports three modes:
        1. Manual mode - single range: Copy from_range to specific to_range (e.g., B2:K2 → B3:K3)
        2. Manual mode - multi-row: Copy single source row to multiple destination rows (e.g., B2:K2 → B3:K10)
        3. Auto-fill mode: Automatically copy formulas down to all data rows

        Args:
            service: Authenticated Google Sheets API service object
            uri: Google Sheets URI
            from_range: Source range in A1 notation (e.g., "B2:K2", "L1:L100")
            to_range: Destination range in A1 notation:
                     - Single row: "B3:K3" (copies to one row)
                     - Multi-row: "B3:K10" (copies to rows 3-10, must have single source row)
                     - Required for manual mode, ignored for auto_fill
            auto_fill: If True, automatically fills down to all rows with data in lookup_column
            lookup_column: Column to check for data when auto_fill=True (default: "A")
            skip_if_exists: If True, skips rows where first destination cell has value (default: True)
            value_input_option: How to interpret data (default: "USER_ENTERED" to parse formulas)

        Returns:
            UpdateResponse with success status and details

        Examples:
            # Auto-fill mode - copies formulas down to all data rows
            copy_range_with_formulas(service, uri, "B2:K2", auto_fill=True)

            # Manual mode - copy row 5 to row 6
            copy_range_with_formulas(service, uri, "B5:Z5", "B6:Z6")

            # Manual mode - copy row 2 to rows 3-10 (multi-row destination)
            copy_range_with_formulas(service, uri, "B2:Z2", "B3:Z10")
        """
        from datatable_tools.formula_adapter import adapt_formula

        try:
            # Parse URI
            spreadsheet_id, gid = parse_google_sheets_uri(uri)
            sheet_properties = await get_sheet_by_gid(service, spreadsheet_id, gid)
            sheet_title = sheet_properties['title']

            # Validate parameters
            if not auto_fill and not to_range:
                raise ValueError("to_range is required when auto_fill=False")

            # Parse from_range
            from_range_parsed = self._parse_simple_range_address(from_range)

            # Calculate source dimensions early for validation
            from_rows = from_range_parsed['end_row'] - from_range_parsed['start_row'] + 1
            from_cols = from_range_parsed['end_col_idx'] - from_range_parsed['start_col_idx'] + 1

            # Auto-fill mode: determine target ranges
            if auto_fill:
                target_ranges = await self._get_autofill_target_ranges(
                    service, spreadsheet_id, sheet_title, from_range_parsed,
                    lookup_column, skip_if_exists
                )

                if not target_ranges:
                    return UpdateResponse(
                        success=True,
                        spreadsheet_url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_properties['sheetId']}",
                        spreadsheet_id=spreadsheet_id,
                        worksheet=sheet_title,
                        range=from_range,
                        updated_cells=0,
                        shape="(0,0)",
                        error=None,
                        message=f"No rows to auto-fill. All rows in {lookup_column} either empty or already have formulas (skip_if_exists={skip_if_exists})."
                    )
            else:
                # Manual mode: handle single-row-to-multi-row or single-range-to-single-range
                to_range_parsed = self._parse_simple_range_address(to_range)
                to_rows = to_range_parsed['end_row'] - to_range_parsed['start_row'] + 1
                to_cols = to_range_parsed['end_col_idx'] - to_range_parsed['start_col_idx'] + 1

                # Check if this is a single-source-row to multi-destination-rows scenario
                if from_rows == 1 and to_rows > 1 and from_cols == to_cols:
                    # Multi-row destination: generate individual target ranges for each row
                    logger.info(f"Multi-row copy mode: copying 1 source row to {to_rows} destination rows")
                    target_ranges = []
                    start_col = column_index_to_letter(to_range_parsed['start_col_idx'])
                    end_col = column_index_to_letter(to_range_parsed['end_col_idx'])

                    # Batch read all first cells at once for performance
                    existing_values = {}
                    if skip_if_exists or lookup_column:
                        # Read entire first column of destination range in one API call
                        batch_check_range = f"'{sheet_title}'!{start_col}{to_range_parsed['start_row']}:{start_col}{to_range_parsed['end_row']}"
                        try:
                            check_result = await asyncio.to_thread(
                                service.spreadsheets().values().get(
                                    spreadsheetId=spreadsheet_id,
                                    range=batch_check_range,
                                    valueRenderOption=ValueRenderOption.UNFORMATTED_VALUE.value
                                ).execute
                            )
                            check_values = check_result.get('values', [])
                            # Map row numbers to their values
                            for idx, row_values in enumerate(check_values):
                                dest_row = to_range_parsed['start_row'] + idx
                                # Check if first cell has value
                                has_value = bool(row_values and row_values[0])
                                existing_values[dest_row] = has_value
                            logger.info(f"Batch checked {len(existing_values)} destination rows in one API call")
                        except Exception as check_error:
                            logger.warning(f"Could not batch check destination cells: {check_error}. Proceeding without skip check.")

                    # Now build target_ranges based on batch results
                    for dest_row in range(to_range_parsed['start_row'], to_range_parsed['end_row'] + 1):
                        should_skip = False
                        if skip_if_exists:
                            # Check if this row has existing data from batch read
                            if existing_values.get(dest_row, False):
                                should_skip = True
                                logger.info(f"Skipping row {dest_row}: destination cell {start_col}{dest_row} already has value")

                        if not should_skip:
                            target_ranges.append(f"{start_col}{dest_row}:{end_col}{dest_row}")

                    if not target_ranges:
                        return UpdateResponse(
                            success=True,
                            spreadsheet_url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_properties['sheetId']}",
                            spreadsheet_id=spreadsheet_id,
                            worksheet=sheet_title,
                            range=to_range,
                            updated_cells=0,
                            shape="(0,0)",
                            error=None,
                            message=f"No rows to copy. All destination rows already have data (skip_if_exists={skip_if_exists})."
                        )
                else:
                    # Single target range (traditional behavior)
                    target_ranges = [to_range]

            # Process each target range
            total_updated_cells = 0
            all_updated_ranges = []

            # Read source range with formulas once
            full_from_range = f"'{sheet_title}'!{from_range}"
            result = await asyncio.to_thread(
                service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=full_from_range,
                    valueRenderOption=ValueRenderOption.FORMULA.value
                ).execute
            )

            source_values = result.get('values', [])
            if not source_values:
                raise ValueError(f"Source range {from_range} is empty")

            # Note: from_rows and from_cols already calculated above

            # PERFORMANCE OPTIMIZATION: For large batch operations (>100 ranges),
            # don't use copyPaste API as it's too slow and times out.
            # Instead, use optimized Python adaptation with batchUpdate values API.
            USE_OPTIMIZED_BATCH = len(target_ranges) > 100

            if USE_OPTIMIZED_BATCH:
                logger.info(f"Using optimized batch processing for {len(target_ranges)} target ranges")

                # Pre-adapt the source formulas once for each unique row offset
                # This avoids re-parsing the same formulas 1000+ times
                batch_data = []

                # Process in chunks to avoid building giant data structures in memory
                CHUNK_SIZE = 500
                for chunk_start in range(0, len(target_ranges), CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE, len(target_ranges))
                    chunk_ranges = target_ranges[chunk_start:chunk_end]

                    logger.info(f"Processing ranges {chunk_start+1}-{chunk_end} of {len(target_ranges)}")

                    for target_range in chunk_ranges:
                        to_range_parsed = self._parse_simple_range_address(target_range)

                        # Calculate offsets
                        row_offset = to_range_parsed['start_row'] - from_range_parsed['start_row']
                        col_offset = to_range_parsed['start_col_idx'] - from_range_parsed['start_col_idx']

                        # Adapt formulas for this target
                        adapted_values = []
                        for row in source_values:
                            adapted_row = []
                            for cell_value in row:
                                if cell_value and isinstance(cell_value, str) and cell_value.startswith('='):
                                    try:
                                        adapted_row.append(adapt_formula(cell_value, row_offset, col_offset))
                                    except Exception:
                                        adapted_row.append(cell_value)  # Keep original on error
                                else:
                                    adapted_row.append(cell_value)
                            adapted_values.append(adapted_row)

                        full_to_range = f"'{sheet_title}'!{target_range}"
                        batch_data.append({
                            'range': full_to_range,
                            'values': adapted_values
                        })

                    # Write this chunk immediately to avoid memory issues and provide progress
                    if batch_data:
                        logger.info(f"Writing chunk of {len(batch_data)} ranges to Google Sheets")
                        await asyncio.to_thread(
                            service.spreadsheets().values().batchUpdate(
                                spreadsheetId=spreadsheet_id,
                                body={
                                    'valueInputOption': value_input_option,
                                    'data': batch_data
                                }
                            ).execute
                        )
                        total_updated_cells += len(batch_data) * from_rows * from_cols
                        batch_data = []  # Clear for next chunk

                all_updated_ranges = target_ranges
                logger.info(f"Optimized batch completed: {total_updated_cells} cells updated")

            else:
                # Original Python-based formula adaptation for small batches (<100 ranges)
                logger.info(f"Using Python formula adaptation for {len(target_ranges)} target ranges")

                # Collect all adapted values for batch write
                batch_data = []
                max_row_needed = 0
                max_col_needed = 0

                for target_range in target_ranges:
                    to_range_parsed = self._parse_simple_range_address(target_range)

                    # Calculate dimensions
                    to_rows = to_range_parsed['end_row'] - to_range_parsed['start_row'] + 1
                    to_cols = to_range_parsed['end_col_idx'] - to_range_parsed['start_col_idx'] + 1

                    # Validate dimensions match
                    # For single-row source (from_rows=1), we allow any number of destination rows
                    # because we're copying the same row multiple times with formula adaptation
                    if from_rows == 1:
                        # Single source row: only validate columns match
                        if from_cols != to_cols:
                            raise ValueError(
                                f"Source range columns ({from_cols}) must match "
                                f"destination range columns ({to_cols})"
                            )
                    else:
                        # Multi-row source: both rows and columns must match
                        if from_rows != to_rows or from_cols != to_cols:
                            raise ValueError(
                                f"Source range dimensions ({from_rows}x{from_cols}) must match "
                                f"destination range dimensions ({to_rows}x{to_cols})"
                            )

                    # Calculate offsets
                    row_offset = to_range_parsed['start_row'] - from_range_parsed['start_row']
                    col_offset = to_range_parsed['start_col_idx'] - from_range_parsed['start_col_idx']

                    # Adapt formulas for each cell with error tracking
                    adapted_values = []
                    formula_errors = []

                    for row_idx, row in enumerate(source_values):
                        adapted_row = []
                        for col_idx, cell_value in enumerate(row):
                            if cell_value and isinstance(cell_value, str) and cell_value.startswith('='):
                                # This is a formula, adapt it
                                try:
                                    adapted_formula = adapt_formula(cell_value, row_offset, col_offset)
                                    adapted_row.append(adapted_formula)
                                except Exception as formula_error:
                                    # Calculate the actual cell address for error reporting
                                    source_col = column_index_to_letter(from_range_parsed['start_col_idx'] + col_idx)
                                    source_row = from_range_parsed['start_row'] + row_idx
                                    dest_col = column_index_to_letter(to_range_parsed['start_col_idx'] + col_idx)
                                    dest_row = to_range_parsed['start_row'] + row_idx

                                    error_info = {
                                        'source_cell': f"{source_col}{source_row}",
                                        'dest_cell': f"{dest_col}{dest_row}",
                                        'formula': cell_value,
                                        'error': str(formula_error)
                                    }
                                    formula_errors.append(error_info)

                                    # Still append the original formula to continue processing
                                    adapted_row.append(cell_value)
                                    logger.warning(f"Formula adaptation error at {source_col}{source_row}: {formula_error}")
                            else:
                                # Regular value, keep as-is
                                adapted_row.append(cell_value)
                        adapted_values.append(adapted_row)

                    # If there were formula errors, raise with details
                    if formula_errors:
                        error_details = "; ".join([
                            f"{err['source_cell']}->{err['dest_cell']}: {err['error']} (formula: {err['formula'][:50]}{'...' if len(err['formula']) > 50 else ''})"
                            for err in formula_errors[:5]  # Show first 5 errors
                        ])
                        if len(formula_errors) > 5:
                            error_details += f"; ... and {len(formula_errors) - 5} more errors"
                        raise ValueError(f"Formula adaptation errors in {len(formula_errors)} cell(s): {error_details}")

                    # Track max dimensions needed
                    max_row_needed = max(max_row_needed, to_range_parsed['end_row'])
                    max_col_needed = max(max_col_needed, to_range_parsed['end_col_idx'] + 1)

                    # Add to batch data
                    full_to_range = f"'{sheet_title}'!{target_range}"
                    batch_data.append({
                        'range': full_to_range,
                        'values': adapted_values
                    })

                # Check if we need to expand grid (only once for all ranges)
                grid_rows = sheet_properties.get('gridProperties', {}).get('rowCount', 1000)
                grid_cols = sheet_properties.get('gridProperties', {}).get('columnCount', 26)

                if max_row_needed > grid_rows or max_col_needed > grid_cols:
                    # Expand grid
                    new_rows = max(grid_rows, max_row_needed)
                    new_cols = max(grid_cols, max_col_needed)

                    logger.info(f"Expanding grid to {new_rows} rows x {new_cols} columns")

                    await asyncio.to_thread(
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={
                                'requests': [{
                                    'updateSheetProperties': {
                                        'properties': {
                                            'sheetId': sheet_properties['sheetId'],
                                            'gridProperties': {
                                                'rowCount': new_rows,
                                                'columnCount': new_cols
                                            }
                                        },
                                        'fields': 'gridProperties(rowCount,columnCount)'
                                    }
                                }]
                            }
                        ).execute
                    )

                # Write all adapted values in a single batch operation
                logger.info(f"Writing {len(batch_data)} ranges in a single batch API call")
                batch_update_result = await asyncio.to_thread(
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={
                            'valueInputOption': value_input_option,
                            'data': batch_data
                        }
                    ).execute
                )

                # Process batch results
                for response in batch_update_result.get('responses', []):
                    updated_cells = response.get('updatedCells', 0)
                    updated_range = response.get('updatedRange', '')
                    total_updated_cells += updated_cells
                    all_updated_ranges.append(updated_range)

                logger.info(f"Batch write completed: {total_updated_cells} cells updated across {len(batch_data)} ranges")

            # Build result message
            if auto_fill:
                message = f"Auto-filled formulas from {from_range} to {len(target_ranges)} row(s). Total {total_updated_cells} cells updated. Ranges: {', '.join(all_updated_ranges[:5])}"
                if len(all_updated_ranges) > 5:
                    message += f" ... and {len(all_updated_ranges) - 5} more"
                result_range = f"{from_range} → {len(target_ranges)} ranges"
            elif len(target_ranges) > 1:
                # Multi-row manual mode
                message = f"Successfully copied range {from_range} to {len(target_ranges)} row(s) with formulas adapted. Total {total_updated_cells} cells updated. Ranges: {', '.join(all_updated_ranges[:5])}"
                if len(all_updated_ranges) > 5:
                    message += f" ... and {len(all_updated_ranges) - 5} more"
                result_range = f"{from_range} → {len(target_ranges)} ranges"
            else:
                # Single target range (traditional behavior)
                message = f"Successfully copied range {from_range} to {target_ranges[0]} with formulas adapted. {total_updated_cells} cells updated."
                result_range = all_updated_ranges[0] if all_updated_ranges else target_ranges[0]

            return UpdateResponse(
                success=True,
                spreadsheet_url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_properties['sheetId']}",
                spreadsheet_id=spreadsheet_id,
                worksheet=sheet_title,
                range=result_range,
                updated_cells=total_updated_cells,
                shape=f"({from_rows},{from_cols})",
                error=None,
                message=message
            )

        except Exception as e:
            logger.error(f"Error copying range with formulas in {uri}: {e}")
            target_info = f"auto-fill mode (lookup_column={lookup_column})" if auto_fill else f"to {to_range}"
            raise Exception(f"Failed to copy range {from_range} {target_info} in {uri}: {e}") from e

    async def _get_autofill_target_ranges(
        self,
        service,
        spreadsheet_id: str,
        sheet_title: str,
        from_range_parsed: Dict[str, Any],
        lookup_column: str,
        skip_if_exists: bool
    ) -> List[str]:
        """
        Helper method to determine target ranges for auto-fill mode.

        Reads the sheet to:
        1. Detect header row location
        2. Find all rows with data in lookup_column
        3. Build target range strings for each row (excluding source row)
        4. Optionally skip rows that already have values (skip_if_exists)

        Returns:
            List of target range strings in A1 notation (e.g., ["B3:K3", "B4:K4"])
        """
        # Get source row and column range
        source_row = from_range_parsed['start_row']
        start_col = column_index_to_letter(from_range_parsed['start_col_idx'])
        end_col = column_index_to_letter(from_range_parsed['end_col_idx'])

        # Read entire lookup column and first destination column to determine:
        # 1. Which rows have data in lookup column
        # 2. Which rows already have values (if skip_if_exists=True)

        # Use a generous range to read - from row 1 to 10000
        lookup_range = f"'{sheet_title}'!{lookup_column}1:{lookup_column}10000"
        first_dest_col = column_index_to_letter(from_range_parsed['start_col_idx'])
        dest_range = f"'{sheet_title}'!{first_dest_col}1:{first_dest_col}10000"

        # Read both columns in parallel
        result_lookup = await asyncio.to_thread(
            service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=lookup_range,
                valueRenderOption=ValueRenderOption.UNFORMATTED_VALUE.value
            ).execute
        )

        result_dest = await asyncio.to_thread(
            service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=dest_range,
                valueRenderOption=ValueRenderOption.UNFORMATTED_VALUE.value
            ).execute
        )

        lookup_values = result_lookup.get('values', [])
        dest_values = result_dest.get('values', [])

        # Auto-detect header row: find first row with non-empty value in lookup_column
        header_row = 0
        for idx, row in enumerate(lookup_values):
            if row and row[0]:  # Has value in lookup column
                header_row = idx + 1  # Convert to 1-indexed
                break

        if header_row == 0:
            logger.warning(f"Could not detect header row in {lookup_column}, assuming row 1")
            header_row = 1

        logger.info(f"Auto-detected header row: {header_row}")

        # Build target ranges for rows with data in lookup_column
        target_ranges = []

        # Start checking from the row after header
        for row_idx in range(header_row, len(lookup_values)):
            actual_row_number = row_idx + 1  # Convert to 1-indexed

            # Skip source row
            if actual_row_number == source_row:
                continue

            # Check if lookup column has data
            lookup_has_data = False
            if row_idx < len(lookup_values):
                row_data = lookup_values[row_idx]
                if row_data and row_data[0]:  # Has value
                    lookup_has_data = True

            if not lookup_has_data:
                # Stop at first empty cell in lookup column
                logger.info(f"Stopping auto-fill at row {actual_row_number}: {lookup_column} is empty")
                break

            # Check if destination already has value (if skip_if_exists)
            if skip_if_exists:
                dest_has_value = False
                if row_idx < len(dest_values):
                    dest_row_data = dest_values[row_idx]
                    if dest_row_data and dest_row_data[0]:  # Has value
                        dest_has_value = True

                if dest_has_value:
                    logger.info(f"Skipping row {actual_row_number}: {first_dest_col}{actual_row_number} already has value")
                    continue

            # Add this row to target ranges
            target_range = f"{start_col}{actual_row_number}:{end_col}{actual_row_number}"
            target_ranges.append(target_range)

        logger.info(f"Auto-fill determined {len(target_ranges)} target ranges: {target_ranges}")
        return target_ranges


    def _parse_simple_range_address(self, range_address: str) -> Dict[str, Any]:
        """
        Parse A1 notation range address into components.

        Args:
            range_address: A1 notation (e.g., "B5:Z5", "L1:L100", "A1")

        Returns:
            Dict with keys: start_col, start_row, end_col, end_row, start_col_idx, end_col_idx

        Examples:
            >>> _parse_simple_range_address("B5:Z5")
            {'start_col': 'B', 'start_row': 5, 'end_col': 'Z', 'end_row': 5, 'start_col_idx': 1, 'end_col_idx': 25}
        """
        # Remove sheet name if present
        if '!' in range_address:
            range_address = range_address.split('!')[-1]

        # Check if it's a range or single cell
        if ':' in range_address:
            start_cell, end_cell = range_address.split(':')
        else:
            start_cell = end_cell = range_address

        # Parse start cell
        match = re.match(r'^([A-Z]+)(\d+)$', start_cell, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid cell address: {start_cell}")

        start_col = match.group(1).upper()
        start_row = int(match.group(2))

        # Parse end cell
        match = re.match(r'^([A-Z]+)(\d+)$', end_cell, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid cell address: {end_cell}")

        end_col = match.group(1).upper()
        end_row = int(match.group(2))

        return {
            'start_col': start_col,
            'start_row': start_row,
            'end_col': end_col,
            'end_row': end_row,
            'start_col_idx': column_letter_to_index(start_col),
            'end_col_idx': column_letter_to_index(end_col)
        }
