"""
Google Sheets Service using omnimcp auth standard
Consolidated service for all Google Sheets operations
"""

import os
import logging
from typing import List, Optional, Dict, Any
from googleapiclient.discovery import build
from fastmcp import Context

from datatable_tools.auth.service_decorator import require_google_service
from core.error import UserError

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """Consolidated Google Sheets service for all spreadsheet operations"""

    @staticmethod
    @require_google_service("sheets", "sheets_read")
    async def read_sheet(service, ctx: Context, spreadsheet_id: str, sheet_name: str = None) -> List[List[str]]:
        """Read data from a Google Sheet"""
        # Get the spreadsheet using the injected service
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # Determine sheet name/id
        if sheet_name:
            # Find sheet by name
            sheet_id = None
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            if sheet_id is None:
                raise UserError(f"Sheet '{sheet_name}' not found")
            range_name = f"'{sheet_name}'!A:ZZ"
        else:
            # Use first sheet
            range_name = "A:ZZ"

        # Get values
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        return result.get('values', [])

    @staticmethod
    @require_google_service("sheets", "sheets_write")
    async def write_sheet(service, ctx: Context, spreadsheet_id: str, data: List[List[str]], sheet_name: str = None) -> bool:
        """Write data to a Google Sheet"""
        # Determine sheet name/id
        if sheet_name:
            range_name = f"'{sheet_name}'!A1"
        else:
            range_name = "A1"

        # Clear existing data first
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_name.split('!')[0] if '!' in range_name else "Sheet1"
        ).execute()

        # Write new data
        body = {
            'values': data
        }
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

        return True

    @staticmethod
    @require_google_service("sheets", "sheets_read")
    async def get_range_values(service, ctx: Context, spreadsheet_id: str, range_notation: str) -> List[List[str]]:
        """Get values from a specific range"""
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_notation
        ).execute()

        return result.get('values', [])

    @staticmethod
    @require_google_service("sheets", "sheets_write")
    async def update_range(service, ctx: Context, spreadsheet_id: str, range_notation: str, values: List[List[str]]) -> bool:
        """Update values in a specific range"""
        from googleapiclient.errors import HttpError

        body = {
            'values': values
        }

        try:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_notation,
                valueInputOption='RAW',
                body=body
            ).execute()
            return True

        except HttpError as e:
            # Check if it's a 400 error related to invalid worksheet/range
            if e.resp.status == 400 and "Unable to parse range" in str(e):
                # Extract worksheet name from range_notation (e.g., "Sheet30!A1:E12" -> "Sheet30")
                worksheet_name = None
                if '!' in range_notation:
                    worksheet_name = range_notation.split('!')[0].strip("'\"")

                # Get available worksheets
                try:
                    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                    available_sheets = [sheet['properties']['title'] for sheet in spreadsheet.get('sheets', [])]
                except HttpError as fetch_error:
                    # If we can't fetch worksheets, keep original error with additional context
                    if worksheet_name:
                        raise UserError(
                            f"{str(e)}\n\n"
                            f"Additional context: Worksheet '{worksheet_name}' not found in spreadsheet. "
                            f"Unable to retrieve available worksheets: {fetch_error}"
                        ) from e
                    else:
                        raise UserError(f"{str(e)}") from e

                # Build enhanced error message with original error + available worksheets
                if worksheet_name and available_sheets:
                    error_msg = (
                        f"{str(e)}\n\n"
                        f"Worksheet '{worksheet_name}' not found in spreadsheet. "
                        f"Available worksheets: {', '.join(repr(s) for s in available_sheets)}. "
                        f"Please use one of these worksheet names in your range."
                    )
                elif available_sheets:
                    error_msg = (
                        f"{str(e)}\n\n"
                        f"Invalid range format: {range_notation}. "
                        f"Available worksheets: {', '.join(repr(s) for s in available_sheets)}. "
                        f"Use format: 'WorksheetName!A1:B2' or just 'A1:B2' for the first sheet."
                    )
                else:
                    error_msg = f"{str(e)}\n\nThe spreadsheet has no worksheets."

                raise UserError(error_msg) from e
            else:
                # Re-raise other HTTP errors
                raise

    @staticmethod
    @require_google_service("sheets", "sheets_write")
    async def clear_range(service, ctx: Context, spreadsheet_id: str, range_notation: str) -> bool:
        """Clear values in a specific range"""
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_notation
        ).execute()

        return True

    @staticmethod
    @require_google_service("sheets", "sheets_write")
    async def create_spreadsheet(service, ctx: Context, title: str) -> Dict[str, Any]:
        """Create a new spreadsheet"""
        spreadsheet = {
            'properties': {
                'title': title
            }
        }

        result = service.spreadsheets().create(body=spreadsheet).execute()

        return {
            "spreadsheet_id": result['spreadsheetId'],
            "title": result['properties']['title'],
            "url": result['spreadsheetUrl']
        }

    @staticmethod
    @require_google_service("sheets", "sheets_write")
    async def create_new_spreadsheet(service, ctx: Context, title: str,
                                    data: List[List[str]],
                                    headers: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Create a new spreadsheet with initial data.

        Args:
            title: Name for the new spreadsheet
            data: Initial data rows to write
            headers: Optional column headers

        Returns:
            Dict containing spreadsheet ID, URL, worksheet info, and success status
        """
        # Create the spreadsheet
        spreadsheet = {
            'properties': {
                'title': title
            }
        }

        result = service.spreadsheets().create(body=spreadsheet).execute()
        spreadsheet_id = result['spreadsheetId']
        spreadsheet_url = result['spreadsheetUrl']

        # Get the first sheet's info
        created_spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        first_sheet = created_spreadsheet['sheets'][0] if created_spreadsheet['sheets'] else None

        if not first_sheet:
            raise UserError("Failed to create spreadsheet with initial sheet")

        sheet_props = first_sheet['properties']
        sheet_title = sheet_props['title']
        sheet_id = sheet_props['sheetId']

        # Prepare data for writing
        write_data = []
        if headers:
            write_data.append(headers)
        write_data.extend(data)

        # Write initial data if provided
        if write_data:
            range_name = f"'{sheet_title}'!A1"
            body = {'values': write_data}
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

        total_rows = len(write_data)
        total_cols = len(write_data[0]) if write_data else 0

        return {
            "success": True,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_url": spreadsheet_url,
            "worksheet": sheet_title,
            "sheet_id": sheet_id,
            "updated_range": f"A1:{chr(65 + total_cols - 1)}{total_rows}" if total_rows > 0 and total_cols > 0 else "A1:A1",
            "updated_cells": total_rows * total_cols,
            "message": f"Successfully created new spreadsheet '{title}' with {len(data)} data rows"
        }

    @staticmethod
    async def get_spreadsheet_info(service, ctx: Context, spreadsheet_id: str) -> Dict[str, Any]:
        """Get spreadsheet metadata"""
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        worksheets = []
        for sheet in spreadsheet.get('sheets', []):
            props = sheet['properties']
            worksheets.append({
                "title": props['title'],
                "id": props['sheetId']
            })

        return {
            "spreadsheet_id": spreadsheet['spreadsheetId'],
            "title": spreadsheet['properties']['title'],
            "url": spreadsheet['spreadsheetUrl'],
            "worksheets": worksheets
        }

    @staticmethod
    @require_google_service("sheets", "sheets_read")
    async def get_worksheet_info(service, ctx: Context, spreadsheet_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Get worksheet information including used range

        Args:
            service: Google Sheets service
            ctx: Context
            spreadsheet_id: Spreadsheet ID
            sheet_name: Sheet name or "gid:{gid}" format
        """
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # Find the target sheet
        target_sheet = None
        if sheet_name:
            # Check if sheet_name is in "gid:{gid}" format
            if sheet_name.startswith("gid:"):
                gid = int(sheet_name[4:])
                for sheet in spreadsheet['sheets']:
                    if sheet['properties']['sheetId'] == gid:
                        target_sheet = sheet
                        break
            else:
                # Find by sheet name
                for sheet in spreadsheet['sheets']:
                    if sheet['properties']['title'] == sheet_name:
                        target_sheet = sheet
                        break
        else:
            target_sheet = spreadsheet['sheets'][0] if spreadsheet['sheets'] else None

        if not target_sheet:
            raise UserError(f"Sheet '{sheet_name}' not found")

        sheet_props = target_sheet['properties']
        sheet_title = sheet_props['title']
        sheet_id = sheet_props['sheetId']

        # Get data to determine used range
        range_name = f"'{sheet_title}'!A:ZZ"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        all_values = result.get('values', [])
        if all_values:
            row_count = len(all_values)
            col_count = max(len(row) for row in all_values) if all_values else 0
        else:
            row_count = 0
            col_count = 0

        return {
            "title": sheet_title,
            "id": sheet_id,
            "row_count": row_count,
            "col_count": col_count,
            "used_range": f"A1:{chr(65 + col_count - 1)}{row_count}" if row_count > 0 and col_count > 0 else "A1:A1",
            "url": f"{spreadsheet['spreadsheetUrl']}#gid={sheet_id}"
        }

    @staticmethod
    async def read_sheet_structured(service, ctx: Context, spreadsheet_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """
        Read sheet data with structure information (headers detection, etc.)
        Returns data in the format expected by the DataTable system

        Args:
            service: Google Sheets service
            ctx: Context
            spreadsheet_id: Spreadsheet ID
            sheet_name: Sheet name or "gid:{gid}" format
        """
        logger.info(f"read_sheet_structured called with spreadsheet_id={spreadsheet_id}, sheet_name={sheet_name}")

        # Get spreadsheet info
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # Find the target sheet
        target_sheet = None
        if sheet_name:
            # Check if sheet_name is in "gid:{gid}" format
            if sheet_name.startswith("gid:"):
                gid = int(sheet_name[4:])
                logger.info(f"Looking for sheet with gid={gid}")
                for sheet in spreadsheet['sheets']:
                    sheet_id = sheet['properties']['sheetId']
                    sheet_title = sheet['properties']['title']
                    logger.info(f"  Checking sheet: {sheet_title} (gid={sheet_id})")
                    if sheet_id == gid:
                        target_sheet = sheet
                        logger.info(f"  ✓ Found matching sheet: {sheet_title}")
                        break
            else:
                # Find by sheet name
                logger.info(f"Looking for sheet with name={sheet_name}")
                for sheet in spreadsheet['sheets']:
                    if sheet['properties']['title'] == sheet_name:
                        target_sheet = sheet
                        break
        else:
            logger.info("No sheet_name provided, using first sheet")
            target_sheet = spreadsheet['sheets'][0] if spreadsheet['sheets'] else None

        if not target_sheet:
            raise UserError(f"Sheet '{sheet_name}' not found")

        sheet_props = target_sheet['properties']
        sheet_title = sheet_props['title']
        sheet_id = sheet_props['sheetId']

        # Get data
        range_name = f"'{sheet_title}'!A:ZZ" if sheet_name else "A:ZZ"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        all_data = result.get('values', [])

        # Calculate worksheet info
        if all_data:
            row_count = len(all_data)
            col_count = max(len(row) for row in all_data) if all_data else 0
        else:
            row_count = 0
            col_count = 0

        worksheet_info = {
            "title": sheet_title,
            "id": sheet_id,
            "row_count": row_count,
            "col_count": col_count,
            "used_range": f"A1:{chr(65 + col_count - 1)}{row_count}" if row_count > 0 and col_count > 0 else "A1:A1",
            "url": f"{spreadsheet['spreadsheetUrl']}#gid={sheet_id}"
        }

        # Process headers and data
        headers = []
        data = []

        if all_data:
            # Use first row as headers
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

        return {
            "success": True,
            "values": all_data,
            "headers": headers,
            "data": data,
            "worksheet": worksheet_info,
            "used_range": worksheet_info["used_range"],
            "row_count": worksheet_info["row_count"],
            "column_count": worksheet_info["col_count"],
            "worksheet_url": worksheet_info["url"],
            "message": f"Successfully read {len(data)} rows from worksheet '{worksheet_info['title']}'"
        }


    @staticmethod
    @require_google_service("sheets", "sheets_write")
    async def check_write_permission(service, ctx: Context, spreadsheet_id: str) -> bool:
        """
        Check if we have write permission for a spreadsheet

        Args:
            spreadsheet_id: The spreadsheet ID to check

        Returns:
            True if we have write permission, False otherwise
        """
        try:
            # Try to get the spreadsheet metadata - this will work if we have any access
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

            # Check if we can edit - we'll try a harmless operation
            # Get the first sheet to check permissions
            if not spreadsheet.get('sheets'):
                return False

            # If we got here, we at least have read access
            # To check write permission, we need to check if we can perform write operations
            # The safest way is to check the spreadsheet metadata for editable status
            # However, Google Sheets API doesn't directly expose this, so we'll assume
            # if we can get the spreadsheet with write scope, we have write permission
            return True

        except Exception as e:
            # If we can't access it at all, we don't have permission
            logger.warning(f"No access to spreadsheet {spreadsheet_id}: {e}")
            return False

    @staticmethod
    @require_google_service("sheets", "sheets_write")
    async def write_sheet_structured(service, ctx: Context, spreadsheet_identifier: str,
                                   data: List[List[str]], headers: Optional[List[str]] = None,
                                   sheet_name: str = None, title: Optional[str] = None) -> Dict[str, Any]:
        """
        Write data to sheet with structure (create if needed)
        Accepts either spreadsheet ID or spreadsheet name
        Returns structured response compatible with DataTable system

        If the spreadsheet exists but user lacks write permission, automatically creates
        a new spreadsheet with the data.

        Args:
            spreadsheet_identifier: Either spreadsheet ID or spreadsheet name
            data: List of data rows to write
            headers: Optional column headers
            sheet_name: Optional worksheet name (uses first sheet if not specified)
            title: Optional title for new spreadsheet (if identifier not found or no permission)
        """
        # Resolve spreadsheet identifier to ID
        spreadsheet_id = None
        needs_new_spreadsheet = False
        creation_reason = None

        # Check if it looks like a spreadsheet ID (contains specific characters)
        if len(spreadsheet_identifier) > 20 and ('_' in spreadsheet_identifier or '-' in spreadsheet_identifier):
            # Assume it's already a spreadsheet ID
            spreadsheet_id = spreadsheet_identifier
        else:
            # Need to search by name using Drive API - this requires a separate service call
            # For now, we'll try to use it as an ID and handle the error
            try:
                # Test if it's a valid spreadsheet ID by trying to get it
                service.spreadsheets().get(spreadsheetId=spreadsheet_identifier).execute()
                spreadsheet_id = spreadsheet_identifier
            except Exception:
                # Not a valid ID, so it must be a name - we'll need Drive API for this
                # For now, we'll use the title parameter to create a new spreadsheet
                spreadsheet_id = None
                needs_new_spreadsheet = True
                creation_reason = "not_found"

        # Check write permissions for existing spreadsheet
        if spreadsheet_id and not needs_new_spreadsheet:
            try:
                # Try to get the spreadsheet to verify it exists and check permissions
                service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                # If we can access it with write scope, assume we can write
                # Note: The actual write operation below will fail if we don't have permission
                # and that's when we'll catch it and create a new spreadsheet
            except Exception as e:
                logger.warning(f"Cannot access spreadsheet {spreadsheet_id}: {e}")
                needs_new_spreadsheet = True
                creation_reason = "insufficient_permissions"

        # Create spreadsheet if needed
        if needs_new_spreadsheet or (not spreadsheet_id and title):
            if creation_reason == "insufficient_permissions":
                spreadsheet_name = title or f"Copy of Spreadsheet {spreadsheet_identifier[:8]}"
                logger.info(f"No write permission for spreadsheet {spreadsheet_identifier}, creating new spreadsheet")
            elif creation_reason == "not_found":
                spreadsheet_name = title or f"New Spreadsheet"
                logger.info(f"Spreadsheet {spreadsheet_identifier} not found, creating new spreadsheet")
            else:
                spreadsheet_name = title or "New Spreadsheet"

            spreadsheet = {
                'properties': {
                    'title': spreadsheet_name
                }
            }
            result = service.spreadsheets().create(body=spreadsheet).execute()
            original_spreadsheet_id = spreadsheet_id
            spreadsheet_id = result['spreadsheetId']
            logger.info(f"Created new spreadsheet {spreadsheet_id} with name '{spreadsheet_name}'")
        elif not spreadsheet_id:
            raise UserError(f"Spreadsheet '{spreadsheet_identifier}' not found. Please provide a valid spreadsheet ID or use the 'title' parameter to create a new spreadsheet.")
        else:
            original_spreadsheet_id = None

        # Prepare data for writing
        write_data = []
        if headers:
            write_data.append(headers)
        write_data.extend(data)

        try:
            # Get spreadsheet info to determine actual sheet name
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

            # Determine sheet name/id
            if sheet_name:
                # Verify the sheet exists
                target_sheet = None
                for sheet in spreadsheet['sheets']:
                    if sheet['properties']['title'] == sheet_name:
                        target_sheet = sheet
                        break
                if not target_sheet:
                    raise UserError(f"Sheet '{sheet_name}' not found")
                range_name = f"'{sheet_name}'!A1"
                clear_range = f"'{sheet_name}'"
            else:
                # Use the first sheet's actual name
                first_sheet = spreadsheet['sheets'][0] if spreadsheet['sheets'] else None
                if not first_sheet:
                    raise UserError("No sheets found in spreadsheet")
                actual_sheet_name = first_sheet['properties']['title']
                range_name = f"'{actual_sheet_name}'!A1"
                clear_range = f"'{actual_sheet_name}'"

            # Clear existing data first
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=clear_range
            ).execute()

            # Write new data
            body = {
                'values': write_data
            }
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

            # Use the sheet info we already retrieved
            if sheet_name:
                # We already validated target_sheet exists above
                sheet_props = target_sheet['properties']
            else:
                # We already retrieved first_sheet above
                sheet_props = first_sheet['properties']

            sheet_title = sheet_props['title']
            sheet_id = sheet_props['sheetId']

            worksheet_info = {
                "title": sheet_title,
                "id": sheet_id,
                "url": f"{spreadsheet['spreadsheetUrl']}#gid={sheet_id}"
            }

            total_rows = len(write_data)
            total_cols = len(headers) if headers else (len(write_data[0]) if write_data else 0)

            # Build message with creation info if applicable
            message = f"Successfully wrote {len(data)} rows to worksheet '{worksheet_info['title']}'"
            if original_spreadsheet_id and creation_reason == "insufficient_permissions":
                message += f" (created new spreadsheet due to insufficient permissions on {original_spreadsheet_id})"

            return {
                "success": True,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": worksheet_info,
                "updated_range": f"A1:{chr(65 + total_cols - 1)}{total_rows}" if total_rows > 0 and total_cols > 0 else "A1:A1",
                "updated_cells": total_rows * total_cols,
                "matched_columns": headers if headers else [],
                "worksheet_url": worksheet_info["url"],
                "message": message,
                "created_new_spreadsheet": bool(original_spreadsheet_id),
                "original_spreadsheet_id": original_spreadsheet_id if original_spreadsheet_id else None
            }

        except Exception as write_error:
            # If write failed due to permissions, create a new spreadsheet and retry
            if not original_spreadsheet_id and "permission" in str(write_error).lower():
                logger.warning(f"Write permission denied for {spreadsheet_id}, creating new spreadsheet: {write_error}")

                # Create a new spreadsheet
                spreadsheet_name = title or f"Copy of Spreadsheet {spreadsheet_identifier[:8]}"
                new_spreadsheet = {
                    'properties': {
                        'title': spreadsheet_name
                    }
                }
                result = service.spreadsheets().create(body=new_spreadsheet).execute()
                new_spreadsheet_id = result['spreadsheetId']
                logger.info(f"Created new spreadsheet {new_spreadsheet_id} due to permission error")

                # Get new spreadsheet info
                spreadsheet = service.spreadsheets().get(spreadsheetId=new_spreadsheet_id).execute()
                first_sheet = spreadsheet['sheets'][0] if spreadsheet['sheets'] else None
                if not first_sheet:
                    raise UserError("Failed to create new spreadsheet")

                actual_sheet_name = first_sheet['properties']['title']
                range_name = f"'{actual_sheet_name}'!A1"

                # Write to the new spreadsheet
                body = {'values': write_data}
                service.spreadsheets().values().update(
                    spreadsheetId=new_spreadsheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body=body
                ).execute()

                sheet_props = first_sheet['properties']
                worksheet_info = {
                    "title": sheet_props['title'],
                    "id": sheet_props['sheetId'],
                    "url": f"{spreadsheet['spreadsheetUrl']}#gid={sheet_props['sheetId']}"
                }

                total_rows = len(write_data)
                total_cols = len(headers) if headers else (len(write_data[0]) if write_data else 0)

                return {
                    "success": True,
                    "spreadsheet_id": new_spreadsheet_id,
                    "worksheet": worksheet_info,
                    "updated_range": f"A1:{chr(65 + total_cols - 1)}{total_rows}" if total_rows > 0 and total_cols > 0 else "A1:A1",
                    "updated_cells": total_rows * total_cols,
                    "matched_columns": headers if headers else [],
                    "worksheet_url": worksheet_info["url"],
                    "message": f"Successfully wrote {len(data)} rows to new spreadsheet '{worksheet_info['title']}' (created due to insufficient permissions on original spreadsheet)",
                    "created_new_spreadsheet": True,
                    "original_spreadsheet_id": spreadsheet_id
                }
            else:
                # Re-raise other errors
                raise