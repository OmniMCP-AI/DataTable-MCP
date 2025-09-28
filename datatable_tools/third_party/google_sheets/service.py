"""
Google Sheets Service using omnimcp auth standard
Consolidated service for all Google Sheets operations
"""

import os
from typing import List, Optional, Dict, Any
from googleapiclient.discovery import build
from fastmcp import Context

from datatable_tools.auth.service_decorator import require_google_service
from core.error import UserError


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
        body = {
            'values': values
        }
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_notation,
            valueInputOption='RAW',
            body=body
        ).execute()

        return True

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
        """Get worksheet information including used range"""
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # Find the target sheet
        target_sheet = None
        if sheet_name:
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
        """
        # Get spreadsheet info
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # Find the target sheet
        target_sheet = None
        if sheet_name:
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
    async def write_sheet_structured(service, ctx: Context, spreadsheet_id: str,
                                   data: List[List[str]], headers: Optional[List[str]] = None,
                                   sheet_name: str = None, title: Optional[str] = None) -> Dict[str, Any]:
        """
        Write data to sheet with structure (create if needed)
        Returns structured response compatible with DataTable system
        """
        # Create spreadsheet if spreadsheet_id is None and title is provided
        if not spreadsheet_id and title:
            spreadsheet = {
                'properties': {
                    'title': title
                }
            }
            result = service.spreadsheets().create(body=spreadsheet).execute()
            spreadsheet_id = result['spreadsheetId']

        # Prepare data for writing
        write_data = []
        if headers:
            write_data.append(headers)
        write_data.extend(data)

        # Determine sheet name/id
        if sheet_name:
            range_name = f"'{sheet_name}'!A1"
            clear_range = f"'{sheet_name}'"
        else:
            range_name = "A1"
            clear_range = "Sheet1"

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

        # Get updated worksheet info
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        target_sheet = None
        if sheet_name:
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    target_sheet = sheet
                    break
        else:
            target_sheet = spreadsheet['sheets'][0] if spreadsheet['sheets'] else None

        if target_sheet:
            sheet_props = target_sheet['properties']
            sheet_title = sheet_props['title']
            sheet_id = sheet_props['sheetId']

            worksheet_info = {
                "title": sheet_title,
                "id": sheet_id,
                "url": f"{spreadsheet['spreadsheetUrl']}#gid={sheet_id}"
            }
        else:
            worksheet_info = {"title": "Unknown", "id": 0, "url": ""}

        total_rows = len(write_data)
        total_cols = len(headers) if headers else (len(write_data[0]) if write_data else 0)

        return {
            "success": True,
            "spreadsheet_id": spreadsheet_id,
            "worksheet": worksheet_info,
            "updated_range": f"A1:{chr(65 + total_cols - 1)}{total_rows}" if total_rows > 0 and total_cols > 0 else "A1:A1",
            "updated_cells": total_rows * total_cols,
            "matched_columns": headers if headers else [],
            "worksheet_url": worksheet_info["url"],
            "message": f"Successfully wrote {len(data)} rows to worksheet '{worksheet_info['title']}'"
        }