"""
Google Sheets Service using MongoDB credential sharing
Consolidated service for all Google Sheets operations
"""

import os
from typing import List, Optional, Dict, Any
import gspread_asyncio
from google.oauth2.credentials import Credentials

from core.factory import get_mongodb
from core.auth_models import UserExternalAuthInfo, GoogleCredentials
from core.error import UserError


async def get_google_credentials(user_id: str) -> GoogleCredentials:
    """
    Get Google credentials for a user from MongoDB
    Based on the shared authentication approach
    """
    collection = get_mongodb(db_name="play")[
        UserExternalAuthInfo.MongoConfig.collection
    ]
    doc = await collection.find_one(
        {
            "$and": [
                {"user_id": user_id},
                {"auth_info.scope": {"$regex": "spreadsheets", "$options": "i"}},
                {"auth_info.scope": {"$regex": "drive", "$options": "i"}},
            ]
        }
    )
    if not doc:
        raise UserError("Google credentials not found")
    auth_info = UserExternalAuthInfo.model_validate(doc)
    return GoogleCredentials(
        access_token=auth_info.auth_info["access_token"],
        refresh_token=auth_info.auth_info["refresh_token"],
        scope=auth_info.auth_info["scope"],
    )


class GoogleSheetsService:
    """Consolidated Google Sheets service for all spreadsheet operations"""

    async def get_client(self, user_id: str):
        """Get authenticated Google Sheets client for user"""
        user_credentials = await get_google_credentials(user_id)
        creds = Credentials(
            token=user_credentials.access_token,
            refresh_token=user_credentials.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
            scopes=user_credentials.scope.split(),
        )
        async_client_manager = gspread_asyncio.AsyncioGspreadClientManager(
            lambda: creds
        )
        return await async_client_manager.authorize()

    async def read_sheet(self, user_id: str, spreadsheet_id: str, sheet_name: str = None) -> List[List[str]]:
        """Read data from a Google Sheet"""
        client = await self.get_client(user_id)
        spreadsheet = await client.open_by_key(spreadsheet_id)

        if sheet_name:
            worksheet = await spreadsheet.worksheet(sheet_name)
        else:
            worksheet = await spreadsheet.get_worksheet(0)

        return await worksheet.get_all_values()

    async def write_sheet(self, user_id: str, spreadsheet_id: str, data: List[List[str]], sheet_name: str = None) -> bool:
        """Write data to a Google Sheet"""
        client = await self.get_client(user_id)
        spreadsheet = await client.open_by_key(spreadsheet_id)

        if sheet_name:
            worksheet = await spreadsheet.worksheet(sheet_name)
        else:
            worksheet = await spreadsheet.get_worksheet(0)

        await worksheet.clear()
        await worksheet.update('A1', data)
        return True

    async def get_range_values(self, user_id: str, spreadsheet_id: str, range_notation: str) -> List[List[str]]:
        """Get values from a specific range"""
        client = await self.get_client(user_id)
        spreadsheet = await client.open_by_key(spreadsheet_id)
        worksheet = await spreadsheet.worksheet(range_notation.split('!')[0] if '!' in range_notation else 'Sheet1')

        range_part = range_notation.split('!')[1] if '!' in range_notation else range_notation
        return await worksheet.get(range_part)

    async def update_range(self, user_id: str, spreadsheet_id: str, range_notation: str, values: List[List[str]]) -> bool:
        """Update values in a specific range"""
        client = await self.get_client(user_id)
        spreadsheet = await client.open_by_key(spreadsheet_id)
        worksheet = await spreadsheet.worksheet(range_notation.split('!')[0] if '!' in range_notation else 'Sheet1')

        range_part = range_notation.split('!')[1] if '!' in range_notation else range_notation
        await worksheet.update(range_part, values)
        return True

    async def clear_range(self, user_id: str, spreadsheet_id: str, range_notation: str) -> bool:
        """Clear values in a specific range"""
        client = await self.get_client(user_id)
        spreadsheet = await client.open_by_key(spreadsheet_id)
        worksheet = await spreadsheet.worksheet(range_notation.split('!')[0] if '!' in range_notation else 'Sheet1')

        range_part = range_notation.split('!')[1] if '!' in range_notation else range_notation
        await worksheet.batch_clear([range_part])
        return True

    async def create_spreadsheet(self, user_id: str, title: str) -> Dict[str, Any]:
        """Create a new spreadsheet"""
        client = await self.get_client(user_id)
        spreadsheet = await client.create(title)
        return {
            "spreadsheet_id": spreadsheet.id,
            "title": spreadsheet.title,
            "url": spreadsheet.url
        }

    async def get_spreadsheet_info(self, user_id: str, spreadsheet_id: str) -> Dict[str, Any]:
        """Get spreadsheet metadata"""
        client = await self.get_client(user_id)
        spreadsheet = await client.open_by_key(spreadsheet_id)
        worksheets = await spreadsheet.worksheets()

        return {
            "spreadsheet_id": spreadsheet.id,
            "title": spreadsheet.title,
            "url": spreadsheet.url,
            "worksheets": [{"title": ws.title, "id": ws.id} for ws in worksheets]
        }

    async def get_worksheet_info(self, user_id: str, spreadsheet_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Get worksheet information including used range"""
        client = await self.get_client(user_id)
        spreadsheet = await client.open_by_key(spreadsheet_id)

        if sheet_name:
            worksheet = await spreadsheet.worksheet(sheet_name)
        else:
            worksheet = await spreadsheet.get_worksheet(0)

        # Get used range by finding last non-empty cell
        all_values = await worksheet.get_all_values()
        if all_values:
            row_count = len(all_values)
            col_count = max(len(row) for row in all_values) if all_values else 0
        else:
            row_count = 0
            col_count = 0

        return {
            "title": worksheet.title,
            "id": worksheet.id,
            "row_count": row_count,
            "col_count": col_count,
            "used_range": f"A1:{chr(65 + col_count - 1)}{row_count}" if row_count > 0 and col_count > 0 else "A1:A1",
            "url": f"{spreadsheet.url}#gid={worksheet.id}"
        }

    async def read_sheet_structured(self, user_id: str, spreadsheet_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """
        Read sheet data with structure information (headers detection, etc.)
        Returns data in the format expected by the DataTable system
        """
        # Get worksheet info
        worksheet_info = await self.get_worksheet_info(user_id, spreadsheet_id, sheet_name)

        # Read all data
        all_data = await self.read_sheet(user_id, spreadsheet_id, sheet_name)

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

    async def write_sheet_structured(self, user_id: str, spreadsheet_id: str,
                                   data: List[List[str]], headers: Optional[List[str]] = None,
                                   sheet_name: str = None, title: Optional[str] = None) -> Dict[str, Any]:
        """
        Write data to sheet with structure (create if needed)
        Returns structured response compatible with DataTable system
        """
        # Prepare data for writing
        write_data = []
        if headers:
            write_data.append(headers)
        write_data.extend(data)

        # Create spreadsheet if spreadsheet_id is None and title is provided
        if not spreadsheet_id and title:
            create_result = await self.create_spreadsheet(user_id, title)
            spreadsheet_id = create_result["spreadsheet_id"]

        # Write the data
        success = await self.write_sheet(user_id, spreadsheet_id, write_data, sheet_name)

        if success:
            # Get updated worksheet info
            worksheet_info = await self.get_worksheet_info(user_id, spreadsheet_id, sheet_name)

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
        else:
            raise Exception("Failed to write data to spreadsheet")