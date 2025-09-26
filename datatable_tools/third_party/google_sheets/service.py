"""
Google Sheets Service using MongoDB credential sharing
Based on requirement-auth.md approach
"""

import os
from typing import List
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
    """Google Sheets service using shared MongoDB credentials"""

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