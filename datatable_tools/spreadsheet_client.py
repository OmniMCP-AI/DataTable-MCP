import os
import logging
import aiohttp
from typing import Dict, Any
from datatable_tools.spreadsheet_models import (
    ReadSheetRequest,
    ReadSheetResponse,
    WriteSheetRequest,
    WriteSheetResponse
)

logger = logging.getLogger(__name__)


class SpreadsheetClient:
    """Client for interacting with the SPREADSHEET_API"""

    def __init__(self, api_endpoint: str = None):
        self.api_endpoint = api_endpoint or os.getenv('SPREADSHEET_API', 'http://localhost:9394')
        if not self.api_endpoint.startswith(('http://', 'https://')):
            self.api_endpoint = f"http://{self.api_endpoint}"

    async def read_sheet(self, request: ReadSheetRequest, user_id: str) -> ReadSheetResponse:
        """
        Read data from a spreadsheet using the SPREADSHEET_API endpoint

        Args:
            request: ReadSheetRequest containing spreadsheet and worksheet info
            user_id: User ID for authentication

        Returns:
            ReadSheetResponse with spreadsheet data
        """
        url = f"{self.api_endpoint}/v1/tool/sheet/worksheet/read_sheet"
        headers = {
            "user-id": user_id,
            "content-type": "application/json"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=request.model_dump(),
                    headers=headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return ReadSheetResponse(**data)
                    else:
                        error_text = await response.text()
                        logger.error(f"Spreadsheet API error {response.status}: {error_text}")
                        raise Exception(f"Spreadsheet API error {response.status}: {error_text}")

        except aiohttp.ClientError as e:
            logger.error(f"Connection error to spreadsheet API: {e}")
            raise Exception(f"Failed to connect to spreadsheet API at {self.api_endpoint}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error reading spreadsheet: {e}")
            raise

    async def write_sheet(self, request: WriteSheetRequest, user_id: str) -> WriteSheetResponse:
        """
        Write data to a spreadsheet using the SPREADSHEET_API endpoint

        Args:
            request: WriteSheetRequest containing data to write
            user_id: User ID for authentication

        Returns:
            WriteSheetResponse with write operation results
        """
        url = f"{self.api_endpoint}/v1/tool/sheet/worksheet/write_sheet"
        headers = {
            "user-id": user_id,
            "content-type": "application/json"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=request.model_dump(),
                    headers=headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return WriteSheetResponse(**data)
                    else:
                        error_text = await response.text()
                        logger.error(f"Spreadsheet API error {response.status}: {error_text}")
                        raise Exception(f"Spreadsheet API error {response.status}: {error_text}")

        except aiohttp.ClientError as e:
            logger.error(f"Connection error to spreadsheet API: {e}")
            raise Exception(f"Failed to connect to spreadsheet API at {self.api_endpoint}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error writing to spreadsheet: {e}")
            raise


# Global instance
spreadsheet_client = SpreadsheetClient()