#!/usr/bin/env python3
"""
Debug script to check spreadsheet API endpoints
"""

import asyncio
import aiohttp
import os
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_API_ENDPOINT = os.getenv('SPREADSHEET_API', 'http://localhost:9394')

async def test_endpoints():
    """Test different endpoint paths to find the correct one"""

    test_urls = [
        f"{SPREADSHEET_API_ENDPOINT}/api/v1/tool/worksheet/read_sheet",
        f"{SPREADSHEET_API_ENDPOINT}/api/v1/tool/worksheet/write_sheet",
        f"{SPREADSHEET_API_ENDPOINT}/api/v1/worksheet/read_sheet",
        f"{SPREADSHEET_API_ENDPOINT}/api/v1/worksheet/write_sheet",
        f"{SPREADSHEET_API_ENDPOINT}/v1/tool/worksheet/read_sheet",
        f"{SPREADSHEET_API_ENDPOINT}/v1/tool/worksheet/write_sheet",
        f"{SPREADSHEET_API_ENDPOINT}/",
        f"{SPREADSHEET_API_ENDPOINT}/docs",
        f"{SPREADSHEET_API_ENDPOINT}/openapi.json"
    ]

    headers = {
        "user-id": "68501372a3569b6897673a48",
        "content-type": "application/json"
    }

    test_request = {
        "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
        "worksheet": "Class Data"
    }

    async with aiohttp.ClientSession() as session:
        for url in test_urls:
            print(f"Testing: {url}")
            try:
                if url.endswith(('.json', '/', '/docs')):
                    # GET request for these endpoints
                    async with session.get(url) as response:
                        print(f"  Status: {response.status}")
                        if response.status == 200:
                            text = await response.text()
                            print(f"  Response: {text[:200]}...")
                        else:
                            text = await response.text()
                            print(f"  Error: {text}")
                else:
                    # POST request for API endpoints
                    async with session.post(url, json=test_request, headers=headers) as response:
                        print(f"  Status: {response.status}")
                        text = await response.text()
                        if response.status != 404:
                            print(f"  Response: {text[:200]}...")
                        else:
                            print(f"  404 - Not Found")
            except Exception as e:
                print(f"  Exception: {e}")
            print()

if __name__ == "__main__":
    asyncio.run(test_endpoints())