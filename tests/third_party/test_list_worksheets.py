#!/usr/bin/env python3
"""
Test script for list_worksheets tool - lists all worksheets in a Google Sheets spreadsheet
"""

import asyncio
import os
import sys
import logging

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_dir)

# Google Sheets API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Import the GoogleSheetDataTable class
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def authenticate():
    """Authenticate with Google Sheets API using environment variables"""
    # Get OAuth credentials from environment variables
    refresh_token = os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
    client_id = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")

    if not all([refresh_token, client_id, client_secret]):
        print("❌ ERROR: Missing required environment variables!")
        print("📋 Please set the following environment variables:")
        print("   - TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
        print("   - TEST_GOOGLE_OAUTH_CLIENT_ID")
        print("   - TEST_GOOGLE_OAUTH_CLIENT_SECRET")
        return None

    try:
        # Create credentials from environment variables
        creds = Credentials(
            token=None,  # Will be refreshed
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES
        )

        # Refresh the token
        creds.refresh(Request())

        # Build the service
        service = build('sheets', 'v4', credentials=creds)
        print("✅ Successfully authenticated with Google Sheets API")
        return service

    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return None


async def test_list_worksheets():
    """Test listing worksheets in a Google Sheets spreadsheet"""
    print("\n" + "=" * 60)
    print("🧪 Testing list_worksheets tool")
    print("=" * 60)

    # Authenticate
    service = authenticate()
    if not service:
        return

    # Test spreadsheet URI - you can change this to any Google Sheets URL
    # Using the same test spreadsheet as other tests
    test_uri = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit"

    print(f"\n📄 Testing with URI: {test_uri}")

    try:
        # Create GoogleSheetDataTable instance
        google_sheet = GoogleSheetDataTable()

        # Call list_worksheets method
        print("\n🔍 Calling list_worksheets...")
        result = await google_sheet.list_worksheets(service, test_uri)

        # Display results
        print("\n" + "=" * 60)
        print("📊 Results:")
        print("=" * 60)
        print(f"Success: {result.success}")
        print(f"Spreadsheet ID: {result.spreadsheet_id}")
        print(f"Spreadsheet Title: {result.spreadsheet_title}")
        print(f"Spreadsheet URL: {result.spreadsheet_url}")
        print(f"Total Worksheets: {result.total_worksheets}")
        print(f"Message: {result.message}")

        if result.error:
            print(f"Error: {result.error}")

        if result.success and result.worksheets:
            print("\n📋 Worksheet Details:")
            print("-" * 60)
            for ws in result.worksheets:
                print(f"  [{ws.index}] {ws.title}")
                print(f"      Sheet ID (gid): {ws.sheet_id}")
                print(f"      Dimensions: {ws.row_count} rows × {ws.column_count} columns")
                print(f"      URL: {result.spreadsheet_url}#gid={ws.sheet_id}")
                print()

        print("=" * 60)
        if result.success:
            print("✅ Test completed successfully!")
        else:
            print("❌ Test failed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()


async def test_with_spreadsheet_id():
    """Test with just the spreadsheet ID (not full URL)"""
    print("\n" + "=" * 60)
    print("🧪 Testing list_worksheets with spreadsheet ID only")
    print("=" * 60)

    # Authenticate
    service = authenticate()
    if not service:
        return

    # Test with spreadsheet ID only
    test_spreadsheet_id = "16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60"

    print(f"\n📄 Testing with Spreadsheet ID: {test_spreadsheet_id}")

    try:
        # Create GoogleSheetDataTable instance
        google_sheet = GoogleSheetDataTable()

        # Call list_worksheets method
        print("\n🔍 Calling list_worksheets...")
        result = await google_sheet.list_worksheets(service, test_spreadsheet_id)

        # Display summary
        print("\n📊 Summary:")
        print(f"Success: {result.success}")
        print(f"Spreadsheet: '{result.spreadsheet_title}'")
        print(f"Found {result.total_worksheets} worksheet(s)")

        if result.success:
            print("✅ Spreadsheet ID test passed!")
        else:
            print(f"❌ Test failed: {result.error}")

    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")


if __name__ == "__main__":
    print("\n🚀 Starting list_worksheets tests")

    # Run both tests
    asyncio.run(test_list_worksheets())
    asyncio.run(test_with_spreadsheet_id())

    print("\n✨ All tests completed!")
