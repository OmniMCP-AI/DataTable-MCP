#!/usr/bin/env python3
"""
Test write_new_worksheet MCP tool

This test verifies that the write_new_worksheet tool can:
1. Create a new worksheet in an existing spreadsheet
2. Write data to the new worksheet
3. Return correct response with worksheet URL
"""

import asyncio
import sys
import os
import argparse

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

TEST_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1jFgc_WYBYPDJzHbtm1LYT8Bh-YH8MrJUcOZYi_OwPvs/edit?gid=0#gid=0"
def authenticate():
    """Authenticate with Google Sheets API"""
    refresh_token = os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
    client_id = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")

    if not all([refresh_token, client_id, client_secret]):
        print("❌ ERROR: Missing required environment variables!")
        print("Required: TEST_GOOGLE_OAUTH_REFRESH_TOKEN, TEST_GOOGLE_OAUTH_CLIENT_ID, TEST_GOOGLE_OAUTH_CLIENT_SECRET")
        return None

    try:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES
        )
        creds.refresh(Request())
        service = build('sheets', 'v4', credentials=creds)
        print("✅ Successfully authenticated")
        return service
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return None


async def test_write_new_worksheet():
    """Test creating a new worksheet with data"""

    # Get authenticated service
    print("Getting Google Sheets service...")
    service = authenticate()
    if not service:
        return False

    # Create instance
    google_sheet = GoogleSheetDataTable()

    # Test data - simple 2D array with headers
    test_data = [
        ["Product", "Price", "Quantity"],
        ["Widget A", "19.99", "100"],
        ["Widget B", "29.99", "50"],
        ["Widget C", "39.99", "75"]
    ]

    # Use an existing test spreadsheet
    # This is the same spreadsheet used in test_auto_header_detection.py
    spreadsheet_url = TEST_SPREADSHEET_URL
    print(f"Using test spreadsheet: {spreadsheet_url}")

    # Now test write_new_worksheet
    print("\nTesting write_new_worksheet with 2D array data...")
    response = await google_sheet.write_new_worksheet(
        service=service,
        uri=spreadsheet_url,
        data=test_data,
        worksheet_name="Products"
    )

    # Verify response
    print("\n=== Response ===")
    print(f"Success: {response.success}")
    print(f"Worksheet: {response.worksheet}")
    print(f"Spreadsheet ID: {response.spreadsheet_id}")
    print(f"Range: {response.range}")
    print(f"Updated cells: {response.updated_cells}")
    print(f"Shape: {response.shape}")
    print(f"Message: {response.message}")
    print(f"Worksheet URL: {response.spreadsheet_url}")

    if not response.success:
        print(f"❌ Test failed: {response.error}")
        return False

    # Verify response fields
    assert response.worksheet == "Products", f"Expected worksheet 'Products', got '{response.worksheet}'"
    assert response.shape == "(4,3)", f"Expected shape (4,3), got {response.shape}"  # 4 rows (including header) x 3 cols
    assert response.updated_cells == 12, f"Expected 12 cells updated, got {response.updated_cells}"  # 4 rows x 3 cols

    print("\n✅ All assertions passed!")

    # Test with list of dicts
    print("\nTesting write_new_worksheet with list of dicts...")
    dict_data = [
        {"Name": "John Doe", "Email": "john@example.com", "Role": "Developer"},
        {"Name": "Jane Smith", "Email": "jane@example.com", "Role": "Designer"},
    ]

    response2 = await google_sheet.write_new_worksheet(
        service=service,
        uri=spreadsheet_url,
        data=dict_data,
        worksheet_name="Users"
    )

    print(f"\nSecond worksheet created: {response2.worksheet}")
    print(f"Shape: {response2.shape}")
    print(f"Message: {response2.message}")

    if not response2.success:
        print(f"❌ Second test failed: {response2.error}")
        return False

    # Test creating duplicate worksheet (should use existing)
    print("\nTesting with existing worksheet name...")
    response3 = await google_sheet.write_new_worksheet(
        service=service,
        uri=spreadsheet_url,
        data=[["Updated", "Data"], ["New", "Values"]],
        worksheet_name="Products"  # Same name as first test
    )

    print(f"\nExisting worksheet used: {response3.worksheet}")
    print(f"Message: {response3.message}")

    if not response3.success:
        print(f"❌ Third test failed: {response3.error}")
        return False

    print("\n✅ All tests passed!")
    print(f"\n📊 View the test spreadsheet: {spreadsheet_url}")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test write_new_worksheet functionality")
    args = parser.parse_args()

    print("=" * 80)
    print("Testing write_new_worksheet MCP tool")
    print("=" * 80)

    success = asyncio.run(test_write_new_worksheet())

    if success:
        print("\n✅ Test completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Test failed!")
        sys.exit(1)
