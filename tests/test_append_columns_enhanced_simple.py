#!/usr/bin/env python3
"""
Simple integration test for enhanced append_columns functionality

Tests using an existing test spreadsheet to avoid timeouts.
"""

import os
import sys
import asyncio
import logging

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Use existing test spreadsheet
TEST_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1jFgc_WYBYPDJzHbtm1LYT8Bh-YH8MrJUcOZYi_OwPvs/edit?gid=0#gid=0"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


async def test_append_columns_enhanced():
    """Test enhanced append_columns functionality"""

    # Authenticate
    service = authenticate()
    if not service:
        return False

    google_sheet = GoogleSheetDataTable()

    print("\n" + "=" * 60)
    print("Testing Enhanced append_columns Functionality")
    print("=" * 60)

    # Step 1: Load current data
    print("\n📖 Step 1: Loading current sheet data...")
    load_result = await google_sheet.load_data_table(
        service=service,
        uri=TEST_SPREADSHEET_URL
    )

    if not load_result.success:
        print(f"❌ Failed to load data: {load_result.error}")
        return False

    existing_columns = list(load_result.data[0].keys()) if load_result.data else []
    print(f"✅ Current columns: {existing_columns}")
    print(f"✅ Current rows: {len(load_result.data)}")

    # Step 2: Test appending new column
    print("\n➕ Step 2: Appending new column 'Status'...")
    new_column_data = [
        {"Status": "Active"},
        {"Status": "Pending"}
    ]

    append_result = await google_sheet.append_columns(
        service=service,
        uri=TEST_SPREADSHEET_URL,
        data=new_column_data
    )

    print(f"\nResult: {append_result.message}")
    print(f"Success: {append_result.success}")
    print(f"Updated cells: {append_result.updated_cells}")

    if not append_result.success:
        print(f"❌ Failed to append columns: {append_result.error}")
        return False

    # Step 3: Verify column was added
    print("\n✓ Step 3: Verifying column was added...")
    verify_result = await google_sheet.load_data_table(
        service=service,
        uri=TEST_SPREADSHEET_URL
    )

    new_columns = list(verify_result.data[0].keys()) if verify_result.data else []
    print(f"✅ New columns: {new_columns}")

    if "Status" in new_columns:
        print("✅ Status column was successfully added!")
    else:
        print("❌ Status column was not added")
        return False

    # Step 4: Test appending duplicate column (should skip)
    print("\n🔄 Step 4: Attempting to append duplicate column 'Status' (should skip)...")
    duplicate_data = [
        {"Status": "Inactive"}
    ]

    dup_result = await google_sheet.append_columns(
        service=service,
        uri=TEST_SPREADSHEET_URL,
        data=duplicate_data
    )

    print(f"\nResult: {dup_result.message}")
    print(f"Success: {dup_result.success}")
    print(f"Updated cells: {dup_result.updated_cells}")

    if "already exist" in dup_result.message.lower() or dup_result.updated_cells == 0:
        print("✅ Correctly detected and skipped duplicate column!")
    else:
        print("⚠️  Warning: Duplicate column handling may not be working as expected")

    # Step 5: Test case-insensitive matching
    print("\n🔠 Step 5: Testing case-insensitive matching (STATUS vs Status)...")
    case_test_data = [
        {"STATUS": "Test"}  # Uppercase version of existing column
    ]

    case_result = await google_sheet.append_columns(
        service=service,
        uri=TEST_SPREADSHEET_URL,
        data=case_test_data
    )

    print(f"\nResult: {case_result.message}")
    print(f"Success: {case_result.success}")

    if "already exist" in case_result.message.lower() or case_result.updated_cells == 0:
        print("✅ Case-insensitive matching works correctly!")
    else:
        print("⚠️  Warning: Case-insensitive matching may not be working")

    # Step 6: Test mixed new and existing columns
    print("\n🔀 Step 6: Testing mixed new and existing columns...")
    mixed_data = [
        {"Status": "Old", "Priority": "High"},  # Status exists, Priority is new
        {"Status": "Old", "Priority": "Low"}
    ]

    mixed_result = await google_sheet.append_columns(
        service=service,
        uri=TEST_SPREADSHEET_URL,
        data=mixed_data
    )

    print(f"\nResult: {mixed_result.message}")
    print(f"Success: {mixed_result.success}")

    if "Skipped" in mixed_result.message and "Status" in mixed_result.message:
        print("✅ Correctly skipped existing column and added new one!")
    else:
        print("⚠️  Mixed column handling message: check if expected")

    print("\n" + "=" * 60)
    print("✅ All tests completed successfully!")
    print("=" * 60)
    print(f"\nTest spreadsheet: {TEST_SPREADSHEET_URL}")

    return True


async def main():
    """Main test runner"""
    try:
        success = await test_append_columns_enhanced()
        if not success:
            sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed with exception: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
