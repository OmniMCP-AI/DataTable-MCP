#!/usr/bin/env python3
"""
Integration test for DataTable MCP with LOCAL Spreadsheet implementation
NOTE: Updated to work with local file-based spreadsheet operations
"""

import asyncio
import os
import sys
import logging
from pathlib import Path

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

# Import direct client for testing (bypasses MCP server framework issues)
from datatable_tools.third_party.spreadsheet import (
    SpreadsheetClient,
    ReadSheetRequest,
    WriteSheetRequest,
    WorkSheetInfo
)

# Test configuration
TEST_USER_ID = os.getenv("TEST_USER_ID", "68501372a3569b6897673a48")
# Note: Using local test spreadsheet IDs since we no longer use external API
LOCAL_TEST_SPREADSHEET = "mcp-integration-test-001"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_sheets_url(spreadsheet_id, worksheet_id=None, worksheet_name=None):
    """Generate a Google Sheets URL for manual verification"""
    base_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/"
    if worksheet_id:
        return base_url + f"edit#gid={worksheet_id}"
    elif worksheet_name:
        return base_url + f"edit#worksheet={worksheet_name}"
    else:
        return base_url + "edit"


async def test_create_sample_spreadsheet():
    """Test creating a sample spreadsheet with our local implementation"""
    print("🧪 Test 1: Creating a sample spreadsheet (local implementation)")
    print("=" * 50)

    try:
        client = SpreadsheetClient()

        # Create sample data
        sample_data = [
            ["Name", "Age", "Role"],
            ["Alice", "25", "Engineer"],
            ["Bob", "30", "Designer"],
            ["Charlie", "28", "Manager"]
        ]

        write_request = WriteSheetRequest(
            spreadsheet_id=LOCAL_TEST_SPREADSHEET,
            worksheet=WorkSheetInfo(name="Sample_Data"),
            values=sample_data
        )

        response = await client.write_sheet(write_request, TEST_USER_ID)

        if response.success:
            print(f"✅ SUCCESS: Created local spreadsheet")
            print(f"   - Spreadsheet ID: {response.spreadsheet_id}")
            print(f"   - Worksheet: {response.worksheet.name}")
            print(f"   - Updated range: {response.updated_range}")
            print(f"   - Updated cells: {response.updated_cells}")
            print(f"   - File URL: {response.worksheet_url}")

            # Generate Google Sheets URL for manual checking
            sheets_url = f"https://docs.google.com/spreadsheets/d/{response.spreadsheet_id}/"
            if hasattr(response.worksheet, 'id') and response.worksheet.id:
                sheets_url += f"edit#gid={response.worksheet.id}"
            else:
                sheets_url += "edit"
            print(f"   - 🌐 Google Sheets URL: {sheets_url}")
            print(f"   - 📋 Click to view: {sheets_url}")

            return response.spreadsheet_id
        else:
            print(f"❌ ERROR: {response.message}")
            return None

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return None


async def test_read_from_spreadsheet(spreadsheet_id):
    """Test reading data from local spreadsheet"""
    if not spreadsheet_id:
        print("\n⏭️  Skipping read test - no spreadsheet to read from")
        return None

    print(f"\n🧪 Test 2: Reading data from local spreadsheet {spreadsheet_id}")
    print("=" * 50)

    try:
        client = SpreadsheetClient()

        read_request = ReadSheetRequest(
            spreadsheet_id=spreadsheet_id,
            worksheet=WorkSheetInfo(name="Sample_Data")
        )

        response = await client.read_sheet(read_request, TEST_USER_ID)

        if response.success:
            print(f"✅ SUCCESS: Read data from local spreadsheet")
            print(f"   - Used range: {response.used_range}")
            print(f"   - Row count: {response.row_count}")
            print(f"   - Column count: {response.column_count}")
            print(f"   - Headers: {response.headers}")
            print(f"   - Sample data: {response.values[:2] if response.values else 'No data'}")

            # Generate Google Sheets URL for manual checking
            sheets_url = f"https://docs.google.com/spreadsheets/d/{response.spreadsheet_id}/"
            if hasattr(response.worksheet, 'id') and response.worksheet.id:
                sheets_url += f"edit#gid={response.worksheet.id}"
            else:
                sheets_url += "edit"
            print(f"   - 🌐 Google Sheets URL: {sheets_url}")
            print(f"   - 📋 Click to view: {sheets_url}")

            return True
        else:
            print(f"❌ ERROR: {response.message}")
            return False

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


async def test_write_additional_data(spreadsheet_id):
    """Test writing additional data to existing spreadsheet"""
    if not spreadsheet_id:
        print("\n⏭️  Skipping additional write test - no spreadsheet available")
        return False

    print(f"\n🧪 Test 3: Writing additional data to spreadsheet {spreadsheet_id}")
    print("=" * 50)

    try:
        client = SpreadsheetClient()

        # Add more employee data
        additional_data = [
            ["Diana", "35", "Director"],
            ["Eve", "29", "Analyst"],
            ["Frank", "31", "Developer"]
        ]

        write_request = WriteSheetRequest(
            spreadsheet_id=spreadsheet_id,
            worksheet=WorkSheetInfo(name="Additional_Staff"),
            values=[["Name", "Age", "Role"]] + additional_data  # Include headers
        )

        response = await client.write_sheet(write_request, TEST_USER_ID)

        if response.success:
            print(f"✅ SUCCESS: Added data to new worksheet")
            print(f"   - Worksheet: {response.worksheet.name}")
            print(f"   - Updated range: {response.updated_range}")
            print(f"   - Updated cells: {response.updated_cells}")

            # Generate Google Sheets URL for the new worksheet
            sheets_url = f"https://docs.google.com/spreadsheets/d/{response.spreadsheet_id}/"
            if hasattr(response.worksheet, 'id') and response.worksheet.id:
                sheets_url += f"edit#gid={response.worksheet.id}"
            else:
                sheets_url += "edit"
            print(f"   - 🌐 Google Sheets URL (New Worksheet): {sheets_url}")
            print(f"   - 📋 Click to view new worksheet: {sheets_url}")

            return True
        else:
            print(f"❌ ERROR: {response.message}")
            return False

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


async def test_file_storage_info():
    """Test displaying file storage information"""
    print("\n🧪 Test 4: File storage information")
    print("=" * 50)

    try:
        temp_dir = Path("/tmp/datatable_spreadsheets")
        if temp_dir.exists():
            files = list(temp_dir.glob("*.xlsx"))
            print(f"✅ SUCCESS: Found {len(files)} spreadsheet file(s)")
            for file in files[:5]:  # Show first 5 files
                size = file.stat().st_size
                print(f"   - {file.name} ({size} bytes)")
            return True
        else:
            print("❌ ERROR: Storage directory not found")
            return False

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


async def run_integration_tests():
    """Run all integration tests"""
    print("🚀 Starting DataTable MCP + LOCAL Spreadsheet Implementation Tests")
    print("=" * 70)
    print(f"Test user ID: {TEST_USER_ID}")
    print(f"Local storage: /tmp/datatable_spreadsheets/")
    print(f"Test spreadsheet ID: {LOCAL_TEST_SPREADSHEET}")
    print("=" * 70)

    # Test 1: Create sample spreadsheet
    spreadsheet_id = await test_create_sample_spreadsheet()

    # Test 2: Read from spreadsheet
    read_success = await test_read_from_spreadsheet(spreadsheet_id)

    # Test 3: Write additional data
    write_success = await test_write_additional_data(spreadsheet_id)

    # Test 4: Show file storage info
    storage_success = await test_file_storage_info()

    print("\n🏁 Integration tests completed!")
    print("=" * 70)

    # Summary
    tests = [
        ("Create Spreadsheet", spreadsheet_id is not None),
        ("Read Spreadsheet", read_success),
        ("Write Additional Data", write_success),
        ("File Storage", storage_success)
    ]

    passed = sum(1 for _, success in tests if success)
    total = len(tests)

    print(f"\n📊 RESULTS: {passed}/{total} tests passed")
    for test_name, success in tests:
        status = "✅" if success else "❌"
        print(f"  {status} {test_name}")

    # Generate clickable URLs for manual verification
    if spreadsheet_id:
        print(f"\n🌐 MANUAL VERIFICATION URLS:")
        print(f"═" * 50)

        # Main spreadsheet URL
        main_url = generate_sheets_url(spreadsheet_id)
        print(f"📋 Main Spreadsheet: {main_url}")

        # Sample Data worksheet
        sample_url = generate_sheets_url(spreadsheet_id, worksheet_name="Sample_Data")
        print(f"📋 Sample Data Sheet: {sample_url}")

        # Additional Staff worksheet
        additional_url = generate_sheets_url(spreadsheet_id, worksheet_name="Additional_Staff")
        print(f"📋 Additional Staff Sheet: {additional_url}")

        print(f"═" * 50)
        print("💡 Click the URLs above to manually verify the spreadsheet data!")

    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Local spreadsheet implementation working perfectly.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review the implementation.")


if __name__ == "__main__":
    # Run the tests
    asyncio.run(run_integration_tests())