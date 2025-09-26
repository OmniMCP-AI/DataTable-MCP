#!/usr/bin/env python3
"""
Updated integration test for DataTable MCP with LOCAL Spreadsheet implementation
"""

import asyncio
import os
import sys
import logging
from pathlib import Path

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

from datatable_tools.third_party.spreadsheet import (
    SpreadsheetClient,
    ReadSheetRequest,
    WriteSheetRequest,
    WorkSheetInfo
)

# Test configuration
TEST_USER_ID = "68501372a3569b6897673a48"

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


async def test_create_and_read_spreadsheet():
    """Test creating and reading a spreadsheet with our local implementation"""
    print("🧪 Test 1: Creating and reading a local spreadsheet")
    print("=" * 60)

    client = SpreadsheetClient()

    # Test data
    test_data = [
        ["Name", "Age", "Department", "Salary"],
        ["Alice Johnson", "28", "Engineering", "95000"],
        ["Bob Smith", "32", "Marketing", "78000"],
        ["Carol Davis", "29", "Design", "85000"],
        ["David Wilson", "35", "Sales", "92000"]
    ]

    try:
        # Write test
        write_request = WriteSheetRequest(
            spreadsheet_id="integration-test-001",
            worksheet=WorkSheetInfo(name="Employee_Data"),
            values=test_data
        )

        write_response = await client.write_sheet(write_request, TEST_USER_ID)

        if write_response.success:
            print(f"✅ WRITE SUCCESS: {write_response.message}")
            print(f"   - Updated range: {write_response.updated_range}")
            print(f"   - Updated cells: {write_response.updated_cells}")
            print(f"   - Worksheet URL: {write_response.worksheet_url}")
        else:
            print(f"❌ WRITE ERROR: {write_response.message}")
            return False

        # Read test
        read_request = ReadSheetRequest(
            spreadsheet_id="integration-test-001",
            worksheet=WorkSheetInfo(name="Employee_Data")
        )

        read_response = await client.read_sheet(read_request, TEST_USER_ID)

        if read_response.success:
            print(f"✅ READ SUCCESS: {read_response.message}")
            print(f"   - Used range: {read_response.used_range}")
            print(f"   - Row count: {read_response.row_count}")
            print(f"   - Column count: {read_response.column_count}")
            print(f"   - Headers: {read_response.headers}")

            # Validate data integrity
            if read_response.values == test_data:
                print("✅ DATA INTEGRITY: All data matches perfectly")
                return True
            else:
                print("❌ DATA INTEGRITY: Data mismatch detected")
                print(f"   Expected: {test_data[:2]}")
                print(f"   Got: {read_response.values[:2]}")
                return False
        else:
            print(f"❌ READ ERROR: {read_response.message}")
            return False

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


async def test_column_name_matching():
    """Test writing with column name matching"""
    print("\n🧪 Test 2: Column name matching functionality")
    print("=" * 60)

    client = SpreadsheetClient()

    try:
        # First, create a spreadsheet with headers
        headers_data = [
            ["Employee_ID", "Full_Name", "Email", "Phone"],
        ]

        write_request = WriteSheetRequest(
            spreadsheet_id="integration-test-002",
            worksheet=WorkSheetInfo(name="Contacts"),
            values=headers_data
        )

        write_response = await client.write_sheet(write_request, TEST_USER_ID)

        if not write_response.success:
            print(f"❌ SETUP ERROR: {write_response.message}")
            return False

        # Now write data using column name matching
        employee_data = [
            ["001", "John Doe", "john@company.com", "555-1234"],
            ["002", "Jane Smith", "jane@company.com", "555-5678"]
        ]

        write_request = WriteSheetRequest(
            spreadsheet_id="integration-test-002",
            worksheet=WorkSheetInfo(name="Contacts"),
            columns_name=["Employee_ID", "Full_Name", "Email", "Phone"],
            values=employee_data
        )

        write_response = await client.write_sheet(write_request, TEST_USER_ID)

        if write_response.success:
            print(f"✅ COLUMN MATCHING SUCCESS: {write_response.message}")
            print(f"   - Matched columns: {write_response.matched_columns}")
            print(f"   - Updated range: {write_response.updated_range}")
            return True
        else:
            print(f"❌ COLUMN MATCHING ERROR: {write_response.message}")
            return False

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


async def test_multiple_worksheets():
    """Test working with multiple worksheets in same spreadsheet"""
    print("\n🧪 Test 3: Multiple worksheets functionality")
    print("=" * 60)

    client = SpreadsheetClient()

    try:
        # Create data for different worksheets
        quarterly_data = {
            "Q1_Sales": [
                ["Product", "Sales", "Profit"],
                ["Product A", "50000", "12000"],
                ["Product B", "32000", "8500"]
            ],
            "Q2_Sales": [
                ["Product", "Sales", "Profit"],
                ["Product A", "55000", "13500"],
                ["Product B", "38000", "9200"]
            ]
        }

        success_count = 0

        for sheet_name, data in quarterly_data.items():
            write_request = WriteSheetRequest(
                spreadsheet_id="integration-test-003",
                worksheet=WorkSheetInfo(name=sheet_name),
                values=data
            )

            write_response = await client.write_sheet(write_request, TEST_USER_ID)

            if write_response.success:
                print(f"✅ Created worksheet '{sheet_name}': {write_response.message}")
                success_count += 1
            else:
                print(f"❌ Failed to create worksheet '{sheet_name}': {write_response.message}")

        return success_count == len(quarterly_data)

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


async def test_file_persistence():
    """Test that files are properly saved and can be accessed"""
    print("\n🧪 Test 4: File persistence and accessibility")
    print("=" * 60)

    try:
        spreadsheet_id = "persistence-test-001"
        temp_dir = Path("/tmp/datatable_spreadsheets")
        expected_file = temp_dir / f"{spreadsheet_id}.xlsx"

        client = SpreadsheetClient()

        # Create a spreadsheet
        write_request = WriteSheetRequest(
            spreadsheet_id=spreadsheet_id,
            worksheet=WorkSheetInfo(name="TestSheet"),
            values=[["Test", "Data"], ["Row1", "Value1"]]
        )

        write_response = await client.write_sheet(write_request, TEST_USER_ID)

        if not write_response.success:
            print(f"❌ WRITE ERROR: {write_response.message}")
            return False

        # Check if file exists
        if expected_file.exists():
            file_size = expected_file.stat().st_size
            print(f"✅ FILE PERSISTENCE: File created successfully")
            print(f"   - File path: {expected_file}")
            print(f"   - File size: {file_size} bytes")

            # Try to read it back
            read_request = ReadSheetRequest(
                spreadsheet_id=spreadsheet_id,
                worksheet=WorkSheetInfo(name="TestSheet")
            )

            read_response = await client.read_sheet(read_request, TEST_USER_ID)

            if read_response.success:
                print(f"✅ FILE ACCESSIBILITY: File can be read back successfully")
                return True
            else:
                print(f"❌ FILE ACCESSIBILITY ERROR: {read_response.message}")
                return False
        else:
            print(f"❌ FILE PERSISTENCE ERROR: File not found at {expected_file}")
            return False

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


async def run_comprehensive_tests():
    """Run all comprehensive integration tests"""
    print("🚀 DataTable MCP - LOCAL Spreadsheet Implementation Tests")
    print("=" * 80)
    print("Testing local Excel file-based spreadsheet operations")
    print(f"Test user ID: {TEST_USER_ID}")
    print(f"Storage location: /tmp/datatable_spreadsheets/")
    print("=" * 80)

    test_results = []

    # Run all tests
    tests = [
        ("Create and Read Spreadsheet", test_create_and_read_spreadsheet),
        ("Column Name Matching", test_column_name_matching),
        ("Multiple Worksheets", test_multiple_worksheets),
        ("File Persistence", test_file_persistence),
    ]

    for test_name, test_func in tests:
        try:
            result = await test_func()
            test_results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            test_results.append((test_name, False))

    # Summary
    print("\n📊 TEST RESULTS SUMMARY")
    print("=" * 80)
    passed = 0
    total = len(test_results)

    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status}: {test_name}")
        if result:
            passed += 1

    print("-" * 80)
    print(f"Overall Result: {passed}/{total} tests passed")

    # Generate clickable URLs for manual verification
    print(f"\n🌐 MANUAL VERIFICATION URLS:")
    print("═" * 80)

    # List all the spreadsheets created during tests
    spreadsheet_urls = {
        "integration-test-001": ["Employee_Data"],
        "integration-test-002": ["Contacts"],
        "integration-test-003": ["Q1_Sales", "Q2_Sales"],
        "persistence-test-001": ["TestSheet"]
    }

    for spreadsheet_id, worksheets in spreadsheet_urls.items():
        main_url = generate_sheets_url(spreadsheet_id)
        print(f"📋 {spreadsheet_id}: {main_url}")

        for worksheet_name in worksheets:
            worksheet_url = generate_sheets_url(spreadsheet_id, worksheet_name=worksheet_name)
            print(f"   └── 📄 {worksheet_name}: {worksheet_url}")

    print("═" * 80)
    print("💡 Click the URLs above to manually verify all spreadsheet data!")

    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Local spreadsheet implementation is fully functional.")
        return True
    else:
        print("\n⚠️  Some tests failed. Please review the implementation.")
        return False


if __name__ == "__main__":
    success = asyncio.run(run_comprehensive_tests())
    sys.exit(0 if success else 1)