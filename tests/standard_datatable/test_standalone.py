#!/usr/bin/env python3
"""
Standalone GoogleSheetDataTable Tests (No FastMCP)

Tests that GoogleSheetDataTable can be used without FastMCP framework.
Demonstrates framework-agnostic nature of Stage 4.2 refactoring.

Usage:
    python tests/standard_datatable/test_standalone.py

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

import asyncio
import os
from datetime import datetime

from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable
from datatable_tools.auth.service_factory import create_google_service_from_env


# Test URIs (same as MCP tests for consistency)
READ_ONLY_URI = "https://docs.google.com/spreadsheets/d/1DpaI7L4yfYptsv6X2TL0InhVbeFfe2TpZPPoY98llR0/edit?gid=1411021775#gid=1411021775"
READ_WRITE_URI = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=265933634#gid=265933634"


def create_google_service():
    """Create Google Sheets service using OAuth credentials (no FastMCP)"""
    # Use the new service factory
    return create_google_service_from_env(env_prefix="TEST_GOOGLE_OAUTH")


async def test_case_1_load_data():
    """Test Case 1: Load data from Google Sheets (no FastMCP)"""
    print("\n" + "="*60)
    print("Test Case 1: Load Data (Standalone - No FastMCP)")
    print("="*60)

    # Create Google service using standard Google API
    service = create_google_service()

    # Instantiate GoogleSheetDataTable (no FastMCP needed!)
    google_sheet = GoogleSheetDataTable()

    # Call load_data_table directly - just pass service and URI
    result = await google_sheet.load_data_table(
        service=service,
        uri=READ_ONLY_URI
    )

    # Verify result (result is a Pydantic model)
    if result.success:
        print(f"✅ Successfully loaded table")
        print(f"   Table ID: {result.table_id}")
        print(f"   Shape: {result.shape}")
        print(f"   Data rows: {len(result.data)}")
        return True
    else:
        print(f"❌ Failed to load table: {result.error}")
        return False


async def test_case_2_update_range():
    """Test Case 2: Update a range in Google Sheets (no FastMCP)"""
    print("\n" + "="*60)
    print("Test Case 2: Update Range (Standalone - No FastMCP)")
    print("="*60)

    # Create Google service using standard Google API
    service = create_google_service()

    # Instantiate GoogleSheetDataTable
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Call update_range directly - no Context, just service + parameters
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=[[f"Updated standalone: {timestamp}"]],
        range_address="F1"
    )

    # Verify result (result is a Pydantic model)
    if result.success:
        print(f"✅ Successfully updated range")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Updated cells: {result.updated_cells}")
        return True
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def test_case_3_create_new_sheet():
    """Test Case 3: Create new spreadsheet (no FastMCP)"""
    print("\n" + "="*60)
    print("Test Case 3: Create New Sheet (Standalone - No FastMCP)")
    print("="*60)

    # Create Google service using standard Google API
    service = create_google_service()

    # Instantiate GoogleSheetDataTable
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Call write_new_sheet directly
    result = await google_sheet.write_new_sheet(
        service=service,
        data=[
            ["Product Name", "Price", "Stock"],
            ["Laptop", 999.99, 15],
            ["Mouse", 25.99, 50],
            ["Keyboard", 79.99, 30]
        ],
        headers=None,  # Will auto-detect from first row
        sheet_name=f"Standalone Test {timestamp}"
    )

    # Verify result (result is a Pydantic model)
    if result.success:
        print(f"✅ Successfully created new spreadsheet")
        print(f"   URL: {result.spreadsheet_url}")
        print(f"   Rows created: {result.rows_created}")
        print(f"   Columns created: {result.columns_created}")
        return True
    else:
        print(f"❌ Failed to create spreadsheet: {result.error}")
        return False


async def run_all_tests():
    """Run all standalone tests"""
    print("\n" + "="*60)
    print("🚀 Standalone GoogleSheetDataTable Tests (No FastMCP)")
    print("="*60)
    print("\nThis demonstrates that GoogleSheetDataTable is framework-agnostic")
    print("and can be used in any Python project without FastMCP.\n")

    results = []

    # Run test cases
    try:
        result1 = await test_case_1_load_data()
        results.append(("Load Data", result1))

        result2 = await test_case_2_update_range()
        results.append(("Update Range", result2))

        result3 = await test_case_3_create_new_sheet()
        results.append(("Create New Sheet", result3))

    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Summary
    print("\n" + "="*60)
    print("📊 Test Summary")
    print("="*60)

    all_passed = True
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n🎉 All standalone tests passed!")
        print("\n✅ GoogleSheetDataTable works without FastMCP")
        print("✅ Framework-agnostic implementation successful")
    else:
        print("\n⚠️  Some tests failed")

    return all_passed


if __name__ == "__main__":
    # Run the standalone tests
    success = asyncio.run(run_all_tests())

    # Exit with appropriate code
    exit(0 if success else 1)
