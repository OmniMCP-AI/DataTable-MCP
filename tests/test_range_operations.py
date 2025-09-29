#!/usr/bin/env python3
"""
Test script for new detailed spreadsheet operations using /range/update
"""

import asyncio
import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

from datatable_tools.lifecycle_tools import create_table
from datatable_tools.detailed_tools import (
    update_spreadsheet_cell,
    update_spreadsheet_row,
    update_spreadsheet_column,
    export_table_to_range
)

# Test configuration from .env
TEST_USER_ID = os.getenv("TEST_USER_ID", "68501372a3569b6897673a48")
EXAMPLE_SPREADSHEET_ID = os.getenv("EXAMPLE_SPREADSHEET_ID", "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_create_sample_table():
    """Create a sample table for testing exports"""
    print("🧪 Test 1: Creating a sample table for range operations")
    print("=" * 60)

    try:
        # Create a sample table with more data
        sample_data = [
            ["Alice", "25", "Engineer", "San Francisco"],
            ["Bob", "30", "Designer", "New York"],
            ["Charlie", "28", "Manager", "Chicago"],
            ["Diana", "32", "Developer", "Austin"],
            ["Eve", "27", "Analyst", "Seattle"]
        ]
        headers = ["Name", "Age", "Role", "City"]

        result = await create_table(
            data=sample_data,
            headers=headers,
            name="Employee Data for Range Tests"
        )

        if result["success"]:
            print(f"✅ SUCCESS: Created table {result['table_id']}")
            print(f"   - Name: {result['name']}")
            print(f"   - Shape: {result['shape']}")
            print(f"   - Headers: {result['headers']}")
            return result["table_id"]
        else:
            print(f"❌ ERROR: {result['error']}")
            return None

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return None


async def test_update_cell():
    """Test updating a single cell"""
    print("\n🧪 Test 2: Updating a single cell")
    print("=" * 60)

    try:
        result = await update_spreadsheet_cell(
            spreadsheet_id=EXAMPLE_SPREADSHEET_ID,
            worksheet="Class Data",
            cell_address="A1",
            value="Updated Cell Value",
            user_id=TEST_USER_ID
        )

        if result["success"]:
            print(f"✅ SUCCESS: Updated cell")
            print(f"   - Spreadsheet: {result['spreadsheet_id']}")
            print(f"   - Worksheet: {result['worksheet']}")
            print(f"   - Range: {result['updated_range']}")
            print(f"   - Cells updated: {result['updated_cells']}")
            print(f"   - Message: {result['message']}")
        else:
            print(f"❌ ERROR: {result['error']}")
            print(f"   - Message: {result['message']}")

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")


async def test_update_row():
    """Test updating an entire row"""
    print("\n🧪 Test 3: Updating an entire row")
    print("=" * 60)

    try:
        result = await update_spreadsheet_row(
            spreadsheet_id=EXAMPLE_SPREADSHEET_ID,
            worksheet="Class Data",
            row_number=2,
            row_data=["John Doe", "Male", "Senior", "California", "Computer Science", "Basketball"],
            user_id=TEST_USER_ID
        )

        if result["success"]:
            print(f"✅ SUCCESS: Updated row {result['row_number']}")
            print(f"   - Spreadsheet: {result['spreadsheet_id']}")
            print(f"   - Worksheet: {result['worksheet']}")
            print(f"   - Range: {result['updated_range']}")
            print(f"   - Cells updated: {result['updated_cells']}")
            print(f"   - Columns updated: {result['columns_updated']}")
        else:
            print(f"❌ ERROR: {result['error']}")
            print(f"   - Message: {result['message']}")

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")


async def test_update_column():
    """Test updating an entire column"""
    print("\n🧪 Test 4: Updating an entire column")
    print("=" * 60)

    try:
        result = await update_spreadsheet_column(
            spreadsheet_id=EXAMPLE_SPREADSHEET_ID,
            worksheet="Class Data",
            column="A",
            column_data=["Name", "Alex", "Beth", "Carl", "Dana", "Evan"],
            user_id=TEST_USER_ID,
            start_row=1
        )

        if result["success"]:
            print(f"✅ SUCCESS: Updated column {result['column']}")
            print(f"   - Spreadsheet: {result['spreadsheet_id']}")
            print(f"   - Worksheet: {result['worksheet']}")
            print(f"   - Range: {result['updated_range']}")
            print(f"   - Cells updated: {result['updated_cells']}")
            print(f"   - Rows updated: {result['rows_updated']}")
        else:
            print(f"❌ ERROR: {result['error']}")
            print(f"   - Message: {result['message']}")

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")


async def test_update_table_range(table_id):
    """Test updating spreadsheet with table data in specific range"""
    if not table_id:
        print("\n⏭️  Skipping table range test - no table available")
        return

    print(f"\n🧪 Test 5: Updating spreadsheet range with table {table_id}")
    print("=" * 60)

    try:
        result = await export_table_to_range(
            table_id=table_id,
            spreadsheet_id=EXAMPLE_SPREADSHEET_ID,
            worksheet="Class Data",
            start_cell="A8",  # Place table starting at row 8
            user_id=TEST_USER_ID,
            include_headers=True
        )

        if result["success"]:
            print(f"✅ SUCCESS: Updated range with table data")
            print(f"   - Table: {result['table_name']}")
            print(f"   - Spreadsheet: {result['spreadsheet_id']}")
            print(f"   - Worksheet: {result['worksheet']}")
            print(f"   - Range: {result['updated_range']}")
            print(f"   - Cells updated: {result['updated_cells']}")
            print(f"   - Rows exported: {result['rows_exported']}")
            print(f"   - Columns exported: {result['columns_exported']}")
            print(f"   - Headers included: {result['included_headers']}")
            if 'worksheet_url' in result:
                print(f"   - Worksheet URL: {result['worksheet_url']}")
        else:
            print(f"❌ ERROR: {result['error']}")
            print(f"   - Message: {result['message']}")

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")


async def run_range_tests():
    """Run all range operation tests"""
    print("🚀 Starting DataTable MCP Range Operations Tests")
    print("=" * 70)
    print(f"Test user ID: {TEST_USER_ID}")
    print(f"Test spreadsheet ID: {EXAMPLE_SPREADSHEET_ID}")
    print("=" * 70)

    # Test 1: Create sample table
    sample_table_id = await test_create_sample_table()

    # Test 2: Update single cell
    await test_update_cell()

    # Test 3: Update row
    await test_update_row()

    # Test 4: Update column
    await test_update_column()

    # Test 5: Update table range
    await test_update_table_range(sample_table_id)

    print("\n🏁 Range operation tests completed!")
    print("=" * 70)


if __name__ == "__main__":
    # Run the tests
    asyncio.run(run_range_tests())