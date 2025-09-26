#!/usr/bin/env python3
"""
Integration test for DataTable MCP with Spreadsheet API integration
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

from datatable_tools.lifecycle_tools import create_table, load_table, list_tables
from datatable_tools.export_tools import export_table

# Test configuration from .env
TEST_USER_ID = os.getenv("TEST_USER_ID", "68501372a3569b6897673a48")
EXAMPLE_SPREADSHEET_ID = os.getenv("EXAMPLE_SPREADSHEET_ID", "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms")
SPREADSHEET_API_ENDPOINT = os.getenv('SPREADSHEET_API', 'http://localhost:9394')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_create_sample_table():
    """Test creating a sample table"""
    print("🧪 Test 1: Creating a sample table")
    print("=" * 50)

    try:
        # Create a sample table
        sample_data = [
            ["Alice", "25", "Engineer"],
            ["Bob", "30", "Designer"],
            ["Charlie", "28", "Manager"]
        ]
        headers = ["Name", "Age", "Role"]

        result = await create_table(
            data=sample_data,
            headers=headers,
            name="Sample Employee Data"
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


async def test_load_from_spreadsheet():
    """Test loading data from Google Spreadsheet"""
    print("\n🧪 Test 2: Loading data from Google Spreadsheet")
    print("=" * 50)

    try:
        result = await load_table(
            source_type="google_sheets",
            source_path=EXAMPLE_SPREADSHEET_ID,
            user_id=TEST_USER_ID,
            sheet_name="Class Data",
            name="Loaded from Google Sheets"
        )

        if result["success"]:
            print(f"✅ SUCCESS: Loaded table {result['table_id']}")
            print(f"   - Name: {result['name']}")
            print(f"   - Shape: {result['shape']}")
            print(f"   - Headers: {result['headers']}")
            print(f"   - Source info: {result.get('source_info', {})}")
            return result["table_id"]
        else:
            print(f"❌ ERROR: {result['error']}")
            print(f"   - Message: {result['message']}")
            return None

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return None


async def test_export_to_spreadsheet(table_id):
    """Test exporting data to Google Spreadsheet"""
    if not table_id:
        print("\n⏭️  Skipping export test - no table to export")
        return

    print(f"\n🧪 Test 3: Exporting table {table_id} to Google Spreadsheet")
    print("=" * 50)

    try:
        result = await export_table(
            table_id=table_id,
            export_format="google_sheets",
            user_id=TEST_USER_ID,
            spreadsheet_name=f"DataTable Export Test",
            worksheet_id="Sheet1"
        )

        if result["success"]:
            print(f"✅ SUCCESS: Exported to spreadsheet {result.get('spreadsheet_id')}")
            print(f"   - Worksheet: {result.get('worksheet')}")
            print(f"   - Updated range: {result.get('updated_range')}")
            print(f"   - Updated cells: {result.get('updated_cells')}")
            print(f"   - Rows exported: {result.get('rows_exported')}")
            print(f"   - Message: {result.get('message')}")
            if 'worksheet_url' in result:
                print(f"   - Worksheet URL: {result['worksheet_url']}")
        else:
            print(f"❌ ERROR: {result['error']}")
            print(f"   - Message: {result['message']}")

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")


async def test_list_tables():
    """Test listing all tables"""
    print("\n🧪 Test 4: Listing all tables")
    print("=" * 50)

    try:
        result = await list_tables()

        if result["success"]:
            print(f"✅ SUCCESS: Found {result['count']} table(s)")
            for table in result["tables"]:
                print(f"   - {table['table_id']}: {table['name']} {table['shape']}")
        else:
            print(f"❌ ERROR: {result['error']}")

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")


async def test_export_to_csv(table_id):
    """Test exporting to CSV (as a control test)"""
    if not table_id:
        print("\n⏭️  Skipping CSV export test - no table to export")
        return

    print(f"\n🧪 Test 5: Exporting table {table_id} to CSV")
    print("=" * 50)

    try:
        result = await export_table(
            table_id=table_id,
            export_format="csv",
            return_content=True
        )

        if result["success"]:
            print(f"✅ SUCCESS: Exported to CSV")
            print(f"   - Rows exported: {result.get('rows_exported')}")
            print(f"   - Columns exported: {result.get('columns_exported')}")
            print(f"   - Content preview: {result.get('content', '')[:100]}...")
        else:
            print(f"❌ ERROR: {result['error']}")

    except Exception as e:
        print(f"❌ EXCEPTION: {e}")


async def run_integration_tests():
    """Run all integration tests"""
    print("🚀 Starting DataTable MCP + Spreadsheet API Integration Tests")
    print("=" * 70)
    print(f"Test user ID: {TEST_USER_ID}")
    print(f"Spreadsheet API endpoint: {SPREADSHEET_API_ENDPOINT}")
    print(f"Example spreadsheet ID: {EXAMPLE_SPREADSHEET_ID}")
    print("=" * 70)

    # Test 1: Create sample table
    sample_table_id = await test_create_sample_table()

    # Test 2: Load from spreadsheet
    loaded_table_id = await test_load_from_spreadsheet()

    # Test 3: Export to spreadsheet (use sample table if load failed)
    export_table_id = loaded_table_id or sample_table_id
    await test_export_to_spreadsheet(export_table_id)

    # Test 4: List all tables
    await test_list_tables()

    # Test 5: Export to CSV (control test)
    await test_export_to_csv(sample_table_id)

    print("\n🏁 Integration tests completed!")
    print("=" * 70)


if __name__ == "__main__":
    # Set environment variable if not set
    if not os.getenv('SPREADSHEET_API'):
        os.environ['SPREADSHEET_API'] = 'http://localhost:9394'

    # Run the tests
    asyncio.run(run_integration_tests())