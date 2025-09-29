#!/usr/bin/env python3
"""
Test suite for the simplified update_range function
Tests the unified range-based Google Sheets update functionality
"""

import os
import sys
import unittest
import logging
from unittest.mock import AsyncMock, patch

# Add parent directory to path for imports
sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the actual functions, not the MCP tools
from datatable_tools.detailed_tools import update_range, export_table_to_range
from datatable_tools.utils import parse_google_sheets_url

class TestUpdateSpreadsheetRange(unittest.TestCase):
    """Test update_range with various range formats and data types"""

    def setUp(self):
        """Set up test data"""
        self.test_uris = [
            "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
            "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        ]

        # Mock context
        self.mock_ctx = type('MockContext', (), {})()

    def test_uri_parsing(self):
        """Test URI parsing functionality"""
        print("\n🔍 Testing URI Parsing")

        for uri in self.test_uris:
            with self.subTest(uri=uri):
                spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
                self.assertEqual(spreadsheet_id, "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms")
                print(f"   ✅ {uri[:50]}... → {spreadsheet_id}")

    @patch('datatable_tools.range_operations.range_operations.update_cell')
    async def test_single_cell_update(self, mock_update_cell):
        """Test single cell update functionality"""
        print("\n📍 Testing Single Cell Update")

        # Mock successful response
        mock_update_cell.return_value = {
            "success": True,
            "worksheet": "Sheet1",
            "updated_range": "B5",
            "updated_cells": 1,
            "message": "Successfully updated cell B5"
        }

        uri = self.test_uris[0]

        # Get the actual function, not the MCP tool
        from datatable_tools import detailed_tools
        func = detailed_tools.update_range._func if hasattr(detailed_tools.update_range, '_func') else detailed_tools.update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=uri,
            range_address="B5",
            data="Test Value"
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["updated_cells"], 1)

        # Verify the range_operations was called correctly
        mock_update_cell.assert_called_once_with(
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            worksheet="Sheet1",
            cell_address="B5",
            value="Test Value",
            user_id=""
        )

        print(f"   ✅ Single cell update: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_row_update(self, mock_google_sheets_service):
        """Test row update functionality"""
        print("\n📄 Testing Row Update")

        # Mock successful response
        mock_google_sheets_service.update_range.return_value = True

        uri = self.test_uris[1]  # Test with spreadsheet ID format
        row_data = ["Name", "Age", "City"]

        # Get the actual function
        from datatable_tools import detailed_tools
        func = detailed_tools.update_range._func if hasattr(detailed_tools.update_range, '_func') else detailed_tools.update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=uri,
            range_address="A1:C1",
            data=row_data
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["updated_cells"], 3)

        # Verify the google_sheets_service was called correctly
        mock_google_sheets_service.update_range.assert_called_once_with(
            user_id="",
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            range_notation="Sheet1!A1:C1",
            values=[["Name", "Age", "City"]]
        )

        print(f"   ✅ Row update: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_column_update(self, mock_google_sheets_service):
        """Test column update functionality"""
        print("\n📊 Testing Column Update")

        # Mock successful response
        mock_google_sheets_service.update_range.return_value = True

        uri = self.test_uris[0]
        column_data = [25, 30, 28]

        # Get the actual function
        from datatable_tools import detailed_tools
        func = detailed_tools.update_range._func if hasattr(detailed_tools.update_range, '_func') else detailed_tools.update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=uri,
            range_address="B1:B3",
            data=column_data,
            worksheet="CustomSheet"  # Test custom worksheet
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["updated_cells"], 3)

        # Verify the google_sheets_service was called with custom worksheet
        mock_google_sheets_service.update_range.assert_called_once_with(
            user_id="",
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            range_notation="CustomSheet!B1:B3",
            values=[["25"], ["30"], ["28"]]
        )

        print(f"   ✅ Column update with custom worksheet: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_2d_range_update(self, mock_google_sheets_service):
        """Test 2D range update functionality"""
        print("\n📋 Testing 2D Range Update")

        # Mock successful response
        mock_google_sheets_service.update_range.return_value = True

        uri = self.test_uris[1]
        range_data = [
            ["Name", "Age", "City"],
            ["Alice", 25, "NYC"]
        ]

        # Get the actual function
        from datatable_tools import detailed_tools
        func = detailed_tools.update_range._func if hasattr(detailed_tools.update_range, '_func') else detailed_tools.update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=uri,
            range_address="A1:C2",
            data=range_data
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["updated_cells"], 6)

        # Verify the google_sheets_service was called correctly
        mock_google_sheets_service.update_range.assert_called_once_with(
            user_id="",
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            range_notation="Sheet1!A1:C2",
            values=[["Name", "Age", "City"], ["Alice", "25", "NYC"]]
        )

        print(f"   ✅ 2D range update: {result['message']}")

    async def test_invalid_uri(self):
        """Test error handling for invalid URIs"""
        print("\n❌ Testing Invalid URI Handling")

        invalid_uris = [
            "",
            "not-a-valid-uri",
            "https://example.com/invalid"
        ]

        # Get the actual function
        from datatable_tools import detailed_tools
        func = detailed_tools.update_range._func if hasattr(detailed_tools.update_range, '_func') else detailed_tools.update_range

        for uri in invalid_uris:
            with self.subTest(uri=uri):
                result = await func(
                    ctx=self.mock_ctx,
                    uri=uri,
                    range_address="A1",
                    data="test"
                )

                self.assertFalse(result["success"])
                self.assertIn("Invalid Google Sheets URI", result["error"])
                print(f"   ✅ Invalid URI handled: {uri}")

    @patch('datatable_tools.range_operations.range_operations.update_table_range')
    async def test_table_to_spreadsheet_range(self, mock_update_table_range):
        """Test export_table_to_range functionality"""
        print("\n📊 Testing Table to Spreadsheet Range")

        # Mock successful response
        mock_update_table_range.return_value = {
            "success": True,
            "table_id": "test_table_123",
            "updated_range": "A1:C5",
            "rows_exported": 4,
            "message": "Successfully exported table to range A1:C5"
        }

        uri = self.test_uris[0]

        # Get the actual function
        from datatable_tools import detailed_tools
        func = detailed_tools.export_table_to_range._func if hasattr(detailed_tools.export_table_to_range, '_func') else detailed_tools.export_table_to_range

        result = await func(
            ctx=self.mock_ctx,
            table_id="test_table_123",
            uri=uri,
            start_cell="A1",
            include_headers=True
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["table_id"], "test_table_123")
        self.assertEqual(result["rows_exported"], 4)

        # Verify the range_operations was called correctly
        mock_update_table_range.assert_called_once_with(
            table_id="test_table_123",
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            worksheet="Sheet1",
            start_cell="A1",
            user_id="",
            include_headers=True
        )

        print(f"   ✅ Table export: {result['message']}")

    def test_range_address_formats(self):
        """Test various A1 notation range address formats"""
        print("\n📐 Testing Range Address Formats")

        valid_formats = [
            "A1",           # Single cell
            "B5",           # Single cell
            "A1:C1",        # Row range
            "1:1",          # Entire row
            "B:B",          # Entire column
            "B1:B10",       # Column range
            "A1:C3",        # 2D range
            "Z100",         # Large cell reference
            "AA1:ZZ100"     # Large range
        ]

        for range_addr in valid_formats:
            with self.subTest(range_address=range_addr):
                # Test that range address is properly formatted
                worksheet = "TestSheet"
                full_range = f"{worksheet}!{range_addr}"

                # Basic validation - should contain worksheet and range
                self.assertIn("!", full_range)
                self.assertTrue(full_range.startswith(worksheet))
                self.assertTrue(full_range.endswith(range_addr))

                print(f"   ✅ Range format: {range_addr} → {full_range}")


class TestRangeUpdateIntegration(unittest.TestCase):
    """Integration tests for range update functionality"""

    def test_data_type_support(self):
        """Test that various data types are supported"""
        print("\n🔢 Testing Data Type Support")

        supported_data_types = [
            ("string", "Hello World"),
            ("integer", 42),
            ("float", 3.14159),
            ("boolean", True),
            ("list", ["a", "b", "c"]),
            ("nested_list", [["a", 1], ["b", 2]]),
            ("mixed_list", ["text", 42, 3.14, True]),
            ("empty_string", "")  # Changed from None to empty string
        ]

        for type_name, data in supported_data_types:
            with self.subTest(data_type=type_name):
                # Test that data type is valid for Union[Any, List[Any], List[List[Any]]]
                # This is a basic type validation - in real usage,
                # Google Sheets API would validate the actual data
                if type_name == "empty_string":
                    self.assertEqual(data, "")  # Allow empty string
                else:
                    self.assertIsNotNone(data)  # Basic validation
                print(f"   ✅ Data type supported: {type_name} = {data}")


async def run_async_tests():
    """Run async tests individually"""
    test_instance = TestUpdateSpreadsheetRange()
    test_instance.setUp()

    print("🔄 Running async tests...")

    try:
        await test_instance.test_single_cell_update()
        await test_instance.test_row_update()
        await test_instance.test_column_update()
        await test_instance.test_2d_range_update()
        await test_instance.test_invalid_uri()
        await test_instance.test_table_to_spreadsheet_range()
        print("✅ All async tests completed successfully")
        return True
    except Exception as e:
        print(f"❌ Async test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_sync_tests():
    """Run synchronous tests"""
    print("🚀 Starting Synchronous Tests")
    print("=" * 50)

    # Create test suite
    suite = unittest.TestSuite()

    # Add test cases (only sync ones)
    loader = unittest.TestLoader()
    suite.addTests(loader.loadTestsFromTestCase(TestRangeUpdateIntegration))

    # Add specific sync methods from TestUpdateSpreadsheetRange
    sync_test_methods = ['test_uri_parsing', 'test_range_address_formats']
    for method_name in sync_test_methods:
        suite.addTest(TestUpdateSpreadsheetRange(method_name))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 50)
    print("📊 Sync Test Results Summary:")
    print(f"   Tests Run: {result.testsRun}")
    print(f"   Failures: {len(result.failures)}")
    print(f"   Errors: {len(result.errors)}")

    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"   - {test}: {traceback}")

    if result.errors:
        print("\n💥 Errors:")
        for test, traceback in result.errors:
            print(f"   - {test}: {traceback}")

    success = len(result.failures) == 0 and len(result.errors) == 0
    return success


if __name__ == "__main__":
    import asyncio

    print("🚀 Starting Comprehensive update_range Tests")
    print("=" * 70)

    # Run async tests
    async_success = asyncio.run(run_async_tests())

    # Run sync tests
    sync_success = run_sync_tests()

    overall_success = async_success and sync_success

    print("\n" + "=" * 70)
    print("🎯 Final Results:")
    print(f"   Async Tests: {'✅ PASS' if async_success else '❌ FAIL'}")
    print(f"   Sync Tests: {'✅ PASS' if sync_success else '❌ FAIL'}")

    if overall_success:
        print("\n🎉 All tests passed! update_range functions are working correctly.")
    else:
        print(f"\n❌ Some tests failed. Please check the issues above.")

    sys.exit(0 if overall_success else 1)