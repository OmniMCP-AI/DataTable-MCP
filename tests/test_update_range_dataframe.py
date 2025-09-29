#!/usr/bin/env python3
"""
Test suite for the enhanced update_range function with DataFrame-like parameter support
Tests various data formats similar to pandas DataFrame and create_table
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

# Import the actual functions
from datatable_tools.detailed_tools import update_range
from datatable_tools.utils import parse_google_sheets_url


class TestUpdateRangeDataFrame(unittest.TestCase):
    """Test update_range with DataFrame-like data formats"""

    def setUp(self):
        """Set up test data"""
        self.test_uri = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"
        self.mock_ctx = type('MockContext', (), {})()

    @patch('datatable_tools.range_operations.range_operations.update_cell')
    async def test_scalar_data(self, mock_update_cell):
        """Test scalar data input"""
        print("\n📊 Testing Scalar Data")

        # Mock successful response for single cell update
        mock_update_cell.return_value = {
            "success": True,
            "worksheet": "Sheet1",
            "updated_range": "A1",
            "updated_cells": 1,
            "message": "Successfully updated cell A1"
        }

        # Get the actual function (handle MCP wrapper)
        func = update_range.fn if hasattr(update_range, 'fn') else update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=self.test_uri,
            range_address="A1",
            data=42
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["updated_cells"], 1)
        print(f"   ✅ Scalar: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_dict_column_data(self, mock_google_sheets_service):
        """Test dictionary with column data"""
        print("\n📊 Testing Dictionary Column Data")

        # Make the mock async
        mock_google_sheets_service.update_range = AsyncMock(return_value=True)

        data = {
            "Name": ["Alice", "Bob", "Charlie"],
            "Age": [25, 30, 28],
            "City": ["NYC", "LA", "Chicago"]
        }

        # Get the actual function (handle MCP wrapper)
        func = update_range.fn if hasattr(update_range, 'fn') else update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=self.test_uri,
            range_address="A1:C3",
            data=data
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["data_shape"], (3, 3))

        # Verify the Google Sheets service was called correctly
        mock_google_sheets_service.update_range.assert_called_once()
        call_args = mock_google_sheets_service.update_range.call_args
        values = call_args[1]["values"]

        # Should have 3 rows, 3 columns
        self.assertEqual(len(values), 3)
        self.assertEqual(len(values[0]), 3)

        print(f"   ✅ Dict columns: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_records_format(self, mock_google_sheets_service):
        """Test list of dictionaries (records format)"""
        print("\n📊 Testing Records Format")

        # Make the mock async
        mock_google_sheets_service.update_range = AsyncMock(return_value=True)

        data = [
            {"Name": "Alice", "Age": 25, "City": "NYC"},
            {"Name": "Bob", "Age": 30, "City": "LA"}
        ]

        # Get the actual function (handle MCP wrapper)
        func = update_range.fn if hasattr(update_range, 'fn') else update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=self.test_uri,
            range_address="A1:C2",
            data=data
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["data_shape"], (2, 3))

        print(f"   ✅ Records format: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_single_list(self, mock_google_sheets_service):
        """Test single list (treated as column)"""
        print("\n📊 Testing Single List")

        # Make the mock async
        mock_google_sheets_service.update_range = AsyncMock(return_value=True)

        data = [1, 2, 3, 4, 5]

        # Get the actual function (handle MCP wrapper)
        func = update_range.fn if hasattr(update_range, 'fn') else update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=self.test_uri,
            range_address="A1:A5",
            data=data
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["data_shape"], (5, 1))

        print(f"   ✅ Single list: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_2d_list(self, mock_google_sheets_service):
        """Test 2D list"""
        print("\n📊 Testing 2D List")

        # Make the mock async
        mock_google_sheets_service.update_range = AsyncMock(return_value=True)

        data = [
            ["Name", "Age", "City"],
            ["Alice", 25, "NYC"],
            ["Bob", 30, "LA"]
        ]

        # Get the actual function (handle MCP wrapper)
        func = update_range.fn if hasattr(update_range, 'fn') else update_range

        result = await func(
            ctx=self.mock_ctx,
            uri=self.test_uri,
            range_address="A1:C3",
            data=data
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["data_shape"], (3, 3))

        print(f"   ✅ 2D list: {result['message']}")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_pandas_dataframe(self, mock_google_sheets_service):
        """Test pandas DataFrame (if available)"""
        print("\n📊 Testing Pandas DataFrame")

        try:
            import pandas as pd

            # Make the mock async
            mock_google_sheets_service.update_range = AsyncMock(return_value=True)

            # Create a simple DataFrame
            df = pd.DataFrame({
                "Name": ["Alice", "Bob"],
                "Age": [25, 30],
                "City": ["NYC", "LA"]
            })

            # Get the actual function (handle MCP wrapper)
            func = update_range.fn if hasattr(update_range, 'fn') else update_range

            result = await func(
                ctx=self.mock_ctx,
                uri=self.test_uri,
                range_address="A1:C2",
                data=df
            )

            self.assertTrue(result["success"])
            self.assertEqual(result["data_shape"], (2, 3))

            print(f"   ✅ Pandas DataFrame: {result['message']}")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping DataFrame test")
            self.skipTest("pandas not available")

    @patch('datatable_tools.range_operations.range_operations.google_sheets_service')
    async def test_pandas_series(self, mock_google_sheets_service):
        """Test pandas Series (if available)"""
        print("\n📊 Testing Pandas Series")

        try:
            import pandas as pd

            # Make the mock async
            mock_google_sheets_service.update_range = AsyncMock(return_value=True)

            # Create a simple Series
            series = pd.Series([1, 2, 3, 4], name="Numbers")

            # Get the actual function (handle MCP wrapper)
            func = update_range.fn if hasattr(update_range, 'fn') else update_range

            result = await func(
                ctx=self.mock_ctx,
                uri=self.test_uri,
                range_address="A1:A4",
                data=series
            )

            self.assertTrue(result["success"])
            self.assertEqual(result["data_shape"], (4, 1))

            print(f"   ✅ Pandas Series: {result['message']}")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping Series test")
            self.skipTest("pandas not available")


async def run_async_tests():
    """Run async tests individually"""
    test_instance = TestUpdateRangeDataFrame()
    test_instance.setUp()

    print("🔄 Running async DataFrame-like data tests...")

    try:
        await test_instance.test_scalar_data()
        await test_instance.test_dict_column_data()
        await test_instance.test_records_format()
        await test_instance.test_single_list()
        await test_instance.test_2d_list()
        await test_instance.test_pandas_dataframe()
        await test_instance.test_pandas_series()
        print("✅ All async DataFrame tests completed successfully")
        return True
    except Exception as e:
        print(f"❌ Async test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import asyncio

    print("🚀 Starting Enhanced update_range DataFrame Tests")
    print("=" * 70)

    # Run async tests
    async_success = asyncio.run(run_async_tests())

    print("\n" + "=" * 70)
    print("🎯 Final Results:")
    print(f"   DataFrame Tests: {'✅ PASS' if async_success else '❌ FAIL'}")

    if async_success:
        print("\n🎉 All tests passed! update_range now supports DataFrame-like data formats.")
    else:
        print(f"\n❌ Some tests failed. Please check the issues above.")

    sys.exit(0 if async_success else 1)