#!/usr/bin/env python3
"""
Comprehensive test suite for the enhanced create_table function
Tests all supported data input formats similar to pd.DataFrame
"""

import asyncio
import sys
import logging
import unittest
from datetime import datetime
import json

# Add parent directory to path for imports
sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datatable_tools.lifecycle_tools import create_table, _process_data_input
from datatable_tools.table_manager import table_manager
from fastmcp import Context

class TestCreateTableDataFormats(unittest.TestCase):
    """Test create_table with various data input formats"""

    def setUp(self):
        """Clean up any existing tables before each test"""
        table_manager.cleanup_expired_tables(force=True)
        self.ctx = Context()

    async def async_create_table(self, data, headers=None, name="Test Table"):
        """Helper to call create_table async function"""
        return await create_table(self.ctx, data, headers, name)

    def test_traditional_2d_list(self):
        """Test traditional 2D list format"""
        print("\n🧪 Testing Traditional 2D List Format")

        data = [
            ["Alice", 25, "Engineer"],
            ["Bob", 30, "Manager"],
            ["Carol", 28, "Designer"]
        ]
        headers = ["Name", "Age", "Role"]

        result = asyncio.run(self.async_create_table(data, headers))

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 3])
        self.assertEqual(result["headers"], headers)
        print(f"   ✅ Created table: {result['table_id']} with shape {result['shape']}")

    def test_dictionary_column_format(self):
        """Test dictionary format (column-oriented)"""
        print("\n🧪 Testing Dictionary Column Format")

        data = {
            "Name": ["Alice", "Bob", "Carol"],
            "Age": [25, 30, 28],
            "Role": ["Engineer", "Manager", "Designer"]
        }

        result = asyncio.run(self.async_create_table(data))

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 3])
        self.assertEqual(set(result["headers"]), set(["Name", "Age", "Role"]))
        print(f"   ✅ Created table: {result['table_id']} with shape {result['shape']}")

    def test_records_format(self):
        """Test list of dictionaries (records format)"""
        print("\n🧪 Testing Records Format (List of Dictionaries)")

        data = [
            {"Name": "Alice", "Age": 25, "Role": "Engineer"},
            {"Name": "Bob", "Age": 30, "Role": "Manager"},
            {"Name": "Carol", "Age": 28, "Role": "Designer"}
        ]

        result = asyncio.run(self.async_create_table(data))

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 3])
        self.assertEqual(set(result["headers"]), set(["Age", "Name", "Role"]))  # Sorted order
        print(f"   ✅ Created table: {result['table_id']} with shape {result['shape']}")

    def test_single_column_list(self):
        """Test single column from list"""
        print("\n🧪 Testing Single Column List")

        data = [1, 2, 3, 4, 5]
        headers = ["Numbers"]

        result = asyncio.run(self.async_create_table(data, headers))

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [5, 1])
        self.assertEqual(result["headers"], headers)
        print(f"   ✅ Created table: {result['table_id']} with shape {result['shape']}")

    def test_single_row_dictionary(self):
        """Test single row as dictionary"""
        print("\n🧪 Testing Single Row Dictionary")

        data = {"Name": "Alice", "Age": 25, "Role": "Engineer"}

        result = asyncio.run(self.async_create_table(data))

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [1, 3])
        self.assertEqual(set(result["headers"]), set(["Age", "Name", "Role"]))
        print(f"   ✅ Created table: {result['table_id']} with shape {result['shape']}")

    def test_scalar_values(self):
        """Test scalar values"""
        print("\n🧪 Testing Scalar Values")

        test_cases = [
            (42, "integer"),
            (3.14, "float"),
            ("Hello World", "string"),
            (True, "boolean"),
            (None, "None")
        ]

        for value, desc in test_cases:
            with self.subTest(value=value, desc=desc):
                result = asyncio.run(self.async_create_table(value, name=f"Scalar {desc}"))

                self.assertTrue(result["success"])
                self.assertEqual(result["shape"], [1, 1])
                self.assertEqual(result["headers"], ["Value"])
                print(f"   ✅ Created {desc} table: {result['table_id']} with value {value}")

    def test_empty_data(self):
        """Test empty data structures"""
        print("\n🧪 Testing Empty Data Structures")

        test_cases = [
            ([], "empty list"),
            ({}, "empty dict"),
        ]

        for data, desc in test_cases:
            with self.subTest(data=data, desc=desc):
                result = asyncio.run(self.async_create_table(data, name=f"Empty {desc}"))

                self.assertTrue(result["success"])
                self.assertEqual(result["shape"], [0, 0])
                print(f"   ✅ Created {desc} table: {result['table_id']} with shape {result['shape']}")

    def test_mixed_data_types(self):
        """Test mixed data types in columns"""
        print("\n🧪 Testing Mixed Data Types")

        data = {
            "ID": [1, 2, 3],
            "Name": ["Alice", "Bob", "Carol"],
            "Score": [95.5, 87.2, 92.0],
            "Active": [True, False, True],
            "Notes": ["Good", None, "Excellent"]
        }

        result = asyncio.run(self.async_create_table(data))

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 5])
        print(f"   ✅ Created mixed types table: {result['table_id']} with shape {result['shape']}")

    def test_pandas_dataframe(self):
        """Test pandas DataFrame input"""
        print("\n🧪 Testing Pandas DataFrame Input")

        try:
            import pandas as pd

            df = pd.DataFrame({
                "A": [1, 2, 3],
                "B": ["x", "y", "z"],
                "C": [1.1, 2.2, 3.3]
            })

            result = asyncio.run(self.async_create_table(df))

            self.assertTrue(result["success"])
            self.assertEqual(result["shape"], [3, 3])
            self.assertEqual(result["headers"], ["A", "B", "C"])
            print(f"   ✅ Created DataFrame table: {result['table_id']} with shape {result['shape']}")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping DataFrame test")

    def test_pandas_series(self):
        """Test pandas Series input"""
        print("\n🧪 Testing Pandas Series Input")

        try:
            import pandas as pd

            series = pd.Series([10, 20, 30, 40], name="Values")

            result = asyncio.run(self.async_create_table(series))

            self.assertTrue(result["success"])
            self.assertEqual(result["shape"], [4, 1])
            self.assertEqual(result["headers"], ["Values"])
            print(f"   ✅ Created Series table: {result['table_id']} with shape {result['shape']}")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping Series test")

    def test_numpy_arrays(self):
        """Test numpy array input"""
        print("\n🧪 Testing NumPy Array Input")

        try:
            import numpy as np

            # Test 1D array
            arr_1d = np.array([1, 2, 3, 4, 5])
            result = asyncio.run(self.async_create_table(arr_1d, name="NumPy 1D"))

            self.assertTrue(result["success"])
            self.assertEqual(result["shape"], [5, 1])
            print(f"   ✅ Created 1D array table: {result['table_id']} with shape {result['shape']}")

            # Test 2D array
            arr_2d = np.array([[1, 2, 3], [4, 5, 6]])
            result = asyncio.run(self.async_create_table(arr_2d, name="NumPy 2D"))

            self.assertTrue(result["success"])
            self.assertEqual(result["shape"], [2, 3])
            print(f"   ✅ Created 2D array table: {result['table_id']} with shape {result['shape']}")

        except ImportError:
            print("   ⚠️  NumPy not available, skipping array tests")

    def test_custom_headers_override(self):
        """Test custom headers override automatic detection"""
        print("\n🧪 Testing Custom Headers Override")

        data = {"A": [1, 2], "B": [3, 4]}
        custom_headers = ["X", "Y"]

        result = asyncio.run(self.async_create_table(data, custom_headers))

        self.assertTrue(result["success"])
        self.assertEqual(result["headers"], custom_headers)
        print(f"   ✅ Custom headers applied: {result['headers']}")

    def test_error_handling(self):
        """Test error handling for invalid data"""
        print("\n🧪 Testing Error Handling")

        # Test mismatched column lengths
        data = {"A": [1, 2, 3], "B": [4, 5]}  # Different lengths

        result = asyncio.run(self.async_create_table(data))

        self.assertFalse(result["success"])
        self.assertIn("same length", result["error"])
        print(f"   ✅ Correctly caught error: {result['error']}")

    def test_large_data_performance(self):
        """Test performance with larger datasets"""
        print("\n🧪 Testing Large Data Performance")

        # Create larger dataset
        import time
        size = 1000

        data = {
            "ID": list(range(size)),
            "Value": [f"Item_{i}" for i in range(size)],
            "Score": [i * 0.1 for i in range(size)]
        }

        start_time = time.time()
        result = asyncio.run(self.async_create_table(data, name="Large Dataset"))
        end_time = time.time()

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [size, 3])

        duration = end_time - start_time
        print(f"   ✅ Created {size}-row table in {duration:.3f}s: {result['table_id']}")

class TestProcessDataInput(unittest.TestCase):
    """Test the _process_data_input helper function directly"""

    def test_process_data_input_direct(self):
        """Test _process_data_input function directly"""
        print("\n🧪 Testing _process_data_input Function Directly")

        test_cases = [
            # (input_data, expected_rows, expected_cols, description)
            ([1, 2, 3], 3, 1, "simple list"),
            ([[1, 2], [3, 4]], 2, 2, "2D list"),
            ({"A": [1, 2], "B": [3, 4]}, 2, 2, "dict columns"),
            ([{"A": 1, "B": 2}, {"A": 3, "B": 4}], 2, 2, "records"),
            (42, 1, 1, "scalar"),
        ]

        for data, exp_rows, exp_cols, desc in test_cases:
            with self.subTest(data=data, desc=desc):
                processed_data, processed_headers = _process_data_input(data)

                self.assertEqual(len(processed_data), exp_rows, f"Wrong row count for {desc}")
                if processed_data:
                    self.assertEqual(len(processed_data[0]), exp_cols, f"Wrong col count for {desc}")
                self.assertEqual(len(processed_headers), exp_cols, f"Wrong header count for {desc}")

                print(f"   ✅ {desc}: {exp_rows}x{exp_cols} -> {processed_headers}")


def run_comprehensive_tests():
    """Run all create_table tests"""
    print("🚀 Starting Comprehensive create_table Tests")
    print("=" * 70)

    # Create test suite
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTest(unittest.makeSuite(TestCreateTableDataFormats))
    suite.addTest(unittest.makeSuite(TestProcessDataInput))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 70)
    print("📊 Test Results Summary:")
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
    if success:
        print("\n🎉 All tests passed! create_table function is working correctly.")
    else:
        print(f"\n❌ Some tests failed. Please check the issues above.")

    return success


if __name__ == "__main__":
    # Run comprehensive tests
    success = run_comprehensive_tests()
    sys.exit(0 if success else 1)