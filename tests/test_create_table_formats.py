#!/usr/bin/env python3
"""
Comprehensive test suite for the enhanced create_table function
Tests all supported data input formats similar to pd.DataFrame
"""

import sys
import logging
import unittest
import time
from datetime import datetime
import json

# Add parent directory to path for imports
sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datatable_tools.lifecycle_tools import _process_data_input
from datatable_tools.table_manager import table_manager

class TestCreateTableDataFormats(unittest.TestCase):
    """Test create_table with various data input formats"""

    def setUp(self):
        """Clean up any existing tables before each test"""
        table_manager.cleanup_expired_tables(force=True)

    def create_table_helper(self, data, headers=None, name="Test Table"):
        """Helper to create table using table manager directly"""
        try:
            # Process data using the same logic as the MCP tool
            processed_data, processed_headers = _process_data_input(data, headers)

            # Create table using table manager
            table_id = table_manager.create_table(
                data=processed_data,
                headers=processed_headers,
                name=name,
                source_info={"type": "test_creation"}
            )

            table = table_manager.get_table(table_id)
            if not table:
                raise Exception("Failed to create table")

            return {
                "success": True,
                "table_id": table_id,
                "name": table.metadata.name,
                "shape": table.shape,
                "headers": table.headers,
                "message": f"Created table '{name}' with {table.shape[0]} rows and {table.shape[1]} columns"
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to create table"
            }

    def test_traditional_2d_list(self):
        """Test traditional 2D list format"""
        print("\n🧪 Testing Traditional 2D List Format")

        data = [
            ["Alice", 25, "Engineer"],
            ["Bob", 30, "Manager"],
            ["Carol", 28, "Designer"]
        ]
        headers = ["Name", "Age", "Role"]

        result = self.create_table_helper(data, headers)

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

        result = self.create_table_helper(data)

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

        result = self.create_table_helper(data)

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 3])
        self.assertEqual(set(result["headers"]), set(["Age", "Name", "Role"]))  # Sorted order
        print(f"   ✅ Created table: {result['table_id']} with shape {result['shape']}")

    def test_single_column_list(self):
        """Test single column from list"""
        print("\n🧪 Testing Single Column List")

        data = [1, 2, 3, 4, 5]
        headers = ["Numbers"]

        result = self.create_table_helper(data, headers)

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [5, 1])
        self.assertEqual(result["headers"], headers)
        print(f"   ✅ Created table: {result['table_id']} with shape {result['shape']}")

    def test_single_row_dictionary(self):
        """Test single row as dictionary"""
        print("\n🧪 Testing Single Row Dictionary")

        data = {"Name": "Alice", "Age": 25, "Role": "Engineer"}

        result = self.create_table_helper(data)

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
                result = self.create_table_helper(value, name=f"Scalar {desc}")

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
                result = self.create_table_helper(data, name=f"Empty {desc}")

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

        result = self.create_table_helper(data)

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

            result = self.create_table_helper(df)

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

            result = self.create_table_helper(series)

            self.assertTrue(result["success"])
            self.assertEqual(result["shape"], [4, 1])
            self.assertEqual(result["headers"], ["Values"])
            print(f"   ✅ Created Series table: {result['table_id']} with shape {result['shape']}")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping Series test")

    def test_comprehensive_data_types(self):
        """Test comprehensive mixed data types with dates and complex structures"""
        print("\n🧪 Testing Comprehensive Data Types")

        data = {
            "ID": [1, 2, 3],
            "Name": ["Alice", "Bob", "Carol"],
            "Score": [95.5, 87.2, 92.0],
            "Active": [True, False, True],
            "Notes": ["Excellent", None, "Good"],
            "Date": ["2024-01-01", "2024-01-02", "2024-01-03"]
        }

        result = self.create_table_helper(data, name="Comprehensive Types")

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 6])
        expected_headers = {"Active", "Date", "ID", "Name", "Notes", "Score"}
        self.assertEqual(set(result["headers"]), expected_headers)
        print(f"   ✅ Created comprehensive table: {result['table_id']} with shape {result['shape']}")

    def test_products_data_realistic(self):
        """Test realistic product data scenario"""
        print("\n🧪 Testing Realistic Product Data")

        data = {
            "Product": ["Laptop", "Mouse", "Keyboard"],
            "Price": [999.99, 29.99, 79.99],
            "Stock": [25, 150, 75],
            "Category": ["Electronics", "Electronics", "Electronics"]
        }

        result = self.create_table_helper(data, name="Product Catalog")

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 4])
        expected_headers = {"Category", "Price", "Product", "Stock"}
        self.assertEqual(set(result["headers"]), expected_headers)
        print(f"   ✅ Created product table: {result['table_id']} with shape {result['shape']}")

    def test_phones_records_format(self):
        """Test phone products in records format"""
        print("\n🧪 Testing Phone Products Records")

        data = [
            {"Name": "iPhone 14", "Price": 999, "Brand": "Apple"},
            {"Name": "Galaxy S23", "Price": 899, "Brand": "Samsung"},
            {"Name": "Pixel 7", "Price": 699, "Brand": "Google"}
        ]

        result = self.create_table_helper(data, name="Phone Catalog")

        self.assertTrue(result["success"])
        self.assertEqual(result["shape"], [3, 3])
        expected_headers = {"Brand", "Name", "Price"}
        self.assertEqual(set(result["headers"]), expected_headers)
        print(f"   ✅ Created phone table: {result['table_id']} with shape {result['shape']}")

    def test_country_data_comprehensive(self):
        """Test comprehensive country data similar to pandas demo"""
        print("\n🧪 Testing Country Data")

        try:
            import pandas as pd

            df = pd.DataFrame({
                "Country": ["USA", "UK", "Canada", "Australia"],
                "Population": [331_000_000, 67_000_000, 38_000_000, 25_000_000],
                "GDP": [21.43, 2.83, 1.74, 1.55],
                "Currency": ["USD", "GBP", "CAD", "AUD"]
            })

            result = self.create_table_helper(df, name="Country Stats")

            self.assertTrue(result["success"])
            self.assertEqual(result["shape"], [4, 4])
            self.assertEqual(result["headers"], ["Country", "Population", "GDP", "Currency"])
            print(f"   ✅ Created country table: {result['table_id']} with shape {result['shape']}")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping country data test")

    def test_numpy_arrays(self):
        """Test numpy array input"""
        print("\n🧪 Testing NumPy Array Input")

        try:
            import numpy as np

            # Test 1D array
            arr_1d = np.array([1, 2, 3, 4, 5])
            result = self.create_table_helper(arr_1d, name="NumPy 1D")

            self.assertTrue(result["success"])
            self.assertEqual(result["shape"], [5, 1])
            print(f"   ✅ Created 1D array table: {result['table_id']} with shape {result['shape']}")

            # Test 2D array
            arr_2d = np.array([[1, 2, 3], [4, 5, 6]])
            result = self.create_table_helper(arr_2d, name="NumPy 2D")

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

        result = self.create_table_helper(data, custom_headers)

        self.assertTrue(result["success"])
        self.assertEqual(result["headers"], custom_headers)
        print(f"   ✅ Custom headers applied: {result['headers']}")

    def test_error_handling(self):
        """Test error handling for invalid data"""
        print("\n🧪 Testing Error Handling")

        # Test mismatched column lengths
        data = {"A": [1, 2, 3], "B": [4, 5]}  # Different lengths

        result = self.create_table_helper(data)

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
        result = self.create_table_helper(data, name="Large Dataset")
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

    # Add test cases (modern approach)
    loader = unittest.TestLoader()
    suite.addTests(loader.loadTestsFromTestCase(TestCreateTableDataFormats))
    suite.addTests(loader.loadTestsFromTestCase(TestProcessDataInput))

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