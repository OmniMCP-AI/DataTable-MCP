#!/usr/bin/env python3
"""
Comprehensive test suite for the simplified export_table function
Tests all supported export URI formats and verifies file creation
"""

import os
import sys
import json
import tempfile
import unittest
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datatable_tools.export_tools import _export_file, _export_data_file, _export_data_google_sheets
from datatable_tools.utils import detect_export_type, parse_export_uri
from datatable_tools.table_manager import table_manager
from datatable_tools.lifecycle_tools import _process_data_input

class TestExportTableFormats(unittest.TestCase):
    """Test export_table with various URI formats"""

    def setUp(self):
        """Set up test data and clean environment"""
        table_manager.cleanup_expired_tables(force=True)

        # Create a test table with sample data
        test_data = [
            ["Alice", 25, "Engineer", 75000],
            ["Bob", 30, "Manager", 85000],
            ["Carol", 28, "Designer", 65000],
            ["David", 35, "Director", 95000]
        ]
        test_headers = ["Name", "Age", "Role", "Salary"]

        self.table_id = table_manager.create_table(
            data=test_data,
            headers=test_headers,
            name="Test Export Data",
            source_info={"type": "test_export"}
        )

        self.table = table_manager.get_table(self.table_id)
        self.assertIsNotNone(self.table)

        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        print(f"   📁 Using temp directory: {self.temp_dir}")

    def tearDown(self):
        """Clean up test files"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    async def export_helper(self, uri, encoding=None, delimiter=None):
        """Helper to export table using the simplified logic"""
        try:
            from datatable_tools.utils import parse_export_uri

            # Parse the URI to determine export type
            export_info = parse_export_uri(uri)
            export_type = export_info["export_type"]

            if export_type == "google_sheets":
                return {
                    "success": False,
                    "error": "Google Sheets export not yet implemented in tests",
                    "message": "Google Sheets testing requires authentication setup"
                }

            # Use the file export logic directly
            return await _export_file(self.table, export_info, encoding, delimiter)

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to export table to {uri}"
            }

    def test_uri_detection(self):
        """Test URI format detection"""
        print("\n🔍 Testing URI Format Detection")

        test_cases = [
            ("/path/to/data.csv", "csv"),
            ("/path/to/workbook.xlsx", "excel"),
            ("/path/to/data.json", "json"),
            ("/path/to/data.parquet", "parquet"),
            ("https://docs.google.com/spreadsheets/d/abc123/edit", "google_sheets"),
            ("1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms", "google_sheets"),
            ("https://example.com/data.csv", "csv"),
            ("/some/file.txt", "file")
        ]

        for uri, expected_type in test_cases:
            with self.subTest(uri=uri):
                detected_type = detect_export_type(uri)
                self.assertEqual(detected_type, expected_type)
                print(f"   ✅ {uri} → {detected_type}")

    def test_csv_export(self):
        """Test CSV export functionality"""
        print("\n📄 Testing CSV Export")

        import asyncio

        # Test basic CSV export
        csv_path = os.path.join(self.temp_dir, "test_export.csv")
        result = asyncio.run(self.export_helper(csv_path))

        self.assertTrue(result["success"])
        self.assertEqual(result["export_type"], "csv")
        self.assertTrue(os.path.exists(csv_path))
        self.assertEqual(result["rows_exported"], 4)
        self.assertEqual(result["columns_exported"], 4)

        # Verify CSV content
        with open(csv_path, 'r') as f:
            content = f.read()
            self.assertIn("Name,Age,Role,Salary", content)
            self.assertIn("Alice,25,Engineer,75000", content)

        print(f"   ✅ CSV exported: {result['file_size']} bytes")

        # Test CSV with custom delimiter
        csv_semicolon_path = os.path.join(self.temp_dir, "test_semicolon.csv")
        result = asyncio.run(self.export_helper(csv_semicolon_path, delimiter=";"))

        self.assertTrue(result["success"])

        # Verify semicolon delimiter
        with open(csv_semicolon_path, 'r') as f:
            content = f.read()
            self.assertIn("Name;Age;Role;Salary", content)

        print(f"   ✅ CSV with semicolon delimiter exported")

    def test_excel_export(self):
        """Test Excel export functionality"""
        print("\n📊 Testing Excel Export")

        import asyncio

        # Test .xlsx export
        xlsx_path = os.path.join(self.temp_dir, "test_export.xlsx")
        result = asyncio.run(self.export_helper(xlsx_path))

        self.assertTrue(result["success"])
        self.assertEqual(result["export_type"], "excel")
        self.assertTrue(os.path.exists(xlsx_path))

        # Verify Excel file can be read back
        try:
            import pandas as pd
            df_read = pd.read_excel(xlsx_path)
            self.assertEqual(len(df_read), 4)
            self.assertEqual(list(df_read.columns), ["Name", "Age", "Role", "Salary"])
            print(f"   ✅ Excel exported and verified: {result['file_size']} bytes")
        except ImportError:
            print(f"   ✅ Excel exported: {result['file_size']} bytes (pandas not available for verification)")

    def test_json_export(self):
        """Test JSON export functionality"""
        print("\n📋 Testing JSON Export")

        import asyncio

        json_path = os.path.join(self.temp_dir, "test_export.json")
        result = asyncio.run(self.export_helper(json_path))

        self.assertTrue(result["success"])
        self.assertEqual(result["export_type"], "json")
        self.assertTrue(os.path.exists(json_path))

        # Verify JSON content
        with open(json_path, 'r') as f:
            data = json.load(f)
            self.assertEqual(len(data), 4)
            self.assertEqual(data[0]["Name"], "Alice")
            self.assertEqual(data[0]["Age"], 25)

        print(f"   ✅ JSON exported and verified: {result['file_size']} bytes")

    def test_parquet_export(self):
        """Test Parquet export functionality"""
        print("\n🗃️ Testing Parquet Export")

        import asyncio

        parquet_path = os.path.join(self.temp_dir, "test_export.parquet")
        result = asyncio.run(self.export_helper(parquet_path))

        self.assertTrue(result["success"])
        self.assertEqual(result["export_type"], "parquet")
        self.assertTrue(os.path.exists(parquet_path))

        # Verify Parquet file can be read back
        try:
            import pandas as pd
            df_read = pd.read_parquet(parquet_path)
            self.assertEqual(len(df_read), 4)
            self.assertEqual(list(df_read.columns), ["Name", "Age", "Role", "Salary"])
            print(f"   ✅ Parquet exported and verified: {result['file_size']} bytes")
        except ImportError:
            print(f"   ✅ Parquet exported: {result['file_size']} bytes (pandas not available for verification)")

    def test_google_sheets_detection(self):
        """Test Google Sheets URI detection"""
        print("\n📗 Testing Google Sheets Detection")

        import asyncio

        test_urls = [
            "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
            "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        ]

        for url in test_urls:
            with self.subTest(url=url):
                result = asyncio.run(self.export_helper(url))
                # Should detect as Google Sheets but not implement export yet
                self.assertFalse(result["success"])
                self.assertIn("Google Sheets", result["error"])
                print(f"   ✅ Google Sheets detected: {url}")

    def test_directory_creation(self):
        """Test automatic directory creation"""
        print("\n📁 Testing Directory Creation")

        import asyncio

        # Create nested directory path
        nested_path = os.path.join(self.temp_dir, "nested", "deep", "path", "test.csv")

        result = asyncio.run(self.export_helper(nested_path))

        self.assertTrue(result["success"])
        self.assertTrue(os.path.exists(nested_path))
        self.assertTrue(os.path.isdir(os.path.dirname(nested_path)))

        print(f"   ✅ Nested directories created automatically")

    def test_error_handling(self):
        """Test error handling for invalid scenarios"""
        print("\n❌ Testing Error Handling")

        import asyncio

        # Test invalid URI
        with self.assertRaises(ValueError):
            detect_export_type("")

        # Test invalid file path (permission denied simulation)
        if os.name != 'nt':  # Skip on Windows due to different permission model
            try:
                invalid_path = "/root/forbidden/test.csv"  # Likely no permission
                result = asyncio.run(self.export_helper(invalid_path))
                # Should handle the error gracefully
                if not result["success"]:
                    print(f"   ✅ Permission error handled gracefully")
                else:
                    print(f"   ⚠️  Unexpected success (may have permissions)")
            except:
                print(f"   ✅ Permission error handled gracefully")

    def test_encoding_support(self):
        """Test different encoding support for CSV"""
        print("\n🔤 Testing Encoding Support")

        import asyncio

        # Test UTF-8 encoding
        csv_utf8_path = os.path.join(self.temp_dir, "test_utf8.csv")
        result = asyncio.run(self.export_helper(csv_utf8_path, encoding="utf-8"))

        self.assertTrue(result["success"])

        # Test different encoding
        csv_latin1_path = os.path.join(self.temp_dir, "test_latin1.csv")
        result = asyncio.run(self.export_helper(csv_latin1_path, encoding="latin1"))

        self.assertTrue(result["success"])

        print(f"   ✅ Multiple encodings supported")

    def test_file_preview_content(self):
        """Test file content preview for text formats"""
        print("\n👁️ Testing File Content Preview")

        import asyncio

        # Test CSV preview
        csv_path = os.path.join(self.temp_dir, "preview_test.csv")
        result = asyncio.run(self.export_helper(csv_path))

        self.assertTrue(result["success"])

        # Show file content preview like in demo
        with open(csv_path, 'r') as f:
            content = f.read()
            preview = content[:100] + "..." if len(content) > 100 else content
            print(f"   📄 CSV Preview: {preview}")

        # Test JSON preview
        json_path = os.path.join(self.temp_dir, "preview_test.json")
        result = asyncio.run(self.export_helper(json_path))

        self.assertTrue(result["success"])

        with open(json_path, 'r') as f:
            content = f.read()
            preview = content[:100] + "..." if len(content) > 100 else content
            print(f"   📋 JSON Preview: {preview}")

        print(f"   ✅ File content previews generated successfully")

    def test_comprehensive_export_demo(self):
        """Test comprehensive export demo scenarios like the original demo script"""
        print("\n🎭 Testing Comprehensive Export Demo Scenarios")

        import asyncio

        # Create demo table with more realistic employee data
        demo_data = [
            ["Alice Johnson", 25, "Software Engineer", 75000, "Engineering"],
            ["Bob Smith", 30, "Product Manager", 85000, "Product"],
            ["Carol Davis", 28, "UX Designer", 65000, "Design"],
            ["David Wilson", 35, "Engineering Director", 120000, "Engineering"],
            ["Eva Brown", 32, "Data Scientist", 95000, "Data"]
        ]
        demo_headers = ["Name", "Age", "Role", "Salary", "Department"]

        demo_table_id = table_manager.create_table(
            data=demo_data,
            headers=demo_headers,
            name="Employee Data Demo",
            source_info={"type": "demo_export"}
        )

        demo_table = table_manager.get_table(demo_table_id)
        self.assertIsNotNone(demo_table)

        async def export_demo_helper(uri, encoding=None, delimiter=None):
            """Helper for demo export testing"""
            export_info = parse_export_uri(uri)
            return await _export_file(demo_table, export_info, encoding, delimiter)

        # Test all formats with demo data
        test_exports = [
            ("employees.csv", "📄 CSV Export"),
            ("employees_semicolon.csv", "📄 CSV with Semicolon", None, ";"),
            ("employees.xlsx", "📊 Excel Export"),
            ("employees.json", "📋 JSON Export"),
            ("employees.parquet", "🗃️ Parquet Export")
        ]

        for export_test in test_exports:
            filename = export_test[0]
            description = export_test[1]
            encoding = export_test[2] if len(export_test) > 2 else None
            delimiter = export_test[3] if len(export_test) > 3 else None

            file_path = os.path.join(self.temp_dir, filename)
            result = asyncio.run(export_demo_helper(file_path, encoding, delimiter))

            self.assertTrue(result["success"], f"Failed to export {filename}")
            self.assertEqual(result["rows_exported"], 5)
            self.assertEqual(result["columns_exported"], 5)

            file_size_kb = result["file_size"] / 1024
            print(f"   ✅ {description}: {file_size_kb:.1f} KB ({result['rows_exported']} rows)")

        print(f"   🎉 All demo export scenarios completed successfully")

    def test_large_data_export(self):
        """Test export performance with larger dataset"""
        print("\n⚡ Testing Large Data Export Performance")

        import asyncio
        import time

        # Create larger test table
        large_data = []
        for i in range(1000):
            large_data.append([f"User_{i}", 20 + (i % 50), f"Role_{i % 10}", 50000 + (i * 100)])

        large_table_id = table_manager.create_table(
            data=large_data,
            headers=["Name", "Age", "Role", "Salary"],
            name="Large Test Data",
            source_info={"type": "performance_test"}
        )

        large_table = table_manager.get_table(large_table_id)

        async def export_large_data():
            # Test CSV export performance
            csv_path = os.path.join(self.temp_dir, "large_export.csv")

            start_time = time.time()
            export_info = parse_export_uri(csv_path)
            result = await _export_file(large_table, export_info, None, None)
            end_time = time.time()

            return result, end_time - start_time

        result, duration = asyncio.run(export_large_data())

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 1000)

        file_size_mb = result["file_size"] / (1024 * 1024)

        print(f"   ✅ Exported 1000 rows in {duration:.3f}s ({file_size_mb:.2f} MB)")


class TestExportDataFormats(unittest.TestCase):
    """Test export_data with various data input formats like create_table"""

    def setUp(self):
        """Set up test environment"""
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        print(f"\n   📁 Using temp directory: {self.temp_dir}")

    def tearDown(self):
        """Clean up test files"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    async def export_data_helper(self, data, uri, headers=None, encoding=None, delimiter=None):
        """Helper to export data using the new export_data logic"""
        try:
            # Process data into standardized format
            processed_data, processed_headers = _process_data_input(data, headers)

            # Parse the URI to determine export type
            export_info = parse_export_uri(uri)
            export_type = export_info["export_type"]

            if export_type == "google_sheets":
                return {
                    "success": False,
                    "error": "Google Sheets export not yet implemented in tests",
                    "message": "Google Sheets testing requires authentication setup"
                }

            # Use the data export logic directly
            return await _export_data_file(processed_data, processed_headers, export_info, encoding, delimiter)

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to export data to {uri}"
            }

    def test_dataframe_export(self):
        """Test exporting pandas DataFrame"""
        print("\n🐼 Testing DataFrame Export")

        try:
            import pandas as pd
            import asyncio

            # Create test DataFrame
            df = pd.DataFrame({
                "Name": ["Alice", "Bob", "Carol"],
                "Age": [25, 30, 28],
                "City": ["NYC", "LA", "Chicago"]
            })

            csv_path = os.path.join(self.temp_dir, "dataframe_export.csv")
            result = asyncio.run(self.export_data_helper(df, csv_path))

            self.assertTrue(result["success"])
            self.assertEqual(result["rows_exported"], 3)
            self.assertEqual(result["columns_exported"], 3)

            # Verify content
            with open(csv_path, 'r') as f:
                content = f.read()
                self.assertIn("Name,Age,City", content)
                self.assertIn("Alice,25,NYC", content)

            print(f"   ✅ DataFrame exported successfully: {result['file_size']} bytes")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping DataFrame test")

    def test_dictionary_export(self):
        """Test exporting dictionary data (column-oriented)"""
        print("\n📚 Testing Dictionary Export")

        import asyncio

        # Test column-oriented dictionary
        data = {
            "Product": ["Apple", "Banana", "Orange"],
            "Price": [1.20, 0.80, 1.50],
            "Stock": [100, 80, 60]
        }

        json_path = os.path.join(self.temp_dir, "dict_export.json")
        result = asyncio.run(self.export_data_helper(data, json_path))

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 3)
        self.assertEqual(result["columns_exported"], 3)

        # Verify JSON content
        with open(json_path, 'r') as f:
            import json
            json_data = json.load(f)
            self.assertEqual(len(json_data), 3)
            self.assertEqual(json_data[0]["Product"], "Apple")
            self.assertEqual(json_data[0]["Price"], 1.2)

        print(f"   ✅ Dictionary exported successfully: {result['file_size']} bytes")

    def test_records_format_export(self):
        """Test exporting list of dictionaries (records format)"""
        print("\n📋 Testing Records Format Export")

        import asyncio

        # Test records format
        data = [
            {"Name": "Alice", "Score": 95, "Grade": "A"},
            {"Name": "Bob", "Score": 87, "Grade": "B"},
            {"Name": "Carol", "Score": 92, "Grade": "A"}
        ]

        excel_path = os.path.join(self.temp_dir, "records_export.xlsx")
        result = asyncio.run(self.export_data_helper(data, excel_path))

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 3)
        self.assertEqual(result["columns_exported"], 3)

        print(f"   ✅ Records format exported successfully: {result['file_size']} bytes")

    def test_single_column_export(self):
        """Test exporting single column data (list)"""
        print("\n📊 Testing Single Column Export")

        import asyncio

        # Test 1D list
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        headers = ["Numbers"]

        csv_path = os.path.join(self.temp_dir, "single_column.csv")
        result = asyncio.run(self.export_data_helper(data, csv_path, headers))

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 10)
        self.assertEqual(result["columns_exported"], 1)

        # Verify content
        with open(csv_path, 'r') as f:
            content = f.read()
            self.assertIn("Numbers", content)
            self.assertIn("1\n2\n3", content)

        print(f"   ✅ Single column exported successfully: {result['file_size']} bytes")

    def test_scalar_value_export(self):
        """Test exporting scalar values"""
        print("\n🔢 Testing Scalar Value Export")

        import asyncio

        # Test scalar value
        data = 42
        headers = ["Answer"]

        json_path = os.path.join(self.temp_dir, "scalar_export.json")
        result = asyncio.run(self.export_data_helper(data, json_path, headers))

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 1)
        self.assertEqual(result["columns_exported"], 1)

        # Verify JSON content
        with open(json_path, 'r') as f:
            import json
            json_data = json.load(f)
            self.assertEqual(len(json_data), 1)
            self.assertEqual(json_data[0]["Answer"], 42)

        print(f"   ✅ Scalar value exported successfully: {result['file_size']} bytes")

    def test_2d_list_export(self):
        """Test exporting 2D list (traditional format)"""
        print("\n📋 Testing 2D List Export")

        import asyncio

        # Test 2D list
        data = [
            ["Alice", 25, "Engineer"],
            ["Bob", 30, "Manager"],
            ["Carol", 28, "Designer"]
        ]
        headers = ["Name", "Age", "Role"]

        parquet_path = os.path.join(self.temp_dir, "2d_list_export.parquet")
        result = asyncio.run(self.export_data_helper(data, parquet_path, headers))

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 3)
        self.assertEqual(result["columns_exported"], 3)

        print(f"   ✅ 2D list exported successfully: {result['file_size']} bytes")

    def test_series_export(self):
        """Test exporting pandas Series"""
        print("\n📈 Testing Series Export")

        try:
            import pandas as pd
            import asyncio

            # Create test Series
            series = pd.Series([10, 20, 30, 40, 50], name="Values")

            csv_path = os.path.join(self.temp_dir, "series_export.csv")
            result = asyncio.run(self.export_data_helper(series, csv_path))

            self.assertTrue(result["success"])
            self.assertEqual(result["rows_exported"], 5)
            self.assertEqual(result["columns_exported"], 1)

            # Verify content
            with open(csv_path, 'r') as f:
                content = f.read()
                self.assertIn("Values", content)

            print(f"   ✅ Series exported successfully: {result['file_size']} bytes")

        except ImportError:
            print("   ⚠️  Pandas not available, skipping Series test")

    def test_mixed_data_types_export(self):
        """Test exporting data with mixed types"""
        print("\n🔀 Testing Mixed Data Types Export")

        import asyncio

        # Test mixed data types
        data = {
            "String": ["Hello", "World", "Test"],
            "Integer": [1, 2, 3],
            "Float": [1.1, 2.2, 3.3],
            "Boolean": [True, False, True]
        }

        csv_path = os.path.join(self.temp_dir, "mixed_types.csv")
        result = asyncio.run(self.export_data_helper(data, csv_path))

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 3)
        self.assertEqual(result["columns_exported"], 4)

        # Verify content handles mixed types
        with open(csv_path, 'r') as f:
            content = f.read()
            self.assertIn("String,Integer,Float,Boolean", content)
            self.assertIn("Hello,1,1.1,True", content)

        print(f"   ✅ Mixed data types exported successfully: {result['file_size']} bytes")

    def test_empty_data_export(self):
        """Test exporting empty data"""
        print("\n🕳️ Testing Empty Data Export")

        import asyncio

        # Test empty list
        data = []
        headers = ["Empty"]

        csv_path = os.path.join(self.temp_dir, "empty_export.csv")
        result = asyncio.run(self.export_data_helper(data, csv_path, headers))

        self.assertTrue(result["success"])
        self.assertEqual(result["rows_exported"], 0)

        print(f"   ✅ Empty data exported successfully: {result['file_size']} bytes")

    def test_comprehensive_data_formats(self):
        """Test comprehensive data format scenarios"""
        print("\n🎭 Testing Comprehensive Data Format Scenarios")

        import asyncio

        test_scenarios = [
            # (data, description, expected_rows, expected_cols)
            ({"A": [1, 2], "B": [3, 4]}, "Column dict", 2, 2),
            ([{"x": 1, "y": 2}, {"x": 3, "y": 4}], "Records list", 2, 2),
            ([[1, 2], [3, 4]], "2D list", 2, 2),
            ([1, 2, 3], "1D list", 3, 1),
            ("single_value", "String scalar", 1, 1),
            (123, "Numeric scalar", 1, 1),
        ]

        for i, (data, description, expected_rows, expected_cols) in enumerate(test_scenarios):
            with self.subTest(scenario=description):
                file_path = os.path.join(self.temp_dir, f"scenario_{i}.csv")
                result = asyncio.run(self.export_data_helper(data, file_path))

                self.assertTrue(result["success"], f"Failed for {description}")
                self.assertEqual(result["rows_exported"], expected_rows)
                self.assertEqual(result["columns_exported"], expected_cols)

                print(f"   ✅ {description}: {result['rows_exported']}×{result['columns_exported']} → {result['file_size']} bytes")

        print(f"   🎉 All data format scenarios completed successfully")


class TestExportUtilities(unittest.TestCase):
    """Test the export utility functions directly"""

    def test_parse_export_uri(self):
        """Test parse_export_uri function"""
        print("\n🔧 Testing Export URI Parsing")

        test_cases = [
            ("/path/to/data.csv", {"export_type": "csv", "file_path": "/path/to/data.csv"}),
            ("/path/to/data.xlsx", {"export_type": "excel", "file_path": "/path/to/data.xlsx"}),
            ("/path/to/data.json", {"export_type": "json", "file_path": "/path/to/data.json"}),
            ("https://docs.google.com/spreadsheets/d/abc123/edit", {"export_type": "google_sheets", "spreadsheet_id": "abc123"}),
        ]

        for uri, expected_partial in test_cases:
            with self.subTest(uri=uri):
                result = parse_export_uri(uri)

                for key, value in expected_partial.items():
                    self.assertEqual(result[key], value)

                self.assertEqual(result["original_uri"], uri)
                print(f"   ✅ {uri} → {result['export_type']}")


def run_export_tests():
    """Run all export_table tests"""
    print("🚀 Starting Comprehensive export_table Tests")
    print("=" * 70)

    # Create test suite
    suite = unittest.TestSuite()

    # Add test cases
    loader = unittest.TestLoader()
    suite.addTests(loader.loadTestsFromTestCase(TestExportTableFormats))
    suite.addTests(loader.loadTestsFromTestCase(TestExportDataFormats))
    suite.addTests(loader.loadTestsFromTestCase(TestExportUtilities))

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
        print("\n🎉 All tests passed! export_table function is working correctly.")
    else:
        print(f"\n❌ Some tests failed. Please check the issues above.")

    return success


if __name__ == "__main__":
    # Run comprehensive tests
    success = run_export_tests()
    sys.exit(0 if success else 1)