#!/usr/bin/env python3
"""
Simple test script to demonstrate create_table with different data formats
This is a quick demo script that shows all the supported data input formats
"""

import sys
import json
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, '..')

from datatable_tools.lifecycle_tools import _process_data_input
from datatable_tools.table_manager import table_manager

def demo_create_table_formats():
    """Demonstrate all supported data formats for create_table"""

    print("🚀 Demo: create_table with Different Data Formats")
    print("=" * 60)

    def create_table_helper(data, headers=None, name="Demo Table"):
        """Helper to create table using table manager directly"""
        try:
            # Process data using the same logic as the MCP tool
            processed_data, processed_headers = _process_data_input(data, headers)

            # Create table using table manager
            table_id = table_manager.create_table(
                data=processed_data,
                headers=processed_headers,
                name=name,
                source_info={"type": "demo_creation"}
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

    results = []

    # Clean up any existing tables
    table_manager.cleanup_expired_tables(force=True)

    # Test 1: Traditional 2D List
    print("\n📋 Test 1: Traditional 2D List")
    data = [
        ["Alice", 25, "Engineer"],
        ["Bob", 30, "Manager"],
        ["Carol", 28, "Designer"]
    ]
    headers = ["Name", "Age", "Role"]

    result = create_table_helper(data, headers, "Traditional 2D List")
    results.append(("Traditional 2D List", result))
    print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")

    # Test 2: Dictionary Format (Most Pandas-like)
    print("\n📊 Test 2: Dictionary Format (Column-oriented)")
    data = {
        "Product": ["Laptop", "Mouse", "Keyboard"],
        "Price": [999.99, 29.99, 79.99],
        "Stock": [25, 150, 75],
        "Category": ["Electronics", "Electronics", "Electronics"]
    }

    result = create_table_helper(data, name="Dictionary Format")
    results.append(("Dictionary Format", result))
    print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")
    print(f"   📝 Headers: {result['headers']}")

    # Test 3: Records Format (List of Dicts)
    print("\n📝 Test 3: Records Format (List of Dictionaries)")
    data = [
        {"Name": "iPhone 14", "Price": 999, "Brand": "Apple"},
        {"Name": "Galaxy S23", "Price": 899, "Brand": "Samsung"},
        {"Name": "Pixel 7", "Price": 699, "Brand": "Google"}
    ]

    result = create_table_helper(data, name="Records Format")
    results.append(("Records Format", result))
    print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")
    print(f"   📝 Headers: {result['headers']}")

    # Test 4: Single Column List
    print("\n📈 Test 4: Single Column List")
    data = [10, 20, 30, 40, 50]
    headers = ["Values"]

    result = create_table_helper(data, headers, "Single Column")
    results.append(("Single Column", result))
    print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")

    # Test 5: Single Row Dictionary
    print("\n📄 Test 5: Single Row Dictionary")
    data = {"Name": "Tesla Model 3", "Price": 35000, "Range": 358, "Type": "Electric"}

    result = create_table_helper(data, name="Single Row")
    results.append(("Single Row", result))
    print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")

    # Test 6: Scalar Values
    print("\n🔢 Test 6: Scalar Values")
    scalar_tests = [
        (42, "Integer"),
        (3.14159, "Float"),
        ("Hello, World!", "String"),
        (True, "Boolean")
    ]

    for value, type_name in scalar_tests:
        result = create_table_helper(value, name=f"Scalar {type_name}")
        results.append((f"Scalar {type_name}", result))
        print(f"   ✅ {type_name}: {result['success']}, Value: {value}")

    # Test 7: Mixed Data Types
    print("\n🎭 Test 7: Mixed Data Types")
    data = {
        "ID": [1, 2, 3],
        "Name": ["Alice", "Bob", "Carol"],
        "Score": [95.5, 87.2, 92.0],
        "Active": [True, False, True],
        "Notes": ["Excellent", None, "Good"],
        "Date": ["2024-01-01", "2024-01-02", "2024-01-03"]
    }

    result = create_table_helper(data, name="Mixed Types")
    results.append(("Mixed Types", result))
    print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")

    # Test 8: Pandas DataFrame (if available)
    print("\n🐼 Test 8: Pandas DataFrame")
    try:
        import pandas as pd

        df = pd.DataFrame({
            "Country": ["USA", "UK", "Canada", "Australia"],
            "Population": [331_000_000, 67_000_000, 38_000_000, 25_000_000],
            "GDP": [21.43, 2.83, 1.74, 1.55],
            "Currency": ["USD", "GBP", "CAD", "AUD"]
        })

        result = create_table_helper(df, name="Pandas DataFrame")
        results.append(("Pandas DataFrame", result))
        print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")

    except ImportError:
        print("   ⚠️  Pandas not available, skipping DataFrame test")

    # Test 9: NumPy Array (if available)
    print("\n🔢 Test 9: NumPy Array")
    try:
        import numpy as np

        # 2D array
        arr = np.array([
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ])

        result = create_table_helper(arr, ["X", "Y", "Z"], "NumPy Array")
        results.append(("NumPy Array", result))
        print(f"   ✅ Success: {result['success']}, Shape: {result['shape']}")

    except ImportError:
        print("   ⚠️  NumPy not available, skipping array test")

    # Test 10: Empty Data
    print("\n🗳️  Test 10: Empty Data")
    empty_tests = [
        ([], "Empty List"),
        ({}, "Empty Dict")
    ]

    for data, desc in empty_tests:
        result = create_table_helper(data, name=desc)
        results.append((desc, result))
        print(f"   ✅ {desc}: {result['success']}, Shape: {result['shape']}")

    # Summary
    print("\n" + "=" * 60)
    print("📊 Summary of All Tests")
    print("=" * 60)

    successful = 0
    failed = 0

    for test_name, result in results:
        status = "✅ PASS" if result["success"] else "❌ FAIL"
        shape = result.get("shape", "N/A")
        table_id = result.get("table_id", "N/A")

        print(f"{status} {test_name:<25} Shape: {str(shape):<8} ID: {table_id}")

        if result["success"]:
            successful += 1
        else:
            failed += 1
            print(f"      Error: {result.get('error', 'Unknown error')}")

    print("\n" + "=" * 60)
    print(f"🎯 Results: {successful} passed, {failed} failed")

    if failed == 0:
        print("🎉 All tests passed! create_table supports all expected data formats.")
    else:
        print("❌ Some tests failed. Check the errors above.")

    # List all created tables
    print(f"\n📋 Created {len(results)} tables in this session")
    tables = table_manager.list_tables()
    for table_info in tables:
        print(f"   - {table_info['table_id']}: {table_info['name']} ({table_info['shape']})")

    return failed == 0

if __name__ == "__main__":
    print(f"⏰ Starting demo at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    success = demo_create_table_formats()
    print(f"\n⏰ Demo completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(0 if success else 1)