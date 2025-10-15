#!/usr/bin/env python3
"""
Unit tests for list of dict input support in MCP tools.

This test file verifies that the MCP tools (write_new_sheet, append_rows,
append_columns, update_range) can handle list of dict input format,
which is a DataFrame-like representation.

Usage:
    python tests/test_list_of_dict_input.py
"""

import sys
from pathlib import Path

# Add parent directory to path to import the module
sys.path.insert(0, str(Path(__file__).parent.parent))

from datatable_tools.google_sheets_helpers import process_data_input


# Test case definition structure
TEST_CASES = [
    {
        "name": "Simple List of Dicts",
        "description": "Basic DataFrame-like list of dicts with consistent keys",
        "data": [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "Los Angeles"},
            {"name": "Charlie", "age": 35, "city": "Chicago"}
        ],
        "expected_headers": ["name", "age", "city"],
        "expected_data_rows": 3,
        "expected_first_row": ["Alice", 30, "New York"],
    },
    {
        "name": "Tesla Financial Metrics (Dict Format)",
        "description": "Financial data in list of dicts format",
        "data": [
            {"Metric": "Revenue ($B)", "Q2 2024": "25.5", "Q3 2024": "25.2", "Q4 2024": "25.7"},
            {"Metric": "Net Income ($B)", "Q2 2024": "1.40", "Q3 2024": "2.17", "Q4 2024": "2.36"},
            {"Metric": "Net Margin (%)", "Q2 2024": "5.49", "Q3 2024": "8.61", "Q4 2024": "9.16"},
        ],
        "expected_headers": ["Metric", "Q2 2024", "Q3 2024", "Q4 2024"],
        "expected_data_rows": 3,
        "expected_first_row": ["Revenue ($B)", "25.5", "25.2", "25.7"],
    },
    {
        "name": "Mixed Data Types",
        "description": "List of dicts with mixed data types (str, int, float, bool, None)",
        "data": [
            {"product": "Laptop", "price": 999.99, "in_stock": True, "quantity": 15},
            {"product": "Mouse", "price": 25.99, "in_stock": True, "quantity": 50},
            {"product": "Monitor", "price": 299.99, "in_stock": False, "quantity": 0}
        ],
        "expected_headers": ["product", "price", "in_stock", "quantity"],
        "expected_data_rows": 3,
        "expected_first_row": ["Laptop", 999.99, True, 15],
    },
    {
        "name": "Sparse Data with None Values",
        "description": "List of dicts with missing keys (handled as None)",
        "data": [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "city": "Los Angeles"},  # Missing 'age'
            {"name": "Charlie", "age": 35}  # Missing 'city'
        ],
        "expected_headers": ["name", "age", "city"],
        "expected_data_rows": 3,
        "expected_first_row": ["Alice", 30, "New York"],
        "expected_second_row": ["Bob", None, "Los Angeles"],
        "expected_third_row": ["Charlie", 35, None],
    },
    {
        "name": "Single Dict",
        "description": "Single row as list of dict",
        "data": [
            {"name": "Alice", "age": 30, "city": "New York"}
        ],
        "expected_headers": ["name", "age", "city"],
        "expected_data_rows": 1,
        "expected_first_row": ["Alice", 30, "New York"],
    },
    {
        "name": "Empty List",
        "description": "Empty list should return no headers and no data",
        "data": [],
        "expected_headers": None,
        "expected_data_rows": 0,
        "expected_first_row": None,
    },
    {
        "name": "2D Array (Not List of Dicts)",
        "description": "Regular 2D array should pass through unchanged",
        "data": [
            ["Alice", 30, "New York"],
            ["Bob", 25, "Los Angeles"]
        ],
        "expected_headers": None,  # No headers extracted from 2D array
        "expected_data_rows": 2,
        "expected_first_row": ["Alice", 30, "New York"],
    },
]


def run_test_case(test_case):
    """
    Run a single test case and return the result.

    Args:
        test_case: Dictionary containing test case definition

    Returns:
        tuple: (success: bool, error_messages: list)
    """
    data = test_case["data"]
    expected_headers = test_case["expected_headers"]
    expected_data_rows = test_case["expected_data_rows"]
    expected_first_row = test_case["expected_first_row"]

    errors = []

    # Execute the function
    try:
        headers, data_rows = process_data_input(data)
    except Exception as e:
        return False, [f"Exception during processing: {e}"]

    # Verify headers
    if headers != expected_headers:
        errors.append(f"Headers mismatch:")
        errors.append(f"  Expected: {expected_headers}")
        errors.append(f"  Got:      {headers}")

    # Verify data row count
    if len(data_rows) != expected_data_rows:
        errors.append(f"Data row count mismatch:")
        errors.append(f"  Expected: {expected_data_rows} rows")
        errors.append(f"  Got:      {len(data_rows)} rows")

    # Verify first data row (if data exists)
    if expected_first_row is not None and len(data_rows) > 0:
        if data_rows[0] != expected_first_row:
            errors.append(f"First data row mismatch:")
            errors.append(f"  Expected: {expected_first_row}")
            errors.append(f"  Got:      {data_rows[0]}")

    # Verify additional rows if specified
    if "expected_second_row" in test_case and len(data_rows) > 1:
        if data_rows[1] != test_case["expected_second_row"]:
            errors.append(f"Second data row mismatch:")
            errors.append(f"  Expected: {test_case['expected_second_row']}")
            errors.append(f"  Got:      {data_rows[1]}")

    if "expected_third_row" in test_case and len(data_rows) > 2:
        if data_rows[2] != test_case["expected_third_row"]:
            errors.append(f"Third data row mismatch:")
            errors.append(f"  Expected: {test_case['expected_third_row']}")
            errors.append(f"  Got:      {data_rows[2]}")

    return len(errors) == 0, errors


def print_test_result(test_case, success, errors):
    """Print formatted test result"""
    name = test_case["name"]
    description = test_case["description"]

    print(f"\n{'='*70}")
    print(f"📝 {name}")
    print(f"{'='*70}")
    print(f"Description: {description}")

    # Print input info
    data = test_case["data"]
    if len(data) > 0:
        if isinstance(data[0], dict):
            print(f"\nInput: List of {len(data)} dicts")
            print(f"       Keys: {list(data[0].keys())}")
        else:
            print(f"\nInput: 2D array with {len(data)} rows")
    else:
        print(f"\nInput: Empty list")

    # Print result
    if success:
        print(f"\n✅ PASS")
        if test_case["expected_headers"]:
            print(f"   Headers extracted: {test_case['expected_headers']}")
        print(f"   Data rows: {test_case['expected_data_rows']}")
    else:
        print(f"\n❌ FAIL")
        for error in errors:
            print(f"   {error}")


def run_all_tests():
    """Run all test cases and report results"""
    print("\n" + "="*70)
    print("🧪 Testing List of Dict Input Support")
    print("="*70)
    print("\nThis test suite verifies that MCP tools can handle DataFrame-like")
    print("list of dict input format in addition to 2D arrays.")

    results = []

    for test_case in TEST_CASES:
        success, errors = run_test_case(test_case)
        results.append((test_case["name"], success))
        print_test_result(test_case, success, errors)

    # Print summary
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")

    print("\n" + "="*70)
    if passed == total:
        print(f"🎉 ALL TESTS PASSED! ({passed}/{total})")
        print("="*70)
        print("\n✅ List of dict input support is working correctly!")
        return True
    else:
        print(f"❌ SOME TESTS FAILED ({passed}/{total} passed)")
        print("="*70)
        print("\n⚠️  List of dict input support needs attention!")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
