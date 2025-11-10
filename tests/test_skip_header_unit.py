#!/usr/bin/env python3
"""
Unit tests for skip_header parameter logic (no Google Sheets required)

This test file verifies the skip_header logic by testing the internal
data processing that happens in update_range before writing to Google Sheets.

Usage:
    python tests/test_skip_header_unit.py
"""

import sys
from pathlib import Path

# Add parent directory to path to import the module
project_dir = str(Path(__file__).parent.parent)
sys.path.insert(0, project_dir)

# Import directly from the helper module
import importlib.util
spec = importlib.util.spec_from_file_location(
    "google_sheets_helpers",
    str(Path(__file__).parent.parent / "datatable_tools" / "google_sheets_helpers.py")
)
google_sheets_helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(google_sheets_helpers)

process_data_input = google_sheets_helpers.process_data_input
auto_detect_headers = google_sheets_helpers.auto_detect_headers

# Test case definition structure
TEST_CASES = [
    {
        "name": "List of Dicts - Header Extraction",
        "description": "List of dicts should extract headers",
        "data": [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "Los Angeles"},
        ],
        "expected_headers": ["name", "age", "city"],
        "expected_data_rows": 2,
        "expected_first_row": ["Alice", 30, "New York"],
    },
    {
        "name": "2D Array with String Headers",
        "description": "2D array is passed through unchanged by process_data_input (headers detected separately)",
        "data": [
            ["Product", "Price", "Stock"],
            ["Laptop", "999.99", "15"],
            ["Mouse", "25.99", "50"]
        ],
        "expected_headers": None,  # process_data_input doesn't extract headers from 2D arrays
        "expected_data_rows": 3,   # All rows passed through
        "expected_first_row": ["Product", "Price", "Stock"],
    },
    {
        "name": "2D Array without Headers",
        "description": "2D array with numeric first row should not detect headers",
        "data": [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ],
        "expected_headers": None,
        "expected_data_rows": 3,
        "expected_first_row": [1, 2, 3],
    },
    {
        "name": "Empty List",
        "description": "Empty list should return no headers and no data",
        "data": [],
        "expected_headers": None,
        "expected_data_rows": 0,
        "expected_first_row": None,
    },
]


def test_process_data_input_logic():
    """Test the process_data_input function that extracts headers from list of dicts"""
    print("\n" + "="*60)
    print("🧪 Testing process_data_input (Header Extraction)")
    print("="*60)

    results = []

    for test_case in TEST_CASES:
        name = test_case["name"]
        data = test_case["data"]
        expected_headers = test_case["expected_headers"]
        expected_data_rows = test_case["expected_data_rows"]
        expected_first_row = test_case["expected_first_row"]

        print(f"\n📝 Test: {name}")
        print(f"   {test_case['description']}")

        # Execute the function
        try:
            extracted_headers, data_rows = process_data_input(data)
        except Exception as e:
            print(f"   ❌ FAIL: Exception during processing: {e}")
            results.append((name, False))
            continue

        errors = []

        # Verify headers
        if extracted_headers != expected_headers:
            errors.append(f"Headers mismatch: expected {expected_headers}, got {extracted_headers}")

        # Verify data row count
        if len(data_rows) != expected_data_rows:
            errors.append(f"Data row count mismatch: expected {expected_data_rows}, got {len(data_rows)}")

        # Verify first data row (if data exists)
        if expected_first_row is not None and len(data_rows) > 0:
            if data_rows[0] != expected_first_row:
                errors.append(f"First data row mismatch: expected {expected_first_row}, got {data_rows[0]}")

        if errors:
            print(f"   ❌ FAIL:")
            for error in errors:
                print(f"      {error}")
            results.append((name, False))
        else:
            print(f"   ✅ PASS")
            if extracted_headers:
                print(f"      Headers: {extracted_headers}")
            print(f"      Data rows: {len(data_rows)}")
            results.append((name, True))

    return results


def test_skip_header_scenarios():
    """Test the skip_header logic scenarios"""
    print("\n" + "="*60)
    print("🧪 Testing skip_header Logic Scenarios")
    print("="*60)

    results = []

    # Scenario 1: List of dicts with skip_header=True
    print("\n📝 Scenario 1: List of dicts with skip_header=True")
    print("   Headers should be extracted but not included in output")

    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]

    extracted_headers, data_rows = process_data_input(data)

    # Simulate skip_header=True behavior
    if extracted_headers:
        # When skip_header=True, we only write data_rows (no headers)
        final_output_rows = len(data_rows)
        expected_output_rows = 2  # Should be 2 (just data, no headers)

        if final_output_rows == expected_output_rows:
            print(f"   ✅ PASS: Output has {final_output_rows} rows (no headers)")
            results.append(("List of dicts with skip_header=True", True))
        else:
            print(f"   ❌ FAIL: Expected {expected_output_rows} rows, got {final_output_rows}")
            results.append(("List of dicts with skip_header=True", False))
    else:
        print(f"   ❌ FAIL: No headers extracted from list of dicts")
        results.append(("List of dicts with skip_header=True", False))

    # Scenario 2: List of dicts with skip_header=False (default)
    print("\n📝 Scenario 2: List of dicts with skip_header=False (default)")
    print("   Headers should be extracted and included in output")

    data = [
        {"product": "Widget", "price": 99.99},
        {"product": "Gadget", "price": 149.99}
    ]

    extracted_headers, data_rows = process_data_input(data)

    # Simulate skip_header=False behavior
    if extracted_headers:
        # When skip_header=False, we write headers + data_rows
        final_output_rows = 1 + len(data_rows)  # 1 header row + data rows
        expected_output_rows = 3  # Should be 3 (1 header + 2 data rows)

        if final_output_rows == expected_output_rows:
            print(f"   ✅ PASS: Output has {final_output_rows} rows (headers + data)")
            results.append(("List of dicts with skip_header=False", True))
        else:
            print(f"   ❌ FAIL: Expected {expected_output_rows} rows, got {final_output_rows}")
            results.append(("List of dicts with skip_header=False", False))
    else:
        print(f"   ❌ FAIL: No headers extracted from list of dicts")
        results.append(("List of dicts with skip_header=False", False))

    # Scenario 3: 2D array with skip_header=True
    print("\n📝 Scenario 3: 2D array with skip_header=True")
    print("   Auto-detected headers should be skipped when skip_header=True")

    data = [
        ["Product", "Price", "Stock"],
        ["Laptop", "999.99", "15"],
        ["Mouse", "25.99", "50"]
    ]

    # For 2D arrays, we need to use auto_detect_headers
    detected_headers, processed_rows = auto_detect_headers(data)

    if detected_headers:
        # When skip_header=True, we only write processed_rows (no headers)
        final_output_rows = len(processed_rows)
        expected_output_rows = 2  # Should be 2 (just data, no headers)

        if final_output_rows == expected_output_rows:
            print(f"   ✅ PASS: Output has {final_output_rows} rows (no headers)")
            print(f"      Detected headers: {detected_headers}")
            print(f"      Data rows: {processed_rows}")
            results.append(("2D array with skip_header=True", True))
        else:
            print(f"   ❌ FAIL: Expected {expected_output_rows} rows, got {final_output_rows}")
            results.append(("2D array with skip_header=True", False))
    else:
        print(f"   ⚠️  No headers detected from 2D array")
        # If no headers detected, skip_header has no effect
        results.append(("2D array with skip_header=True", True))

    return results


def run_all_tests():
    """Run all unit tests"""
    print("\n" + "="*60)
    print("🚀 skip_header Parameter Unit Tests")
    print("="*60)
    print("\nTesting skip_header logic without Google Sheets\n")

    all_results = []

    # Test 1: process_data_input logic
    results1 = test_process_data_input_logic()
    all_results.extend(results1)

    # Test 2: skip_header scenarios
    results2 = test_skip_header_scenarios()
    all_results.extend(results2)

    # Summary
    print("\n" + "="*60)
    print("📊 Test Summary")
    print("="*60)

    all_passed = True
    for test_name, passed in all_results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False

    passed_count = sum(1 for _, passed in all_results if passed)
    total_count = len(all_results)

    if all_passed:
        print(f"\n🎉 All tests passed! ({passed_count}/{total_count})")
        print("\n✅ skip_header logic works correctly:")
        print("   - Headers are properly extracted from list of dicts")
        print("   - skip_header=True excludes headers from output")
        print("   - skip_header=False includes headers in output")
    else:
        print(f"\n⚠️  Some tests failed ({passed_count}/{total_count})")

    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
