#!/usr/bin/env python3
"""
Unit tests for _process_data_input header detection.

This test file directly tests the _process_data_input function to verify
that header detection works correctly for various data formats.

Bug Context:
- User reported that write_new_sheet was creating sheets with "Column_1", "Column_2"
  as headers instead of detecting the actual headers from the first row
- Root cause: _process_data_input was detecting headers but they weren't being
  properly passed through to the Google Sheets creation

Usage:
    python tests/test_process_data_input_headers.py
"""

import sys
from pathlib import Path

# Add parent directory to path to import the module
sys.path.insert(0, str(Path(__file__).parent.parent))

from datatable_tools.utils import _process_data_input


# Test case definition structure
TEST_CASES = [
    {
        "name": "Tesla Financial Metrics",
        "description": "Exact data from user's bug report - should detect headers from first row",
        "data": [
            ["Metric", "Q2 2024", "Q3 2024", "Q4 2024", "Q1 2025", "Q2 2025"],
            ["Revenue ($B)", "25.5", "25.2", "25.7", "19.3", "22.5"],
            ["Net Income ($B)", "1.40", "2.17", "2.36", "0.41", "1.17"],
            ["Net Margin (%)", "5.49", "8.61", "9.16", "2.12", "5.21"],
            ["EPS ($)", "0.40", "0.62", "0.66", "0.12", "0.33"],
            ["Revenue Growth QoQ (%)", "8.9", "-1.2", "2.0", "-24.8", "16.5"],
            ["Operating Margin (%)", "6.29", "10.79", "6.16", "2.06", "4.10"],
            ["Gross Margin (%)", "17.95", "19.84", "16.26", "16.31", "17.24"],
            ["R&D ($B)", "1.07", "1.04", "1.28", "1.41", "1.59"],
            ["Operating Cash Flow ($B)", "2.89", "6.25", "4.79", "0.24", "3.42"],
            ["Free Cash Flow ($B)", "1.01", "3.28", "2.03", "-2.56", "1.33"],
            ["Cash Balance ($B)", "30.7", "33.6", "36.6", "27.1", "36.8"],
            ["Total Debt ($B)", "5.89", "5.86", "6.12", "5.53", "6.64"],
            ["Market Cap ($B)", "669.7", "794.7", "1297.5", "558.3", "1023.8"],
            ["P/E Ratio", "119.6", "96.5", "137.7", "100.4", "218.4"]
        ],
        "input_headers": None,
        "expected_headers": ["Metric", "Q2 2024", "Q3 2024", "Q4 2024", "Q1 2025", "Q2 2025"],
        "expected_data_rows": 14,  # 15 - 1 (header removed)
        "expected_first_data_row": ["Revenue ($B)", "25.5", "25.2", "25.7", "19.3", "22.5"],
        "should_detect_headers": True,
    },
    {
        "name": "Product Catalog",
        "description": "Simple table with clear headers",
        "data": [
            ["Product Name", "Price", "Category", "Stock", "Rating"],
            ["Laptop", "999.99", "Electronics", "15", "4.5"],
            ["Mouse", "25.99", "Electronics", "50", "4.2"],
            ["Desk Chair", "199.99", "Furniture", "8", "4.7"],
        ],
        "input_headers": None,
        "expected_headers": ["Product Name", "Price", "Category", "Stock", "Rating"],
        "expected_data_rows": 3,
        "expected_first_data_row": ["Laptop", "999.99", "Electronics", "15", "4.5"],
        "should_detect_headers": True,
    },
    {
        "name": "Comparison Table (Short Headers + Long Content)",
        "description": "Short first row with long content in subsequent rows",
        "data": [
            ["Dimension", "Agent Kit", "n8n"],
            ["Primary Purpose", "Fast, visual, chat-first agents inside OpenAI's ecosystem with advanced capabilities.", "General-purpose workflow automation + AI agents; code-optional but developer-friendly."],
            ["Pricing Model", "Free while in beta; pay only for token usage with transparent pricing structure.", "Open-source free self-host; cloud plans metered by workflow executions."]
        ],
        "input_headers": None,
        "expected_headers": ["Dimension", "Agent Kit", "n8n"],
        "expected_data_rows": 2,
        "expected_first_data_row": ["Primary Purpose", "Fast, visual, chat-first agents inside OpenAI's ecosystem with advanced capabilities.", "General-purpose workflow automation + AI agents; code-optional but developer-friendly."],
        "should_detect_headers": True,
    },
    {
        "name": "Explicit Headers Override",
        "description": "Explicit headers should override auto-detection",
        "data": [
            ["Product", "Price", "Stock"],
            ["Laptop", "999.99", "15"],
            ["Mouse", "25.99", "50"]
        ],
        "input_headers": ["Item Name", "Cost", "Quantity"],
        "expected_headers": ["Item Name", "Cost", "Quantity"],
        "expected_data_rows": 3,  # All rows preserved when explicit headers provided
        "expected_first_data_row": ["Product", "Price", "Stock"],
        "should_detect_headers": False,  # Explicit headers provided
    },
    {
        "name": "Numeric First Row (No Header Detection)",
        "description": "Numeric first row should NOT be detected as headers",
        "data": [
            [1, 2, 3, 4, 5],
            [10, 20, 30, 40, 50],
            [100, 200, 300, 400, 500]
        ],
        "input_headers": None,
        "expected_headers": [],  # No headers generated
        "expected_data_rows": 3,  # All rows preserved
        "expected_first_data_row": [1, 2, 3, 4, 5],
        "should_detect_headers": False,
    },
    {
        "name": "Single Row Data",
        "description": "Single row should be treated as data, not headers",
        "data": [["Name", "Age", "City"]],
        "input_headers": None,
        "expected_headers": [],  # No headers generated
        "expected_data_rows": 1,
        "expected_first_data_row": ["Name", "Age", "City"],
        "should_detect_headers": False,
    },
    {
        "name": "Mixed Types First Row",
        "description": "Mixed types (str/int) in first row - no header detection",
        "data": [
            ["Product", 123, "Category"],
            ["Laptop", 999, "Electronics"],
            ["Mouse", 25, "Electronics"]
        ],
        "input_headers": None,
        "expected_headers": [],  # No headers generated
        "expected_data_rows": 3,
        "expected_first_data_row": ["Product", 123, "Category"],
        "should_detect_headers": False,
    },
    {
        "name": "All String First Row (Similar Lengths)",
        "description": "Single-word strings can be headers - updated behavior",
        "data": [
            ["Apple", "Banana", "Cherry"],
            ["Orange", "Grape", "Mango"],
            ["Peach", "Plum", "Berry"]
        ],
        "input_headers": None,
        "expected_headers": ["Apple", "Banana", "Cherry"],  # Now detects as headers
        "expected_data_rows": 2,  # First row removed as headers
        "expected_first_data_row": ["Orange", "Grape", "Mango"],
        "should_detect_headers": True,  # Changed: single words match header pattern
    },
    {
        "name": "Empty Data",
        "description": "Empty data should return empty results",
        "data": [],
        "input_headers": None,
        "expected_headers": [],
        "expected_data_rows": 0,
        "expected_first_data_row": None,
        "should_detect_headers": False,
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
    name = test_case["name"]
    data = test_case["data"]
    input_headers = test_case["input_headers"]
    expected_headers = test_case["expected_headers"]
    expected_data_rows = test_case["expected_data_rows"]
    expected_first_data_row = test_case["expected_first_data_row"]
    should_detect_headers = test_case["should_detect_headers"]

    errors = []

    # Execute the function
    try:
        processed_data, processed_headers = _process_data_input(data, headers=input_headers)
    except Exception as e:
        return False, [f"Exception during processing: {e}"]

    # Verify headers
    if processed_headers != expected_headers:
        errors.append(f"Headers mismatch:")
        errors.append(f"  Expected: {expected_headers}")
        errors.append(f"  Got:      {processed_headers}")

        # Check if headers were unexpectedly detected
        if should_detect_headers and len(processed_headers) == 0:
            errors.append(f"  🐛 BUG: No headers detected when detection should have worked!")
        elif not should_detect_headers and len(processed_headers) > 0:
            errors.append(f"  🐛 BUG: Headers detected when they shouldn't be!")

    # Verify data row count
    if len(processed_data) != expected_data_rows:
        errors.append(f"Data row count mismatch:")
        errors.append(f"  Expected: {expected_data_rows} rows")
        errors.append(f"  Got:      {len(processed_data)} rows")

    # Verify first data row (if data exists)
    if expected_first_data_row is not None and len(processed_data) > 0:
        if processed_data[0] != expected_first_data_row:
            errors.append(f"First data row mismatch:")
            errors.append(f"  Expected: {expected_first_data_row}")
            errors.append(f"  Got:      {processed_data[0]}")

            # Check if headers ended up in data
            if should_detect_headers and processed_data[0] == expected_headers:
                errors.append(f"  🐛 BUG: First data row contains headers! Headers were not extracted.")

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
        print(f"\nInput: {len(data)} rows × {len(data[0]) if data else 0} columns")
        if test_case["input_headers"]:
            print(f"Explicit headers: {test_case['input_headers']}")
    else:
        print(f"\nInput: Empty data")

    # Print result
    if success:
        print(f"\n✅ PASS")
        if test_case["should_detect_headers"]:
            print(f"   Headers detected: {test_case['expected_headers']}")
            print(f"   Data rows: {test_case['expected_data_rows']}")
    else:
        print(f"\n❌ FAIL")
        for error in errors:
            print(f"   {error}")


def run_all_tests():
    """Run all test cases and report results"""
    print("\n" + "="*70)
    print("🧪 Testing _process_data_input Header Detection")
    print("="*70)
    print("\nThis test suite verifies the fix for the header detection bug")
    print("where 'Column_1', 'Column_2', etc. were appearing instead of")
    print("the actual headers from the first row.")

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
        print("\n✅ The header detection bug fix is working correctly!")
        return True
    else:
        print(f"❌ SOME TESTS FAILED ({passed}/{total} passed)")
        print("="*70)
        print("\n⚠️  The header detection needs attention!")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
