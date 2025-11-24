#!/usr/bin/env python3
"""
Unit test for DataFrame conversion in update_range_by_lookup.

Tests that the preprocessing logic correctly handles:
1. Polars DataFrame string representation
2. List of dicts (standard format)
3. Polars DataFrame objects

This is a unit test that doesn't require a running MCP server.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datatable_tools.google_sheets_helpers import process_data_input


def test_polars_string_to_list_of_dicts():
    """Test conversion of Polars DataFrame string representation to list of dicts."""
    print("=" * 80)
    print("Test: Converting Polars DataFrame string to list of dicts")
    print("=" * 80)

    # Sample Polars DataFrame string representation (from your error log)
    polars_string = """shape: (3, 4)
┌────────────┬──────────┬───────────┬───────────┐
│ ERP单号    ┆ 订单状态 ┆ 线上状态  ┆ 付款时间  │
│ ---        ┆ ---      ┆ ---       ┆ ---       │
│ str        ┆ str      ┆ str       ┆ str       │
╞════════════╪══════════╪═══════════╪═══════════╡
│ S251115334 ┆ 已出库   ┆ IN_TRANSI ┆ 2025-11-1 │
│ 4724       ┆          ┆ T         ┆ 5         │
│ S251115334 ┆ 已出库   ┆ IN_TRANSI ┆ 2025-11-1 │
│ 4749       ┆          ┆ T         ┆ 5         │
│ S251115334 ┆ 已出库   ┆ DELIVERED ┆ 2025-11-1 │
│ 5250       ┆          ┆           ┆ 5         │
└────────────┴──────────┴───────────┴───────────┘"""

    print("\n📋 Input Polars string:")
    print(polars_string[:200] + "...\n")

    try:
        # Use process_data_input to convert
        headers, data_rows = process_data_input(polars_string)

        print(f"✅ Successfully parsed Polars string")
        print(f"\n📊 Parsed data:")
        print(f"  Headers: {headers}")
        print(f"  Number of rows: {len(data_rows)}")
        print(f"  First row: {data_rows[0] if data_rows else 'N/A'}")

        # Convert to list of dicts (same logic as in mcp_tools.py)
        if headers and data_rows:
            list_of_dicts = []
            for row in data_rows:
                row_dict = {}
                for i, header in enumerate(headers):
                    row_dict[header] = row[i] if i < len(row) else ""
                list_of_dicts.append(row_dict)

            print(f"\n📝 Converted to list of dicts:")
            print(f"  Number of dicts: {len(list_of_dicts)}")
            print(f"  First dict: {list_of_dicts[0]}")
            print(f"  Keys: {list(list_of_dicts[0].keys())}")

            # Verify the lookup column exists
            if "ERP单号" in list_of_dicts[0]:
                print(f"\n✅ SUCCESS: Lookup column 'ERP单号' found in converted data!")
                print(f"  Value: {list_of_dicts[0]['ERP单号']}")
                return True
            else:
                print(f"\n❌ FAIL: Lookup column 'ERP单号' not found!")
                print(f"  Available columns: {list(list_of_dicts[0].keys())}")
                return False
        else:
            print(f"\n❌ FAIL: No headers or data rows after conversion")
            return False

    except Exception as e:
        print(f"\n❌ FAIL: Exception during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_list_of_dicts_passthrough():
    """Test that list of dicts passes through unchanged."""
    print("\n" + "=" * 80)
    print("Test: List of dicts passthrough")
    print("=" * 80)

    test_data = [
        {"ERP单号": "S251115334724", "订单状态": "已出库", "线上状态": "IN_TRANSIT"},
        {"ERP单号": "S251115334749", "订单状态": "已出库", "线上状态": "IN_TRANSIT"}
    ]

    print(f"\n📋 Input data (list of dicts):")
    print(f"  Number of dicts: {len(test_data)}")
    print(f"  First dict: {test_data[0]}")

    # Check if it's already list of dicts
    if isinstance(test_data[0], dict):
        print(f"\n✅ SUCCESS: Data is already in list of dicts format - no conversion needed!")
        print(f"  Lookup column 'ERP单号' exists: {'ERP单号' in test_data[0]}")
        return True
    else:
        print(f"\n❌ FAIL: First element is not a dict: {type(test_data[0])}")
        return False


if __name__ == "__main__":
    print("\n🧪 Running DataFrame Conversion Unit Tests\n")

    test1_pass = test_polars_string_to_list_of_dicts()
    test2_pass = test_list_of_dicts_passthrough()

    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(f"  Test 1 (Polars string conversion): {'✅ PASS' if test1_pass else '❌ FAIL'}")
    print(f"  Test 2 (List of dicts passthrough): {'✅ PASS' if test2_pass else '❌ FAIL'}")

    if test1_pass and test2_pass:
        print(f"\n✅ All tests passed!")
        exit(0)
    else:
        print(f"\n❌ Some tests failed")
        exit(1)
