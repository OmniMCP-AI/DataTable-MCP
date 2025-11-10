#!/usr/bin/env python3
"""
Test skip_header parameter in update_range function

Tests that update_range correctly handles the skip_header parameter
for both list of dicts and DataFrame inputs.

Usage:
    python tests/test_skip_header.py

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

import asyncio
import os
from datetime import datetime

from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable
from datatable_tools.auth.service_factory import create_google_service_from_env

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None


# Test URI - replace with your own test spreadsheet
READ_WRITE_URI = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=1210260183#gid=1210260183"


def create_google_service():
    """Create Google Sheets service using OAuth credentials"""
    return create_google_service_from_env(env_prefix="TEST_GOOGLE_OAUTH")


async def test_skip_header_with_list_of_dicts():
    """Test skip_header=True with list of dicts - should skip headers"""
    print("\n" + "="*60)
    print("Test 1: skip_header=True with list of dicts")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create list of dicts (DataFrame-like)
    data = [
        {"name": "Alice", "age": 25, "status": "Active"},
        {"name": "Bob", "age": 30, "status": "Pending"}
    ]

    print(f"   📊 Input data (list of dicts):")
    print(f"      Rows: {len(data)}")
    print(f"      Keys: {list(data[0].keys())}")
    for row in data:
        print(f"      {row}")

    # Call update_range with skip_header=True
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=data,
        range_address='A20',
        skip_header=True  # Should skip writing headers
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully updated range with skip_header=True")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape - should be (2,3) for 2 rows without headers
        expected_shape = "(2,3)"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches (no headers): {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def test_skip_header_false_with_list_of_dicts():
    """Test skip_header=False (default) with list of dicts - should include headers"""
    print("\n" + "="*60)
    print("Test 2: skip_header=False with list of dicts")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create list of dicts (DataFrame-like)
    data = [
        {"product": "Widget", "price": 99.99, "stock": 10},
        {"product": "Gadget", "price": 149.99, "stock": 5}
    ]

    print(f"   📊 Input data (list of dicts):")
    print(f"      Rows: {len(data)}")
    print(f"      Keys: {list(data[0].keys())}")
    for row in data:
        print(f"      {row}")

    # Call update_range with skip_header=False (default)
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=data,
        range_address='E20',
        skip_header=False  # Should include headers
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully updated range with skip_header=False")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape - should be (3,3) for 2 rows + 1 header row
        expected_shape = "(3,3)"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches (with headers): {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def test_skip_header_with_dataframe():
    """Test skip_header=True with Polars DataFrame - should skip headers"""
    if not POLARS_AVAILABLE:
        print("\n⏭️  Skipping DataFrame test - Polars not available")
        return True

    print("\n" + "="*60)
    print("Test 3: skip_header=True with Polars DataFrame")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create Polars DataFrame
    df = pl.DataFrame({
        'city': ['New York', 'Los Angeles', 'Chicago'],
        'population': [8336817, 3979576, 2693976],
        'state': ['NY', 'CA', 'IL']
    })

    print(f"   📊 Input DataFrame:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(df)

    # Call update_range with skip_header=True
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=df,
        range_address='A30',
        skip_header=True  # Should skip writing column names
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully updated range with skip_header=True (DataFrame)")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape - should be (3,3) for 3 rows without headers
        expected_shape = f"({df.height},{df.width})"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches (no headers): {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def test_skip_header_false_with_dataframe():
    """Test skip_header=False with Polars DataFrame - should include headers"""
    if not POLARS_AVAILABLE:
        print("\n⏭️  Skipping DataFrame test - Polars not available")
        return True

    print("\n" + "="*60)
    print("Test 4: skip_header=False with Polars DataFrame")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create Polars DataFrame
    df = pl.DataFrame({
        'country': ['USA', 'Canada', 'Mexico'],
        'capital': ['Washington DC', 'Ottawa', 'Mexico City'],
        'continent': ['North America', 'North America', 'North America']
    })

    print(f"   📊 Input DataFrame:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(df)

    # Call update_range with skip_header=False
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=df,
        range_address='E30',
        skip_header=False  # Should include column names as headers
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully updated range with skip_header=False (DataFrame)")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape - should be (4,3) for 3 rows + 1 header row
        expected_shape = f"({df.height + 1},{df.width})"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches (with headers): {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def test_skip_header_with_2d_array():
    """Test skip_header with 2D array - should have no effect since no headers to skip"""
    print("\n" + "="*60)
    print("Test 5: skip_header with 2D array (no effect expected)")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    # Create 2D array (no headers to extract)
    data = [
        ["Apple", 1.99, 50],
        ["Banana", 0.99, 100],
        ["Orange", 1.49, 75]
    ]

    print(f"   📊 Input data (2D array):")
    print(f"      Rows: {len(data)}")
    for row in data:
        print(f"      {row}")

    # Call update_range with skip_header=True
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=data,
        range_address='A40',
        skip_header=True  # Should have no effect on 2D arrays
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully updated range with 2D array")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape - should be (3,3) for all data
        expected_shape = "(3,3)"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches: {actual_shape}")
            print(f"   💡 skip_header has no effect on 2D arrays (as expected)")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def test_skip_header_default_behavior():
    """Test default behavior (skip_header not specified) with list of dicts"""
    print("\n" + "="*60)
    print("Test 6: Default behavior (skip_header not specified)")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    # Create list of dicts
    data = [
        {"item": "Keyboard", "price": 79.99},
        {"item": "Mouse", "price": 29.99}
    ]

    print(f"   📊 Input data (list of dicts):")
    print(f"      Rows: {len(data)}")
    for row in data:
        print(f"      {row}")

    # Call update_range WITHOUT skip_header parameter (should default to False)
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=data,
        range_address='E40'
        # skip_header not specified - should default to False
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully updated range with default behavior")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape - should be (3,2) for 2 rows + 1 header row (default includes headers)
        expected_shape = "(3,2)"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches (default includes headers): {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def run_all_tests():
    """Run all skip_header tests"""
    print("\n" + "="*60)
    print("🚀 skip_header Parameter Tests for update_range")
    print("="*60)
    print("\nTesting skip_header parameter with different data types\n")

    results = []

    try:
        # Test 1: skip_header=True with list of dicts
        result1 = await test_skip_header_with_list_of_dicts()
        results.append(("skip_header=True with list of dicts", result1))

        # Test 2: skip_header=False with list of dicts
        result2 = await test_skip_header_false_with_list_of_dicts()
        results.append(("skip_header=False with list of dicts", result2))

        # Test 3: skip_header=True with DataFrame
        result3 = await test_skip_header_with_dataframe()
        results.append(("skip_header=True with DataFrame", result3))

        # Test 4: skip_header=False with DataFrame
        result4 = await test_skip_header_false_with_dataframe()
        results.append(("skip_header=False with DataFrame", result4))

        # Test 5: skip_header with 2D array
        result5 = await test_skip_header_with_2d_array()
        results.append(("skip_header with 2D array", result5))

        # Test 6: Default behavior
        result6 = await test_skip_header_default_behavior()
        results.append(("Default behavior (skip_header not specified)", result6))

    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Summary
    print("\n" + "="*60)
    print("📊 Test Summary")
    print("="*60)

    all_passed = True
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n🎉 All skip_header tests passed!")
        print("\n✅ skip_header parameter works correctly:")
        print("   - skip_header=True: Headers are skipped for list of dicts and DataFrames")
        print("   - skip_header=False (default): Headers are included")
        print("   - 2D arrays: skip_header has no effect (expected)")
    else:
        print("\n⚠️  Some tests failed")

    return all_passed


if __name__ == "__main__":
    # Check environment variables
    env_vars = [
        "TEST_GOOGLE_OAUTH_REFRESH_TOKEN",
        "TEST_GOOGLE_OAUTH_CLIENT_ID",
        "TEST_GOOGLE_OAUTH_CLIENT_SECRET"
    ]

    missing_vars = [var for var in env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("💡 Please set these in your .env file or environment")
        exit(1)

    # Run the skip_header tests
    success = asyncio.run(run_all_tests())

    # Exit with appropriate code
    exit(0 if success else 1)
