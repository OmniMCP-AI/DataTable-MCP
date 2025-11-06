#!/usr/bin/env python3
"""
Standalone GoogleSheetDataTable DataFrame Tests

Tests that GoogleSheetDataTable methods accept Polars DataFrames.
Demonstrates Stage 5.1.2 DataFrame support enhancement.

Usage:
    python tests/standard_datatable/test_dataframe_support.py

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

import asyncio
import os
from datetime import datetime
import polars as pl

from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable
from datatable_tools.auth.service_factory import create_google_service_from_env


# Test URIs write for dataframe test
READ_WRITE_URI = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=1210260183#gid=1210260183"


def create_google_service():
    """Create Google Sheets service using OAuth credentials"""
    return create_google_service_from_env(env_prefix="TEST_GOOGLE_OAUTH")


async def test_update_range_with_dataframe():
    """Test update_range accepts Polars DataFrame"""
    print("\n" + "="*60)
    print("Test 1: update_range with Polars DataFrame")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create Polars DataFrame
    df = pl.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30],
        'status': ['Active', 'Pending'],
        'timestamp': [timestamp, timestamp]
    })

    print(f"   📊 Input DataFrame:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(df)

    # Call update_range with DataFrame
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=df,  # Pass DataFrame directly
        range_address='A10'  # Will auto-expand based on DataFrame size
    )

    # Verify result (Pydantic model)
    if result.success:
        print(f"✅ Successfully updated range with DataFrame")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape matches DataFrame
        expected_shape = f"({df.height + 1},{df.width})"  # +1 for headers
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches: {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to update range: {result.error}")
        return False


async def test_append_rows_with_dataframe():
    """Test append_rows accepts Polars DataFrame"""
    print("\n" + "="*60)
    print("Test 2: append_rows with Polars DataFrame")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create Polars DataFrame
    df = pl.DataFrame({
        'product': ['Widget', 'Gadget', 'Device'],
        'price': [99.99, 149.99, 199.99],
        'stock': [10, 5, 15],
        'updated': [timestamp, timestamp, timestamp]
    })

    print(f"   📊 Input DataFrame:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(df)

    # Call append_rows with DataFrame
    result = await google_sheet.append_rows(
        service=service,
        uri=READ_WRITE_URI,
        data=df  # Pass DataFrame directly
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully appended rows from DataFrame")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape matches DataFrame
        expected_shape = f"({df.height},{df.width})"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches: {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to append rows: {result.error}")
        return False


async def test_append_columns_with_dataframe():
    """Test append_columns accepts Polars DataFrame - creates new sheet to avoid grid limits"""
    print("\n" + "="*60)
    print("Test 3: append_columns with Polars DataFrame")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # First create a fresh sheet with initial data
    initial_df = pl.DataFrame({
        'product': ['Widget', 'Gadget', 'Device'],
        'price': [99.99, 149.99, 199.99]
    })

    print(f"   📊 Creating new sheet with initial data:")
    print(f"      Shape: {initial_df.shape}")
    print(initial_df)

    # Create new sheet
    new_sheet_result = await google_sheet.write_new_sheet(
        service=service,
        data=initial_df,
        sheet_name=f"DataFrame Append Columns Test {timestamp}"
    )

    if not new_sheet_result.success:
        print(f"   ❌ Failed to create test sheet: {new_sheet_result.error}")
        return False

    test_uri = new_sheet_result.spreadsheet_url
    print(f"   ✅ Test sheet created: {test_uri}")

    # Now create DataFrame to append as columns
    df = pl.DataFrame({
        'rating': [4.5, 5.0, 4.0],
        'review_count': [100, 250, 75],
        'last_reviewed': [timestamp, timestamp, timestamp]
    })

    print(f"\n   📊 DataFrame to append as columns:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(df)

    # Call append_columns with DataFrame on the fresh sheet
    result = await google_sheet.append_columns(
        service=service,
        uri=test_uri,  # Use the new sheet URI
        data=df  # Pass DataFrame directly
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully appended columns from DataFrame")
        print(f"   Worksheet: {result.worksheet}")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        print(f"   Updated cells: {result.updated_cells}")

        # Verify shape matches DataFrame (+1 for headers)
        expected_shape = f"({df.height + 1},{df.width})"
        actual_shape = result.shape
        if actual_shape == expected_shape:
            print(f"   ✅ Shape matches: {actual_shape}")
            return True
        else:
            print(f"   ❌ Shape mismatch: expected {expected_shape}, got {actual_shape}")
            return False
    else:
        print(f"❌ Failed to append columns: {result.error}")
        return False


async def test_write_new_sheet_with_dataframe():
    """Test write_new_sheet accepts Polars DataFrame"""
    print("\n" + "="*60)
    print("Test 4: write_new_sheet with Polars DataFrame")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create Polars DataFrame with various data types
    df = pl.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'department': ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'],
        'salary': [120000.50, 85000.75, 75000.00, 90000.25, 95000.00],
        'active': [True, True, False, True, True],
        'hire_date': ['2020-01-15', '2021-03-22', '2019-07-01', '2022-05-10', '2023-02-14']
    })

    print(f"   📊 Input DataFrame:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(f"      Dtypes: {df.dtypes}")
    print(df)

    # Call write_new_sheet with DataFrame
    result = await google_sheet.write_new_sheet(
        service=service,
        data=df,  # Pass DataFrame directly
        sheet_name=f"DataFrame Test {timestamp}"
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully created new sheet from DataFrame")
        print(f"   Spreadsheet URL: {result.spreadsheet_url}")
        print(f"   Rows created: {result.rows_created}")
        print(f"   Columns created: {result.columns_created}")
        print(f"   Shape: {result.shape}")

        # Verify dimensions match DataFrame
        expected_rows = df.height
        expected_cols = df.width
        actual_rows = result.rows_created
        actual_cols = result.columns_created

        if actual_rows == expected_rows and actual_cols == expected_cols:
            print(f"   ✅ Dimensions match: {actual_rows}x{actual_cols}")
            return True
        else:
            print(f"   ❌ Dimension mismatch:")
            print(f"      Expected: {expected_rows}x{expected_cols}")
            print(f"      Got: {actual_rows}x{actual_cols}")
            return False
    else:
        print(f"❌ Failed to create sheet: {result.error}")
        return False


async def test_dataframe_with_none_values():
    """Test DataFrame handling with None/null values"""
    print("\n" + "="*60)
    print("Test 5: DataFrame with None/null values")
    print("="*60)

    service = create_google_service()
    google_sheet = GoogleSheetDataTable()

    # Create DataFrame with None values
    df = pl.DataFrame({
        'col1': ['A', None, 'C'],
        'col2': [1, 2, None],
        'col3': [None, 'X', 'Y']
    })

    print(f"   📊 Input DataFrame with None values:")
    print(df)

    # Call update_range with DataFrame containing None
    result = await google_sheet.update_range(
        service=service,
        uri=READ_WRITE_URI,
        data=df,
        range_address='M1'
    )

    # Verify result
    if result.success:
        print(f"✅ Successfully handled DataFrame with None values")
        print(f"   Range: {result.range}")
        print(f"   Shape: {result.shape}")
        return True
    else:
        print(f"❌ Failed to handle None values: {result.error}")
        return False


async def run_all_tests():
    """Run all DataFrame support tests"""
    print("\n" + "="*60)
    print("🚀 GoogleSheetDataTable Polars DataFrame Support Tests")
    print("="*60)
    print("\nStage 5.1.2: Testing Polars DataFrame support in all methods\n")

    results = []

    try:
        # Test 1: update_range
        result1 = await test_update_range_with_dataframe()
        results.append(("update_range with DataFrame", result1))

        # Test 2: append_rows
        result2 = await test_append_rows_with_dataframe()
        results.append(("append_rows with DataFrame", result2))

        # Test 3: append_columns
        result3 = await test_append_columns_with_dataframe()
        results.append(("append_columns with DataFrame", result3))

        # Test 4: write_new_sheet
        result4 = await test_write_new_sheet_with_dataframe()
        results.append(("write_new_sheet with DataFrame", result4))

        # Test 5: None values
        result5 = await test_dataframe_with_none_values()
        results.append(("DataFrame with None values", result5))

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
        print("\n🎉 All DataFrame support tests passed!")
        print("\n✅ GoogleSheetDataTable works with Polars DataFrames")
        print("✅ Stage 5.1.2 DataFrame support successful")
    else:
        print("\n⚠️  Some tests failed")

    return all_passed


if __name__ == "__main__":
    # Run the DataFrame support tests
    success = asyncio.run(run_all_tests())

    # Exit with appropriate code
    exit(0 if success else 1)
