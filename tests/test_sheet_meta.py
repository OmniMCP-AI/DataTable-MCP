#!/usr/bin/env python3
"""
Google Sheets Metadata API Integration Tests

Tests for the three sheet metadata tools:
- get_last_row: Get the last row number with data (optionally in specific column)
- get_used_range: Get the minimal range containing all data
- get_last_column: Get the last column letter with data

Usage:
    # Run all tests
    python test_sheet_meta.py --env=local --test=all

    # Run specific test
    python test_sheet_meta.py --env=local --test=lastrow
    python test_sheet_meta.py --env=local --test=lastrow_column  # NEW: Test column-specific last row
    python test_sheet_meta.py --env=local --test=usedrange
    python test_sheet_meta.py --env=local --test=lastcolumn

    # Run against test environment
    python test_sheet_meta.py --env=test --test=all

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession

# Test configuration constants
TEST_USER_ID = "68501372a3569b6897673a48"

# Test sheet with known data (from test_mcp_client_calltool.py)
READ_ONLY_URI = "https://docs.google.com/spreadsheets/d/1DpaI7L4yfYptsv6X2TL0InhVbeFfe2TpZPPoY98llR0/edit?gid=1411021775#gid=1411021775"

# Test sheet for metadata operations (country-sales sheet)
RANGE_ADDRESS_TEST_URI = "https://docs.google.com/spreadsheets/d/18iaWb8OUFdNldk03ESY6indsfrURlMsyBwqwMIRkYJY/edit?gid=1435041919#gid=1435041919"


async def test_get_last_row(url, headers):
    """Test get_last_row tool"""
    print(f"🚀 Testing get_last_row Tool")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Get last row from known sheet
            print(f"\n📘 Test 1: Get last row from sheet with data")
            print(f"   URI: {RANGE_ADDRESS_TEST_URI}")

            result = await session.call_tool("get_last_row", {
                "uri": RANGE_ADDRESS_TEST_URI
            })

            print(f"   Result: {result}")

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)
                if content.get('success'):
                    row_number = content.get('row_number')
                    worksheet = content.get('worksheet')
                    spreadsheet_id = content.get('spreadsheet_id')
                    spreadsheet_url = content.get('spreadsheet_url')

                    print(f"   ✅ Success!")
                    print(f"      Last row: {row_number}")
                    print(f"      Worksheet: {worksheet}")
                    print(f"      Spreadsheet ID: {spreadsheet_id}")
                    print(f"      Spreadsheet URL: {spreadsheet_url}")

                    # Verify row_number is a positive integer
                    if isinstance(row_number, int) and row_number > 0:
                        print(f"   ✅ PASS: Last row is a positive integer: {row_number}")
                    else:
                        print(f"   ❌ FAIL: Expected positive integer, got {row_number}")

                    # Verify spreadsheet_url is present and valid
                    if spreadsheet_url and 'docs.google.com/spreadsheets' in spreadsheet_url:
                        print(f"   ✅ PASS: Spreadsheet URL is valid")
                    else:
                        print(f"   ❌ FAIL: Invalid or missing spreadsheet_url")
                else:
                    print(f"   ❌ FAIL: {content.get('message')}")
                    if content.get('error'):
                        print(f"      Error: {content.get('error')}")
            else:
                error_msg = result.content[0].text if result.content else "Unknown error"
                print(f"   ❌ FAIL: {error_msg}")

            # Test 2: Verify response structure
            print(f"\n📘 Test 2: Verify response structure")

            result = await session.call_tool("get_last_row", {
                "uri": READ_ONLY_URI
            })

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)

                required_fields = ['success', 'row_number', 'spreadsheet_id', 'spreadsheet_url', 'worksheet', 'message']
                missing_fields = [field for field in required_fields if field not in content]

                if not missing_fields:
                    print(f"   ✅ PASS: All required fields present")
                    print(f"      Fields: {list(content.keys())}")
                else:
                    print(f"   ❌ FAIL: Missing fields: {missing_fields}")

            print(f"\n✅ get_last_row test completed!")


async def test_get_last_row_with_column(url, headers):
    """Test get_last_row tool with column parameter"""
    print(f"🚀 Testing get_last_row with Column Parameter")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Get last row in specific column
            print(f"\n📘 Test 1: Get last row in column B")
            print(f"   URI: {RANGE_ADDRESS_TEST_URI}")
            print(f"   Column: B")
            print(f"   Expected: Last row in column B should be less than or equal to overall last row")

            # First get overall last row for comparison
            overall_result = await session.call_tool("get_last_row", {
                "uri": RANGE_ADDRESS_TEST_URI
            })

            overall_last_row = 0
            if not overall_result.isError and overall_result.content and overall_result.content[0].text:
                content = json.loads(overall_result.content[0].text)
                if content.get('success'):
                    overall_last_row = content.get('row_number')
                    print(f"   📊 Overall last row (all columns): {overall_last_row}")

            # Now get last row in column B
            result = await session.call_tool("get_last_row", {
                "uri": RANGE_ADDRESS_TEST_URI,
                "column": "B"
            })

            print(f"   Result: {result}")

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)
                if content.get('success'):
                    row_number = content.get('row_number')
                    message = content.get('message')
                    spreadsheet_url = content.get('spreadsheet_url')

                    print(f"   ✅ Success!")
                    print(f"      Last row in column B: {row_number}")
                    print(f"      Message: {message}")
                    print(f"      Spreadsheet URL: {spreadsheet_url}")

                    # Verify row_number is valid
                    if isinstance(row_number, int) and row_number >= 0:
                        print(f"   ✅ PASS: Row number is valid: {row_number}")
                    else:
                        print(f"   ❌ FAIL: Invalid row number: {row_number}")

                    # Verify column B's last row <= overall last row
                    if overall_last_row > 0 and row_number <= overall_last_row:
                        print(f"   ✅ PASS: Column B last row ({row_number}) <= overall last row ({overall_last_row})")
                    elif row_number == 0:
                        print(f"   ⚠️  Column B is empty")
                    else:
                        print(f"   ❌ FAIL: Column B last row ({row_number}) > overall last row ({overall_last_row})")

                    # Verify message includes column reference
                    if "column B" in message:
                        print(f"   ✅ PASS: Message mentions specific column")
                    else:
                        print(f"   ⚠️  WARNING: Message doesn't mention column B")
                else:
                    print(f"   ❌ FAIL: {content.get('message')}")
                    if content.get('error'):
                        print(f"      Error: {content.get('error')}")
            else:
                error_msg = result.content[0].text if result.content else "Unknown error"
                print(f"   ❌ FAIL: {error_msg}")

            # Test 2: Compare different columns
            print(f"\n📘 Test 2: Compare last rows across multiple columns")

            columns_to_test = ["A", "B", "C", "D"]
            column_results = {}

            for col in columns_to_test:
                result = await session.call_tool("get_last_row", {
                    "uri": RANGE_ADDRESS_TEST_URI,
                    "column": col
                })

                if not result.isError and result.content and result.content[0].text:
                    content = json.loads(result.content[0].text)
                    if content.get('success'):
                        column_results[col] = content.get('row_number')

            print(f"   📊 Last rows by column:")
            for col, last_row in column_results.items():
                print(f"      Column {col}: row {last_row}")

            if column_results:
                print(f"   ✅ PASS: Successfully retrieved last rows for multiple columns")
            else:
                print(f"   ❌ FAIL: Could not retrieve last rows")

            # Test 3: Test with non-existent/empty column
            print(f"\n📘 Test 3: Test with potentially empty column (ZZ)")

            result = await session.call_tool("get_last_row", {
                "uri": RANGE_ADDRESS_TEST_URI,
                "column": "ZZ"
            })

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)
                if content.get('success'):
                    row_number = content.get('row_number')
                    message = content.get('message')

                    print(f"   📊 Last row in column ZZ: {row_number}")
                    print(f"   📊 Message: {message}")

                    if row_number == 0:
                        print(f"   ✅ PASS: Empty column correctly returns 0")
                        if "No data found in column ZZ" in message:
                            print(f"   ✅ PASS: Appropriate message for empty column")
                    else:
                        print(f"   ⚠️  Column ZZ has data at row {row_number}")

            # Test 4: Test with two-letter column (AA)
            print(f"\n📘 Test 4: Test with two-letter column (AA)")

            result = await session.call_tool("get_last_row", {
                "uri": RANGE_ADDRESS_TEST_URI,
                "column": "AA"
            })

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)
                if content.get('success'):
                    row_number = content.get('row_number')
                    print(f"   📊 Last row in column AA: {row_number}")
                    print(f"   ✅ PASS: Two-letter column handled correctly")
                else:
                    print(f"   ❌ FAIL: {content.get('message')}")

            print(f"\n✅ get_last_row with column parameter test completed!")


async def test_get_used_range(url, headers):
    """Test get_used_range tool"""
    print(f"🚀 Testing get_used_range Tool")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Get used range from known sheet
            print(f"\n📘 Test 1: Get used range from sheet with data")
            print(f"   URI: {RANGE_ADDRESS_TEST_URI}")

            result = await session.call_tool("get_used_range", {
                "uri": RANGE_ADDRESS_TEST_URI
            })

            print(f"   Result: {result}")

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)
                if content.get('success'):
                    used_range = content.get('used_range')
                    row_count = content.get('row_count')
                    column_count = content.get('column_count')
                    start_cell = content.get('start_cell')
                    end_cell = content.get('end_cell')
                    worksheet = content.get('worksheet')
                    spreadsheet_url = content.get('spreadsheet_url')

                    print(f"   ✅ Success!")
                    print(f"      Used range: {used_range}")
                    print(f"      Dimensions: {row_count} rows × {column_count} columns")
                    print(f"      Start cell: {start_cell}")
                    print(f"      End cell: {end_cell}")
                    print(f"      Worksheet: {worksheet}")
                    print(f"      Spreadsheet URL: {spreadsheet_url}")

                    # Verify used_range format (A1:XX99)
                    if ':' in used_range and row_count > 0 and column_count > 0:
                        print(f"   ✅ PASS: Valid used range format")
                    else:
                        print(f"   ❌ FAIL: Invalid range format or dimensions")

                    # Verify start_cell is A1
                    if start_cell == "A1":
                        print(f"   ✅ PASS: Start cell is A1 (expected)")
                    else:
                        print(f"   ⚠️  WARNING: Start cell is {start_cell}, expected A1")

                    # Verify spreadsheet_url is present and valid
                    if spreadsheet_url and 'docs.google.com/spreadsheets' in spreadsheet_url:
                        print(f"   ✅ PASS: Spreadsheet URL is valid")
                    else:
                        print(f"   ❌ FAIL: Invalid or missing spreadsheet_url")
                else:
                    print(f"   ❌ FAIL: {content.get('message')}")
                    if content.get('error'):
                        print(f"      Error: {content.get('error')}")
            else:
                error_msg = result.content[0].text if result.content else "Unknown error"
                print(f"   ❌ FAIL: {error_msg}")

            # Test 2: Verify response structure
            print(f"\n📘 Test 2: Verify response structure")

            result = await session.call_tool("get_used_range", {
                "uri": READ_ONLY_URI
            })

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)

                required_fields = [
                    'success', 'used_range', 'row_count', 'column_count',
                    'start_cell', 'end_cell', 'spreadsheet_id', 'spreadsheet_url', 'worksheet', 'message'
                ]
                missing_fields = [field for field in required_fields if field not in content]

                if not missing_fields:
                    print(f"   ✅ PASS: All required fields present")
                    print(f"      Fields: {list(content.keys())}")
                else:
                    print(f"   ❌ FAIL: Missing fields: {missing_fields}")

            # Test 3: Combine with read_sheet to verify range
            print(f"\n📘 Test 3: Combine get_used_range with read_sheet")

            # First get used range
            range_result = await session.call_tool("get_used_range", {
                "uri": RANGE_ADDRESS_TEST_URI
            })

            if not range_result.isError and range_result.content and range_result.content[0].text:
                range_content = json.loads(range_result.content[0].text)
                if range_content.get('success'):
                    used_range = range_content.get('used_range')

                    # Now read the sheet using that range
                    read_result = await session.call_tool("read_sheet", {
                        "uri": RANGE_ADDRESS_TEST_URI,
                        "range_address": used_range
                    })

                    if not read_result.isError and read_result.content and read_result.content[0].text:
                        read_content = json.loads(read_result.content[0].text)
                        if read_content.get('success'):
                            data = read_content.get('data', [])
                            print(f"   ✅ PASS: Successfully read data using detected range")
                            print(f"      Used range: {used_range}")
                            print(f"      Data rows: {len(data)}")
                        else:
                            print(f"   ❌ FAIL: Could not read data using range: {used_range}")
                    else:
                        print(f"   ❌ FAIL: read_sheet call failed")

            print(f"\n✅ get_used_range test completed!")


async def test_get_last_column(url, headers):
    """Test get_last_column tool"""
    print(f"🚀 Testing get_last_column Tool")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Get last column from known sheet
            print(f"\n📘 Test 1: Get last column from sheet with data")
            print(f"   URI: {RANGE_ADDRESS_TEST_URI}")

            result = await session.call_tool("get_last_column", {
                "uri": RANGE_ADDRESS_TEST_URI
            })

            print(f"   Result: {result}")

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)
                if content.get('success'):
                    column = content.get('column')
                    column_index = content.get('column_index')
                    worksheet = content.get('worksheet')
                    spreadsheet_id = content.get('spreadsheet_id')
                    spreadsheet_url = content.get('spreadsheet_url')

                    print(f"   ✅ Success!")
                    print(f"      Last column: {column} (index {column_index})")
                    print(f"      Worksheet: {worksheet}")
                    print(f"      Spreadsheet ID: {spreadsheet_id}")
                    print(f"      Spreadsheet URL: {spreadsheet_url}")

                    # Verify column is a valid letter and column_index is non-negative
                    if isinstance(column, str) and column.isalpha() and isinstance(column_index, int) and column_index >= 0:
                        print(f"   ✅ PASS: Valid column letter and index")
                    else:
                        print(f"   ❌ FAIL: Invalid column format")

                    # Verify spreadsheet_url is present and valid
                    if spreadsheet_url and 'docs.google.com/spreadsheets' in spreadsheet_url:
                        print(f"   ✅ PASS: Spreadsheet URL is valid")
                    else:
                        print(f"   ❌ FAIL: Invalid or missing spreadsheet_url")
                else:
                    print(f"   ❌ FAIL: {content.get('message')}")
                    if content.get('error'):
                        print(f"      Error: {content.get('error')}")
            else:
                error_msg = result.content[0].text if result.content else "Unknown error"
                print(f"   ❌ FAIL: {error_msg}")

            # Test 2: Verify response structure
            print(f"\n📘 Test 2: Verify response structure")

            result = await session.call_tool("get_last_column", {
                "uri": READ_ONLY_URI
            })

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)

                required_fields = ['success', 'column', 'column_index', 'spreadsheet_id', 'spreadsheet_url', 'worksheet', 'message']
                missing_fields = [field for field in required_fields if field not in content]

                if not missing_fields:
                    print(f"   ✅ PASS: All required fields present")
                    print(f"      Fields: {list(content.keys())}")
                else:
                    print(f"   ❌ FAIL: Missing fields: {missing_fields}")

            # Test 3: Verify column_index consistency
            print(f"\n📘 Test 3: Verify column letter to index consistency")

            result = await session.call_tool("get_last_column", {
                "uri": RANGE_ADDRESS_TEST_URI
            })

            if not result.isError and result.content and result.content[0].text:
                content = json.loads(result.content[0].text)
                if content.get('success'):
                    column = content.get('column')
                    column_index = content.get('column_index')

                    # Manual conversion to verify
                    # A=0, B=1, ..., Z=25, AA=26, etc.
                    expected_index = 0
                    for i, char in enumerate(reversed(column)):
                        expected_index += (ord(char) - ord('A') + 1) * (26 ** i)
                    expected_index -= 1  # Convert to 0-based

                    if column_index == expected_index:
                        print(f"   ✅ PASS: Column index matches letter")
                        print(f"      {column} = index {column_index}")
                    else:
                        print(f"   ❌ FAIL: Column index mismatch")
                        print(f"      Expected: {expected_index}, Got: {column_index}")

            print(f"\n✅ get_last_column test completed!")


async def run_all_tests(url, headers):
    """Run all metadata tests in sequence"""
    print("🎯 Starting Google Sheets Metadata API Tests")
    print("=" * 80)

    results = {}

    try:
        # Test get_last_row
        print(f"\n{'='*20} GET_LAST_ROW TEST {'='*20}")
        await test_get_last_row(url, headers)
        results['get_last_row'] = {'status': 'passed'}

        # Test get_last_row with column parameter
        print(f"\n{'='*20} GET_LAST_ROW WITH COLUMN TEST {'='*20}")
        await test_get_last_row_with_column(url, headers)
        results['get_last_row_with_column'] = {'status': 'passed'}

        # Test get_used_range
        print(f"\n{'='*20} GET_USED_RANGE TEST {'='*20}")
        await test_get_used_range(url, headers)
        results['get_used_range'] = {'status': 'passed'}

        # Test get_last_column
        print(f"\n{'='*20} GET_LAST_COLUMN TEST {'='*20}")
        await test_get_last_column(url, headers)
        results['get_last_column'] = {'status': 'passed'}

        # Summary
        print(f"\n{'='*80}")
        print("🎉 ALL METADATA TESTS COMPLETED!")
        print(f"{'='*80}")

        for test_name, result in results.items():
            print(f"✅ {test_name.replace('_', ' ').title()}: {result['status']}")

        return results

    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return None


async def run_single_test(test_name, url, headers):
    """Run a single test by name"""
    test_functions = {
        'lastrow': test_get_last_row,
        'lastrow_column': test_get_last_row_with_column,
        'usedrange': test_get_used_range,
        'lastcolumn': test_get_last_column,
    }

    if test_name not in test_functions:
        print(f"❌ Unknown test: {test_name}")
        print(f"Available tests: {', '.join(test_functions.keys())}")
        return

    print(f"🎯 Running single test: {test_name}")
    print("=" * 60)

    try:
        result = await test_functions[test_name](url, headers)
        print(f"\n✅ Test '{test_name}' completed successfully!")
        return result
    except Exception as e:
        print(f"\n❌ Test '{test_name}' failed with error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    import os
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test Google Sheets Metadata APIs")
    parser.add_argument("--env", choices=["local", "prod", "test"], default="local",
                       help="Environment to use: local (127.0.0.1:8321) or test (datatable-mcp-test.maybe.ai) or prod (datatable-mcp.maybe.ai)")
    parser.add_argument("--test", choices=["all", "lastrow", "lastrow_column", "usedrange", "lastcolumn"], default="all",
                       help="Which test to run: all (default), lastrow, lastrow_column, usedrange, or lastcolumn")
    args = parser.parse_args()

    # Set endpoint based on environment argument
    if args.env == "test":
        endpoint = "https://datatable-mcp-test.maybe.ai"
    elif args.env == "prod":
        endpoint = "https://datatable-mcp.maybe.ai"
    else:
        endpoint = "http://127.0.0.1:8321"

    print(f"🔗 Using {args.env} environment: {endpoint}")
    print(f"💡 Use --env=local for local development or --env=prod for production")
    print(f"🧪 Running test: {args.test}")

    # OAuth headers for testing
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    # Run the selected test(s)
    if args.test == "all":
        asyncio.run(run_all_tests(url=f"{endpoint}/mcp", headers=test_headers))
    else:
        asyncio.run(run_single_test(args.test, url=f"{endpoint}/mcp", headers=test_headers))
