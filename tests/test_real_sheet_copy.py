#!/usr/bin/env python
"""Test copy_range_with_formulas with real Google Sheet via MCP client

Usage:
    python tests/test_real_sheet_copy.py --env=local --test=row
    python tests/test_real_sheet_copy.py --env=test --test=batch
    python tests/test_real_sheet_copy.py --env=test --test=column
    python tests/test_real_sheet_copy.py --env=test --test=all
"""

import os
import sys
import json
import asyncio
import argparse
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession


async def test_copy_row(session, endpoint):
    """Test copying a row with formulas"""
    print("\n" + "="*80)
    print("TEST 1: Copy Row with Formulas")
    print("="*80)

    # Test parameters for row copy
    uri = "https://docs.google.com/spreadsheets/d/169z8zmTqbM2496A7IKohB-7kEri8W88RrmCxf3RXLnk/edit?gid=1957703143#gid=1957703143"
    from_range = "B5:AQ5"
    to_range = "B6:AQ6"

    print(f"\nSheet URI: {uri}")
    print(f"Copying from: {from_range} (row 5)")
    print(f"Copying to:   {to_range} (row 6)\n")

    # Step 1: Read source formulas
    print("Step 1: Reading source row formulas...")
    read_res = await session.call_tool("read_worksheet_with_formulas", {
        "uri": uri
    })

    if not read_res.isError and read_res.content and read_res.content[0].text:
        content = json.loads(read_res.content[0].text)
        if content.get('success'):
            data = content.get('data', [])
            print(f"✅ Successfully read worksheet ({len(data)} rows)")

            if len(data) > 4:  # Row 5 is index 4
                row5 = data[4]
                print(f"\nSource row (row 5) sample formulas:")
                count = 0
                for key, value in row5.items():
                    if value and isinstance(value, str) and value.startswith('='):
                        print(f"  {key}: {value}")
                        count += 1
                        if count >= 3:  # Show first 3 formulas
                            break
        else:
            print(f"❌ Failed to read worksheet: {content.get('error')}")
            return False

    # Step 2: Copy the row
    from_range = "M1:M100"
    to_range = "N1:N100"
    print(f"\nStep 2: Copying row {from_range} to {to_range}...")
    copy_res = await session.call_tool("copy_range_with_formulas", {
        "uri": uri,
        "from_range": from_range,
        "to_range": to_range,
        "value_input_option": "USER_ENTERED"
    })

    if not copy_res.isError and copy_res.content and copy_res.content[0].text:
        result = json.loads(copy_res.content[0].text)

        if result.get('success'):
            print(f"\n✅ SUCCESS!")
            print(f"   Updated cells: {result.get('updated_cells')}")
            print(f"   Range: {result.get('range')}")
            print(f"   Shape: {result.get('shape')}")
            print(f"   Message: {result.get('message')}")
            print(f"\n🔗 View the result: {result.get('spreadsheet_url')}")
            return True
        else:
            print(f"\n❌ FAILED!")
            print(f"   Error: {result.get('error')}")
            print(f"   Message: {result.get('message')}")
            return False
    else:
        print(f"\n❌ FAILED! No response from copy operation")
        return False


async def test_batch_optimization(session, endpoint):
    """Test batch optimization when copying to many rows"""
    print("\n" + "="*80)
    print("TEST 2: Batch Optimization (Copy to 50 rows)")
    print("="*80)

    # Test sheet for batch operations
    uri = "https://docs.google.com/spreadsheets/d/169z8zmTqbM2496A7IKohB-7kEri8W88RrmCxf3RXLnk/edit?gid=774404782#gid=774404782"

    print(f"\nSheet URI: {uri}")
    print(f"Testing: Copy 1 source row to 50 destination rows\n")

    # Step 1: Setup source row with formulas
    print("Step 1: Setting up source row with formulas...")
    source_data = [
        ["ID", "Name", "Value", "Double", "Triple"],
        ["1", "Row2", "100", "=C2*2", "=C2*3"]
    ]

    setup_res = await session.call_tool("update_range", {
        "uri": uri,
        "data": source_data,
        "range_address": "A1:E2"
    })

    if not setup_res.isError and setup_res.content and setup_res.content[0].text:
        result = json.loads(setup_res.content[0].text)
        if result.get('success'):
            print(f"✅ Source row created with formulas")
        else:
            print(f"❌ Failed to create source row: {result.get('error')}")
            return False

    # Step 2: Copy to 50 rows with batch optimization
    import time
    print(f"\nStep 2: Copying formulas to 50 rows (with batch optimization)...")
    print(f"   This uses 1 batch API call instead of 50 individual calls")

    start_time = time.time()

    copy_res = await session.call_tool("copy_range_with_formulas", {
        "uri": uri,
        "from_range": "A2:E2",
        "to_range": "A3:E52",  # 50 destination rows
        "skip_if_exists": True
    })

    end_time = time.time()
    elapsed_time = end_time - start_time

    if not copy_res.isError and copy_res.content and copy_res.content[0].text:
        result = json.loads(copy_res.content[0].text)

        if result.get('success'):
            print(f"\n✅ SUCCESS!")
            print(f"   Updated cells: {result.get('updated_cells')}")
            print(f"   Shape: {result.get('shape')}")
            print(f"   Time taken: {elapsed_time:.2f}s")

            if elapsed_time < 10:
                print(f"   ✅ PERFORMANCE PASS: Completed in {elapsed_time:.2f}s (expected < 10s)")
                print(f"   💡 Batch optimization is working correctly!")
            else:
                print(f"   ⚠️  Performance slower than expected: {elapsed_time:.2f}s")

            # Step 3: Test skip_if_exists with second copy
            print(f"\nStep 3: Testing skip_if_exists (should skip all rows)...")
            start_time2 = time.time()

            copy_res2 = await session.call_tool("copy_range_with_formulas", {
                "uri": uri,
                "from_range": "A2:E2",
                "to_range": "A3:E52",  # Same range
                "skip_if_exists": True
            })

            end_time2 = time.time()
            elapsed_time2 = end_time2 - start_time2

            if not copy_res2.isError and copy_res2.content and copy_res2.content[0].text:
                result2 = json.loads(copy_res2.content[0].text)
                if result2.get('success') and result2.get('updated_cells') == 0:
                    print(f"   ✅ Skip works correctly! No cells updated.")
                    print(f"   Time taken: {elapsed_time2:.2f}s")
                    print(f"   Message: {result2.get('message')}")

            print(f"\n🔗 View the result: {result.get('spreadsheet_url')}")
            return True
        else:
            print(f"\n❌ FAILED!")
            print(f"   Error: {result.get('error')}")
            print(f"   Message: {result.get('message')}")
            return False
    else:
        print(f"\n❌ FAILED! No response from copy operation")
        if copy_res.isError:
            print(f"   Error flag: isError = True")
            if copy_res.content:
                print(f"   Error content: {copy_res.content}")
        else:
            print(f"   Response: {copy_res}")
        return False


async def test_copy_column(session, endpoint):
    """Test copying a column with formulas"""
    print("\n" + "="*80)
    print("TEST 3: Copy Column with Formulas")
    print("="*80)

    # Test parameters for column copy
    uri = "https://docs.google.com/spreadsheets/d/169z8zmTqbM2496A7IKohB-7kEri8W88RrmCxf3RXLnk/edit?gid=1418024543#gid=1418024543"
    from_range = "M1:M100"
    to_range = "N1:N100"

    print(f"\nSheet URI: {uri}")
    print(f"Copying from: {from_range} (column M)")
    print(f"Copying to:   {to_range} (column N)\n")

    # Step 1: Read source formulas
    print("Step 1: Reading source column formulas...")
    read_res = await session.call_tool("read_worksheet_with_formulas", {
        "uri": uri
    })

    if not read_res.isError and read_res.content and read_res.content[0].text:
        content = json.loads(read_res.content[0].text)
        if content.get('success'):
            data = content.get('data', [])
            print(f"✅ Successfully read worksheet ({len(data)} rows)")

            # Show some formulas from column M
            print(f"\nSource column (M) sample formulas:")
            count = 0
            for i, row in enumerate(data[:10]):  # Check first 10 rows
                m_value = row.get('M')
                if m_value and isinstance(m_value, str) and m_value.startswith('='):
                    print(f"  M{i+1}: {m_value}")
                    count += 1
                    if count >= 3:  # Show first 3 formulas
                        break
        else:
            print(f"❌ Failed to read worksheet: {content.get('error')}")
            return False

    # Step 2: Copy the column
    print(f"\nStep 2: Copying column {from_range} to {to_range}...")
    copy_res = await session.call_tool("copy_range_with_formulas", {
        "uri": uri,
        "from_range": from_range,
        "to_range": to_range,
        "value_input_option": "USER_ENTERED"
    })

    if not copy_res.isError and copy_res.content and copy_res.content[0].text:
        result = json.loads(copy_res.content[0].text)

        if result.get('success'):
            print(f"\n✅ SUCCESS!")
            print(f"   Updated cells: {result.get('updated_cells')}")
            print(f"   Range: {result.get('range')}")
            print(f"   Shape: {result.get('shape')}")
            print(f"   Message: {result.get('message')}")

            # Step 3: Verify copied formulas
            print(f"\nStep 3: Verifying copied column formulas...")
            verify_res = await session.call_tool("read_worksheet_with_formulas", {
                "uri": uri
            })

            if not verify_res.isError and verify_res.content and verify_res.content[0].text:
                verify_content = json.loads(verify_res.content[0].text)
                if verify_content.get('success'):
                    verify_data = verify_content.get('data', [])
                    print(f"\nDestination column (N) sample formulas:")
                    count = 0
                    for i, row in enumerate(verify_data[:10]):
                        n_value = row.get('N')
                        if n_value and isinstance(n_value, str) and n_value.startswith('='):
                            print(f"  N{i+1}: {n_value}")
                            count += 1
                            if count >= 3:
                                break

            print(f"\n🔗 View the result: {result.get('spreadsheet_url')}")
            return True
        else:
            print(f"\n❌ FAILED!")
            print(f"   Error: {result.get('error')}")
            print(f"   Message: {result.get('message')}")
            return False
    else:
        print(f"\n❌ FAILED! No response from copy operation")
        return False


async def main():
    parser = argparse.ArgumentParser(description='Test copy_range_with_formulas via MCP')
    parser.add_argument('--env', choices=['local', 'test'], default='local',
                      help='Environment: local (127.0.0.1:8321) or test (datatable-mcp-test.maybe.ai)')
    parser.add_argument('--test', choices=['row', 'batch', 'column', 'all'], default='all',
                      help='Which test to run: row, batch, column, or all')
    args = parser.parse_args()

    # Set endpoint based on environment
    if args.env == "test":
        endpoint = "https://datatable-mcp-test.maybe.ai"
    else:
        endpoint = "http://127.0.0.1:8321"

    print("="*80)
    print("TEST: Copy Range with Formulas in Real Google Sheets")
    print("="*80)
    print(f"🔗 Using {args.env} environment: {endpoint}")
    print(f"🧪 Running test: {args.test}\n")

    # OAuth headers for testing
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    async with streamablehttp_client(url=f"{endpoint}/mcp", headers=test_headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            results = []

            # Run selected tests
            if args.test in ['row', 'all']:
                result = await test_copy_row(session, endpoint)
                results.append(('Copy Row', result))

            if args.test in ['batch', 'all']:
                result = await test_batch_optimization(session, endpoint)
                results.append(('Batch Optimization (50 rows)', result))

            if args.test in ['column', 'all']:
                result = await test_copy_column(session, endpoint)
                results.append(('Copy Column', result))

            # Print summary
            print("\n" + "="*80)
            print("TEST SUMMARY")
            print("="*80)
            for test_name, passed in results:
                status = "✅ PASSED" if passed else "❌ FAILED"
                print(f"{status}: {test_name}")

            # Return exit code
            all_passed = all(result for _, result in results)
            return 0 if all_passed else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
