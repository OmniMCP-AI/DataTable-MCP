#!/usr/bin/env python3
"""
Test suite for update_range_by_lookup with empty sheet

This tests two scenarios:
1. Sheet with only headers (no data rows) - should append data
2. Completely empty sheet (no headers, no data) - should write headers + data

Usage:
    python test_update_by_lookup_empty_sheet.py --env=local
    python test_update_by_lookup_empty_sheet.py --env=test
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
import argparse
import os
from mcp import ClientSession

# Test configuration
TEST_USER_ID = "68501372a3569b6897673a48"

# Use existing test spreadsheet instead of creating new one each time
TEST_SPREADSHEET_URI = "https://docs.google.com/spreadsheets/d/1MQg6qAbDCt-B1tJgA05VjDBH-a4ytkyJopWHlDy8rms/edit?gid=1391701892#gid=1391701892"
TEST_WORKSHEET_NAME = "test_empty_lookup"


async def test_empty_sheet_append(url, headers):
    """Test update_by_lookup on a sheet with only headers (no data rows)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test: Update by Lookup on Empty Sheet (Only Headers)")
    print(f"{'='*60}")
    print(f"Purpose: Verify that update_by_lookup appends data when sheet is empty")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: Create/reset test worksheet with only headers
            print(f"\n📝 Step 1: Creating/resetting worksheet '{TEST_WORKSHEET_NAME}' with only headers...")
            print(f"   Using spreadsheet: {TEST_SPREADSHEET_URI}")

            # Create or overwrite the test worksheet with only headers
            create_res = await session.call_tool("write_new_worksheet", {
                "uri": TEST_SPREADSHEET_URI,
                "data": [["外部单号", "收货单号", "单据类型"]],
                "worksheet_name": TEST_WORKSHEET_NAME
            })

            if create_res.isError:
                print(f"❌ Failed to create/reset test worksheet: {create_res.content[0].text if create_res.content else 'Unknown error'}")
                return

            create_content = json.loads(create_res.content[0].text)
            if not create_content.get('success'):
                print(f"❌ Create/reset failed: {create_content.get('message')}")
                return

            test_uri = create_content.get('spreadsheet_url')
            print(f"✅ Created/reset worksheet: {test_uri}")
            print(f"   Sheet has only headers, no data rows")

            # Step 2: Load the sheet to verify it's empty
            print("\n📖 Step 2: Verifying sheet is empty (only headers)...")
            load_res = await session.call_tool("load_data_table", {"uri": test_uri})

            if load_res.isError:
                print(f"❌ Failed to load: {load_res.content[0].text if load_res.content else 'Unknown error'}")
                return

            load_content = json.loads(load_res.content[0].text)
            if not load_content.get('success'):
                print(f"❌ Load failed: {load_content.get('message')}")
                return

            original_data = load_content.get('data', [])
            print(f"✅ Sheet loaded")
            print(f"   Row count (from source_info): {load_content.get('source_info', {}).get('row_count', 0)}")
            print(f"   Data rows: {len(original_data)}")
            print(f"   Expected: 0 data rows (only headers)")

            # Step 3: Attempt update_by_lookup with data
            print("\n🔄 Step 3: Calling update_range_by_lookup with test data...")
            update_data = [
                {
                    "外部单号": "A25103032",
                    "收货单号": "A118742731",
                    "单据类型": "退货"
                },
                {
                    "外部单号": "A25102431",
                    "收货单号": "A118417195",
                    "单据类型": "退货"
                },
                {
                    "外部单号": "A25101931",
                    "收货单号": "A118120533",
                    "单据类型": "退货"
                }
            ]

            print(f"   Updating with {len(update_data)} rows")
            print(f"   Lookup column: 外部单号")

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": test_uri,
                "data": update_data,
                "on": "外部单号",
                "override": False
            })

            if lookup_res.isError:
                print(f"❌ Update failed: {lookup_res.content[0].text if lookup_res.content else 'Unknown error'}")
                return

            result = json.loads(lookup_res.content[0].text)
            print(f"\n✅ Update result:")
            print(f"   Success: {result.get('success')}")
            print(f"   Message: {result.get('message')}")
            print(f"   Updated cells: {result.get('updated_cells')}")
            print(f"   Shape: {result.get('shape')}")

            # Step 4: Verify the data was appended
            print("\n🔍 Step 4: Verifying data was appended correctly...")
            verify_res = await session.call_tool("load_data_table", {"uri": test_uri})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                updated_data = verify_content.get('data', [])
                print(f"✅ Loaded {len(updated_data)} rows after update")

                if len(updated_data) == len(update_data):
                    print(f"   ✅ PASS: Correct number of rows appended ({len(updated_data)})")
                else:
                    print(f"   ❌ FAIL: Expected {len(update_data)} rows, got {len(updated_data)}")

                # Verify data content
                print(f"\n   Verifying row content:")
                for i, row in enumerate(updated_data):
                    expected = update_data[i]
                    match = all(row.get(k) == v for k, v in expected.items())
                    status = "✅" if match else "❌"
                    print(f"      {status} Row {i+1}: {row.get('外部单号')} | {row.get('收货单号')} | {row.get('单据类型')}")

                if all(
                    all(row.get(k) == update_data[i].get(k) for k in update_data[i].keys())
                    for i, row in enumerate(updated_data)
                ):
                    print(f"\n   ✅ PASS: All data matches expected values")
                else:
                    print(f"\n   ❌ FAIL: Some data doesn't match")

            print(f"\n{'='*60}")
            print(f"✅ Test 1 completed successfully!")
            print(f"   Sheet with only headers: update_by_lookup appends data correctly")
            print(f"{'='*60}")


async def test_completely_empty_sheet(url, headers):
    """Test update_by_lookup on a completely empty sheet (no headers, no data)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 2: Update by Lookup on Completely Empty Sheet")
    print(f"{'='*60}")
    print(f"Purpose: Verify that update_by_lookup writes headers + data when sheet is completely empty")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: Create a completely empty worksheet
            print(f"\n📝 Step 1: Creating completely empty worksheet...")
            print(f"   Using spreadsheet: {TEST_SPREADSHEET_URI}")

            # Create worksheet with no data at all
            create_res = await session.call_tool("write_new_worksheet", {
                "uri": TEST_SPREADSHEET_URI,
                "data": [[]],  # Empty data
                "worksheet_name": f"{TEST_WORKSHEET_NAME}_empty"
            })

            if create_res.isError:
                print(f"❌ Failed to create empty worksheet: {create_res.content[0].text if create_res.content else 'Unknown error'}")
                return

            create_content = json.loads(create_res.content[0].text)
            if not create_content.get('success'):
                print(f"❌ Create failed: {create_content.get('message')}")
                return

            test_uri = create_content.get('spreadsheet_url')
            print(f"✅ Created empty worksheet: {test_uri}")

            # Step 2: Verify sheet is completely empty
            print("\n📖 Step 2: Verifying sheet is completely empty...")
            load_res = await session.call_tool("load_data_table", {"uri": test_uri})

            if load_res.isError:
                print(f"❌ Failed to load: {load_res.content[0].text if load_res.content else 'Unknown error'}")
                return

            load_content = json.loads(load_res.content[0].text)
            if not load_content.get('success'):
                print(f"❌ Load failed: {load_content.get('message')}")
                return

            original_data = load_content.get('data', [])
            row_count = load_content.get('source_info', {}).get('row_count', 0)
            print(f"✅ Sheet loaded")
            print(f"   Row count: {row_count}")
            print(f"   Data rows: {len(original_data)}")
            print(f"   Expected: 0 rows (completely empty)")

            # Step 3: Call update_by_lookup with data
            print("\n🔄 Step 3: Calling update_range_by_lookup on completely empty sheet...")
            update_data = [
                {
                    "外部单号": "A25103032",
                    "收货单号": "A118742731",
                    "单据类型": "退货"
                },
                {
                    "外部单号": "A25102431",
                    "收货单号": "A118417195",
                    "单据类型": "退货"
                }
            ]

            print(f"   Updating with {len(update_data)} rows")
            print(f"   Expected: Headers + data should be written")

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": test_uri,
                "data": update_data,
                "on": "外部单号",
                "override": False
            })

            if lookup_res.isError:
                print(f"❌ Update failed: {lookup_res.content[0].text if lookup_res.content else 'Unknown error'}")
                return

            result = json.loads(lookup_res.content[0].text)
            print(f"\n✅ Update result:")
            print(f"   Success: {result.get('success')}")
            print(f"   Message: {result.get('message')}")
            print(f"   Updated cells: {result.get('updated_cells')}")
            print(f"   Shape: {result.get('shape')}")

            # Step 4: Verify headers + data were written
            print("\n🔍 Step 4: Verifying headers + data were written...")
            verify_res = await session.call_tool("load_data_table", {"uri": test_uri})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                updated_data = verify_content.get('data', [])
                print(f"✅ Loaded {len(updated_data)} rows after update")

                if len(updated_data) == len(update_data):
                    print(f"   ✅ PASS: Correct number of rows ({len(updated_data)})")
                else:
                    print(f"   ❌ FAIL: Expected {len(update_data)} rows, got {len(updated_data)}")

                # Verify headers exist
                if updated_data:
                    headers_found = list(updated_data[0].keys())
                    expected_headers = list(update_data[0].keys())
                    if headers_found == expected_headers:
                        print(f"   ✅ PASS: Headers written correctly: {headers_found}")
                    else:
                        print(f"   ❌ FAIL: Headers don't match")
                        print(f"      Expected: {expected_headers}")
                        print(f"      Found: {headers_found}")

                    # Verify data content
                    print(f"\n   Verifying row content:")
                    for i, row in enumerate(updated_data):
                        expected = update_data[i]
                        match = all(row.get(k) == v for k, v in expected.items())
                        status = "✅" if match else "❌"
                        print(f"      {status} Row {i+1}: {row.get('外部单号')} | {row.get('收货单号')} | {row.get('单据类型')}")

            print(f"\n{'='*60}")
            print(f"✅ Test 2 completed successfully!")
            print(f"   Completely empty sheet: update_by_lookup writes headers + data correctly")
            print(f"{'='*60}")


async def run_all_tests(url, headers):
    """Run all tests"""
    print(f"\n{'#'*60}")
    print(f"# Running All Empty Sheet Tests")
    print(f"{'#'*60}")

    await test_empty_sheet_append(url, headers)
    await test_completely_empty_sheet(url, headers)

    print(f"\n{'#'*60}")
    print(f"# ✅ All Tests Completed!")
    print(f"#    Test worksheet '{TEST_WORKSHEET_NAME}' can be reused")
    print(f"{'#'*60}")


def main():
    parser = argparse.ArgumentParser(description='Test update_by_lookup on empty sheet')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against')
    parser.add_argument('--test', choices=['all', 'headers_only', 'completely_empty'],
                       default='all', help='Which test to run')

    args = parser.parse_args()

    # Get environment variables
    refresh_token = os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
    client_id = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")

    if not all([refresh_token, client_id, client_secret]):
        print("❌ Missing required environment variables:")
        print("   TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
        print("   TEST_GOOGLE_OAUTH_CLIENT_ID")
        print("   TEST_GOOGLE_OAUTH_CLIENT_SECRET")
        return

    # Determine URL based on environment
    if args.env == "test":
        url = "https://datatable-mcp-test.maybe.ai/mcp"
    elif args.env == "prod":
        url = "https://datatable-mcp.maybe.ai/mcp"
    else:
        url = "http://127.0.0.1:8321/mcp"

    print(f"\n🔧 Test Configuration:")
    print(f"   Environment: {args.env}")
    print(f"   Endpoint: {url}")
    print(f"   User ID: {TEST_USER_ID}")
    print(f"   Test Spreadsheet: {TEST_SPREADSHEET_URI}")
    print(f"   Test Worksheet: {TEST_WORKSHEET_NAME}")

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": refresh_token,
        "GOOGLE_OAUTH_CLIENT_ID": client_id,
        "GOOGLE_OAUTH_CLIENT_SECRET": client_secret,
    }

    # Run selected test
    test_map = {
        'all': run_all_tests,
        'headers_only': test_empty_sheet_append,
        'completely_empty': test_completely_empty_sheet,
    }

    asyncio.run(test_map[args.test](url, headers))


if __name__ == "__main__":
    main()
