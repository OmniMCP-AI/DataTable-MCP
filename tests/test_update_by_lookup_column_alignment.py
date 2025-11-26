#!/usr/bin/env python3
"""
Test update_range_by_lookup column alignment when sheet has only headers

This test reproduces the bug where:
1. Sheet has headers but no data rows
2. update_range_by_lookup falls back to append_rows
3. Data is appended but columns don't align with headers

Expected behavior:
- Data should be aligned with existing headers
- Column order from incoming data should match header order
- Missing columns should be filled with empty strings
- Extra columns in data should be ignored or raise error

Usage:
    python test_update_by_lookup_column_alignment.py --env=local
    python test_update_by_lookup_column_alignment.py --env=test
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession
from datetime import datetime
import os
import argparse

# Test configuration
TEST_USER_ID = "68501372a3569b6897673a48"

# We'll create a new sheet for each test run to ensure clean state
# Using gid that doesn't exist will force the API to use Sheet1
TEST_SHEET_URI_BASE = "https://docs.google.com/spreadsheets/d/1h6waNEyrv_LKbxGSyZCJLf-QmLgFRNIQM4PfTphIeDM/edit"

async def test_column_alignment(url, headers):
    """Test that update_range_by_lookup correctly aligns columns when appending to header-only sheets"""
    print(f"🚀 Testing Column Alignment for Header-Only Sheets")
    print("=" * 80)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 0: Create a new worksheet for this test run
            print(f"\n📝 Test 0: Creating new worksheet for clean test")

            test_sheet_name = f"ColumnAlignmentTest_{timestamp}"
            headers_only = [["外部单号", "产品名称", "数量", "单价", "总价", "备注"]]

            # Create new sheet with headers only
            create_res = await session.call_tool("write_new_sheet", {
                "data": headers_only,
                "sheet_name": test_sheet_name
            })

            if create_res.isError:
                print(f"   ❌ Failed to create test sheet")
                return

            if create_res.content and create_res.content[0].text:
                result = json.loads(create_res.content[0].text)
                if result.get('success'):
                    test_sheet_uri = result.get('spreadsheet_url')
                    print(f"   ✅ Created test sheet: {test_sheet_name}")
                    print(f"   📄 URI: {test_sheet_uri}")
                else:
                    print(f"   ❌ Failed to create sheet")
                    return
            else:
                print(f"   ❌ No response from create_sheet")
                return

            # Test 1: Verify sheet has only headers (no data)
            print(f"\n📝 Test 1: Verifying sheet has only headers")

            read_res = await session.call_tool("read_sheet", {
                "uri": test_sheet_uri
            })

            if not read_res.isError and read_res.content and read_res.content[0].text:
                content = json.loads(read_res.content[0].text)
                if content.get('success'):
                    data = content.get('data', [])
                    print(f"   ✅ Sheet has {len(data)} data rows (should be 0 for header-only)")

                    if len(data) != 0:
                        print(f"   ⚠️  WARNING: Expected 0 data rows, got {len(data)}")

            # Test 2: Call update_range_by_lookup with data in DIFFERENT column order
            print(f"\n📝 Test 2: Testing update_range_by_lookup with misaligned columns")
            print(f"   Sheet headers: 外部单号, 产品名称, 数量, 单价, 总价, 备注")
            print(f"   Data order:    备注, 外部单号, 总价, 产品名称, 单价, 数量")
            print(f"   Expected: Data should be reordered to match sheet headers")

            # Incoming data with DIFFERENT column order than sheet headers
            # Note: In real scenario, this comes from user/LLM as dict format
            incoming_data = [
                {
                    "备注": "测试订单1",
                    "外部单号": "ORDER-001",
                    "总价": 100.0,
                    "产品名称": "产品A",
                    "单价": 10.0,
                    "数量": 10
                },
                {
                    "备注": "测试订单2",
                    "外部单号": "ORDER-002",
                    "总价": 200.0,
                    "产品名称": "产品B",
                    "单价": 20.0,
                    "数量": 10
                }
            ]

            update_res = await session.call_tool("update_range_by_lookup", {
                "uri": test_sheet_uri,
                "data": incoming_data,
                "on": "外部单号"
            })

            print(f"   Update result: {update_res}")

            if not update_res.isError and update_res.content and update_res.content[0].text:
                result_content = json.loads(update_res.content[0].text)
                if result_content.get('success'):
                    message = result_content.get('message', '')
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   📊 Message: {message}")
                    print(f"   📊 Updated cells: {updated_cells}")
                    print(f"   📊 Shape: {shape}")

                    # Check if message indicates fallback to append
                    if "Sheet had only headers" in message:
                        print(f"   ✅ Correctly detected header-only sheet")
                        print(f"   ✅ Fallback to append_rows triggered")
                    elif "new rows appended" in message:
                        print(f"   ✅ Appended unmatched rows (normal lookup path)")
                    else:
                        print(f"   ⚠️  Message doesn't indicate append operation")
                else:
                    print(f"   ❌ Update failed: {result_content.get('message', 'Unknown error')}")

            # Test 3: Verify data alignment by reading back
            print(f"\n📝 Test 3: Verifying data alignment in sheet")

            verify_res = await session.call_tool("read_sheet", {
                "uri": test_sheet_uri
            })

            if not verify_res.isError and verify_res.content and verify_res.content[0].text:
                content = json.loads(verify_res.content[0].text)
                if content.get('success'):
                    data = content.get('data', [])
                    headers_found = list(data[0].keys()) if data else []

                    print(f"   📊 Data rows: {len(data)}")
                    print(f"   📊 Headers: {headers_found}")

                    if len(data) >= 2:
                        # Check first row
                        row1 = data[0]
                        row2 = data[1]

                        print(f"\n   📝 First row values:")
                        for header in headers_found:
                            print(f"      {header}: {row1.get(header, 'N/A')}")

                        # Verify column alignment
                        expected_mapping = {
                            "外部单号": "ORDER-001",
                            "产品名称": "产品A",
                            "数量": "10",
                            "单价": "10.0",
                            "总价": "100.0",
                            "备注": "测试订单1"
                        }

                        print(f"\n   🔍 Verifying column alignment:")
                        all_correct = True
                        for header, expected_value in expected_mapping.items():
                            actual_value = str(row1.get(header, ''))

                            # For numeric comparisons, compare as floats to handle "10.0" vs "10"
                            try:
                                expected_float = float(expected_value)
                                actual_float = float(actual_value)
                                is_correct = expected_float == actual_float
                            except (ValueError, TypeError):
                                # Not numeric, compare as strings
                                is_correct = actual_value == expected_value

                            status = "✅" if is_correct else "❌"

                            print(f"      {status} {header}: expected='{expected_value}', actual='{actual_value}'")

                            if not is_correct:
                                all_correct = False

                        if all_correct:
                            print(f"\n   ✅ SUCCESS: All columns correctly aligned!")
                            print(f"   ✅ Data matches expected values")
                        else:
                            print(f"\n   ❌ FAILURE: Column alignment is incorrect!")
                            print(f"   ❌ Data does not match expected values")
                            print(f"\n   🔍 Debugging info:")
                            print(f"      This indicates the bug where data is appended")
                            print(f"      without reordering to match sheet headers")
                    else:
                        print(f"   ❌ Expected at least 2 data rows, got {len(data)}")

            # Test 4: Test with missing columns in data
            print(f"\n📝 Test 4: Testing with missing columns in incoming data")
            print(f"   Data missing columns: 单价, 总价")
            print(f"   Expected: Missing columns should be empty")

            partial_data = [
                {
                    "外部单号": "ORDER-003",
                    "产品名称": "产品C",
                    "数量": 5,
                    "备注": "缺少价格信息"
                    # Missing: 单价, 总价
                }
            ]

            partial_update_res = await session.call_tool("update_range_by_lookup", {
                "uri": test_sheet_uri,
                "data": partial_data,
                "on": "外部单号"
            })

            if not partial_update_res.isError and partial_update_res.content and partial_update_res.content[0].text:
                result_content = json.loads(partial_update_res.content[0].text)
                if result_content.get('success'):
                    print(f"   ✅ Partial data append succeeded")

                    # Verify
                    verify_res = await session.call_tool("read_sheet", {
                        "uri": test_sheet_uri
                    })

                    if not verify_res.isError and verify_res.content and verify_res.content[0].text:
                        content = json.loads(verify_res.content[0].text)
                        if content.get('success'):
                            data = content.get('data', [])

                            # Find ORDER-003 row
                            order_003_row = None
                            for row in data:
                                if row.get("外部单号") == "ORDER-003":
                                    order_003_row = row
                                    break

                            if order_003_row:
                                print(f"\n   📝 ORDER-003 row values:")
                                for header in ["外部单号", "产品名称", "数量", "单价", "总价", "备注"]:
                                    value = order_003_row.get(header, '')
                                    print(f"      {header}: '{value}'")

                                # Check missing columns are empty
                                if order_003_row.get("单价", "") == "" and order_003_row.get("总价", "") == "":
                                    print(f"   ✅ Missing columns correctly filled with empty values")
                                else:
                                    print(f"   ❌ Missing columns not empty: 单价='{order_003_row.get('单价')}', 总价='{order_003_row.get('总价')}'")

            print(f"\n✅ Column alignment test completed!")
            print(f"\n📊 Summary:")
            print(f"   - Tested update_range_by_lookup with header-only sheet")
            print(f"   - Tested column reordering to match sheet headers")
            print(f"   - Tested handling of missing columns")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test column alignment for header-only sheets")
    parser.add_argument("--env", choices=["local", "test", "prod"], default="local",
                       help="Environment: local, test, or prod")
    args = parser.parse_args()

    # Set endpoint based on environment
    if args.env == "test":
        endpoint = "https://datatable-mcp-test.maybe.ai"
    elif args.env == "prod":
        endpoint = "https://datatable-mcp.maybe.ai"
    else:
        endpoint = "http://127.0.0.1:8321"

    print(f"🔗 Using {args.env} environment: {endpoint}")

    # OAuth headers
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    asyncio.run(test_column_alignment(url=f"{endpoint}/mcp", headers=test_headers))
