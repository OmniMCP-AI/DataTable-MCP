#!/usr/bin/env python3
"""
Diagnostic test to understand why sheet headers are not being detected in update_range_by_lookup.

This test loads the problematic sheet and examines what data is returned.

Usage:
    python test_diagnose_empty_headers.py --env=local
    python test_diagnose_empty_headers.py --env=test
    python test_diagnose_empty_headers.py --env=prod

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
import argparse
import os
from mcp import ClientSession

# Test configuration - using same pattern as test_mcp_client_calltool.py
TEST_USER_ID = "68501372a3569b6897673a48"

# The problematic sheet from the ORIGINAL error log
PROBLEM_SHEET_URI = "https://docs.google.com/spreadsheets/d/15jns06J6TZqPds2EZFwVtVwPfpHxKb2_um3q2fCvAEE/edit?gid=91360988#gid=91360988"

# Alternative sheet (updated by user)
ALTERNATIVE_SHEET_URI = "https://docs.google.com/spreadsheets/d/1bRSoV6yGeOiWTt_adrSEqSgV69gOPqr2S9rkEgc_XbQ/edit?gid=91360988#gid=91360988"


async def diagnose_sheet_loading(url, headers):
    """Diagnose why sheet headers are not detected."""
    print(f"\n{'='*80}")
    print(f"🔍 Diagnosing Sheet Header Detection Issue")
    print(f"{'='*80}")
    print(f"Sheet URI: {PROBLEM_SHEET_URI}")
    print(f"User ID: {TEST_USER_ID}")
    print()

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Load the table normally (with auto header detection)
            print(f"\n📋 Test 1: Load table with auto header detection (default)")
            print("-" * 80)

            load_res = await session.call_tool("load_data_table", {
                "uri": PROBLEM_SHEET_URI,
            })

            if load_res.isError:
                print(f"❌ Load failed with error:")
                if load_res.content:
                    print(f"   {load_res.content[0].text}")
            else:
                if load_res.content and load_res.content[0].text:
                    content = json.loads(load_res.content[0].text)
                    print(f"✅ Load successful: {content.get('success')}")
                    print(f"\nResponse details:")
                    print(f"  - Table ID: {content.get('table_id')}")
                    print(f"  - Shape: {content.get('shape')}")

                    # Check source info
                    if 'source_info' in content:
                        info = content['source_info']
                        print(f"\nSource Info:")
                        print(f"  - Worksheet: {info.get('worksheet')}")
                        print(f"  - Row count: {info.get('row_count')}")
                        print(f"  - Column count: {info.get('column_count')}")
                        print(f"  - Used range: {info.get('used_range')}")

                    # Check data
                    if 'data' in content:
                        data = content['data']
                        print(f"\nData preview:")
                        print(f"  - Number of rows: {len(data)}")
                        if data:
                            print(f"  - First row keys: {list(data[0].keys())}")
                            print(f"  - First row sample: {dict(list(data[0].items())[:3])}")
                        else:
                            print(f"  ⚠️  Data is empty!")

            # Test 2: Try loading with range_address specified
            print(f"\n\n📋 Test 2: Load table with explicit range_address")
            print("-" * 80)

            # Try loading with A1:Z1000 range
            load_res2 = await session.call_tool("load_data_table", {
                "uri": PROBLEM_SHEET_URI,
                "range_address": "A1:Z1000"
            })

            if load_res2.isError:
                print(f"❌ Load failed with error:")
                if load_res2.content:
                    print(f"   {load_res2.content[0].text}")
            else:
                if load_res2.content and load_res2.content[0].text:
                    content2 = json.loads(load_res2.content[0].text)
                    print(f"✅ Load successful: {content2.get('success')}")
                    print(f"\nResponse details:")
                    print(f"  - Table ID: {content2.get('table_id')}")
                    print(f"  - Shape: {content2.get('shape')}")

                    # Check data
                    if 'data' in content2:
                        data2 = content2['data']
                        print(f"\nData preview:")
                        print(f"  - Number of rows: {len(data2)}")
                        if data2:
                            print(f"  - First row keys: {list(data2[0].keys())}")
                            print(f"  - First row sample: {dict(list(data2[0].items())[:3])}")
                        else:
                            print(f"  ⚠️  Data is empty!")

            # Test 3: Try a simple update_range_by_lookup to see the actual error
            print(f"\n\n📋 Test 3: Attempt update_range_by_lookup (expect error)")
            print("-" * 80)

            test_data = [
                {"ERP单号": "S251115334724", "订单状态": "测试更新"}
            ]

            try:
                update_res = await session.call_tool("update_range_by_lookup", {
                    "uri": PROBLEM_SHEET_URI,
                    "data": test_data,
                    "on": "ERP单号",
                    "override": False
                })

                if update_res.isError:
                    print(f"❌ Update failed (expected):")
                    if update_res.content:
                        error_text = update_res.content[0].text
                        print(f"\n{error_text}")
                        print()
                else:
                    if update_res.content and update_res.content[0].text:
                        content3 = json.loads(update_res.content[0].text)
                        print(f"✅ Update successful (unexpected!)")
                        print(f"   Result: {content3}")
            except Exception as e:
                print(f"❌ Exception during update: {e}")

            print(f"\n{'='*80}")
            print(f"Diagnosis complete!")
            print(f"{'='*80}\n")


async def main():
    parser = argparse.ArgumentParser(description='Diagnose empty headers issue')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against: local (127.0.0.1:8321), test (datatable-mcp-test.maybe.ai), or prod (datatable-mcp.maybe.ai)')
    args = parser.parse_args()

    # Configure environment - matching test_mcp_client_calltool.py pattern
    if args.env == "test":
        endpoint = "https://datatable-mcp-test.maybe.ai"
    elif args.env == "prod":
        endpoint = "https://datatable-mcp.maybe.ai"
    else:  # local
        endpoint = "http://127.0.0.1:8321"

    print(f"\n🔗 Using {args.env} environment: {endpoint}")
    print(f"💡 Use --env=local for local development or --env=test/prod for hosted")

    # OAuth headers for testing - matching test_mcp_client_calltool.py pattern
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    print(f"\n🌐 Testing against: {endpoint}/mcp")

    try:
        await diagnose_sheet_loading(url=f"{endpoint}/mcp", headers=test_headers)
        print("✅ All diagnostic tests completed successfully")
        return 0
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
