#!/usr/bin/env python3
"""
Test copy_range_with_formulas with auto_fill mode

Test URI: https://docs.google.com/spreadsheets/d/15jns06J6TZqPds2EZFwVtVwPfpHxKb2_um3q2fCvAEE/edit?gid=2140520008#gid=2140520008

Usage:
    python tests/test_copy_range_autofill.py --env=test
    python tests/test_copy_range_autofill.py --env=local
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession
import os
import argparse

# Test configuration
TEST_URI = "https://docs.google.com/spreadsheets/d/15jns06J6TZqPds2EZFwVtVwPfpHxKb2_um3q2fCvAEE/edit?gid=2140520008#gid=2140520008"


async def test_copy_range_autofill(url, headers):
    """Test copy_range_with_formulas with auto_fill mode"""
    print(f"🚀 Testing copy_range_with_formulas with auto_fill")
    print(f"📋 Test URI: {TEST_URI}")
    print("=" * 80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Read the sheet first to see current state
            print(f"\n📘 Test 1: Reading current sheet state")
            read_res = await session.call_tool("read_sheet", {"uri": TEST_URI})

            if not read_res.isError and read_res.content and read_res.content[0].text:
                content = json.loads(read_res.content[0].text)
                if content.get('success'):
                    data = content.get('data', [])
                    print(f"   ✅ Current sheet has {len(data)} data rows")

                    # Show first few rows
                    print(f"\n   📊 First 5 rows:")
                    for i, row in enumerate(data[:5], 1):
                        sku = row.get('SKU', 'N/A')
                        # Get first few column values
                        cols = list(row.keys())[:4]
                        values = [f"{col}={row.get(col, 'N/A')}" for col in cols[:3]]
                        print(f"      Row {i}: {', '.join(values)}")

            # Test 2: Auto-fill formulas from row 2 to all data rows
            print(f"\n📝 Test 2: Auto-fill formulas from B2:K2 to all data rows")
            print(f"   Parameters:")
            print(f"     - from_range: B2:K2 (formula row)")
            print(f"     - auto_fill: True")
            print(f"     - lookup_column: A (SKU column)")
            print(f"     - skip_if_exists: True (skip rows with existing formulas)")

            autofill_res = await session.call_tool("copy_range_with_formulas", {
                "uri": TEST_URI,
                "from_range": "B2:AE2",
                "auto_fill": True,
                "lookup_column": "A",
                "skip_if_exists": True
            })

            print(f"\n   Result: {autofill_res}")

            if not autofill_res.isError and autofill_res.content and autofill_res.content[0].text:
                result = json.loads(autofill_res.content[0].text)
                if result.get('success'):
                    print(f"\n   ✅ Auto-fill succeeded!")
                    print(f"      Updated cells: {result.get('updated_cells', 0)}")
                    print(f"      Range: {result.get('range', 'N/A')}")
                    print(f"      Message: {result.get('message', 'N/A')}")
                else:
                    print(f"\n   ❌ Auto-fill failed: {result.get('message', 'Unknown error')}")
            else:
                error_msg = autofill_res.content[0].text if autofill_res.content else "Unknown error"
                print(f"\n   ❌ Error: {error_msg}")

            # Test 3: Read the sheet again to verify formulas were copied
            print(f"\n📘 Test 3: Verifying formulas were copied")
            read_res2 = await session.call_tool("read_sheet", {"uri": TEST_URI})

            if not read_res2.isError and read_res2.content and read_res2.content[0].text:
                content = json.loads(read_res2.content[0].text)
                if content.get('success'):
                    data = content.get('data', [])
                    print(f"   ✅ Sheet now has {len(data)} data rows")

                    # Show first few rows with their calculated values
                    print(f"\n   📊 First 5 rows after auto-fill:")
                    for i, row in enumerate(data[:5], 1):
                        sku = row.get('SKU', 'N/A')
                        # Get first date column value to verify formula worked
                        cols = list(row.keys())
                        if len(cols) > 1:
                            first_date_col = cols[1] if len(cols) > 1 else None
                            first_value = row.get(first_date_col, 'N/A') if first_date_col else 'N/A'
                            print(f"      Row {i}: SKU={sku}, {first_date_col}={first_value}")

            print(f"\n✅ Auto-fill test completed!")


async def test_manual_copy(url, headers):
    """Test manual copy mode (single range to single range)"""
    print(f"\n🚀 Testing manual copy mode")
    print("=" * 80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test: Copy B2:K2 to B7:K7 manually
            print(f"\n📝 Test: Manual copy from B2:K2 to B7:K7")

            manual_res = await session.call_tool("copy_range_with_formulas", {
                "uri": TEST_URI,
                "from_range": "B2:K2",
                "to_range": "B7:K7",
                "auto_fill": False
            })

            print(f"\n   Result: {manual_res}")

            if not manual_res.isError and manual_res.content and manual_res.content[0].text:
                result = json.loads(manual_res.content[0].text)
                if result.get('success'):
                    print(f"\n   ✅ Manual copy succeeded!")
                    print(f"      Updated cells: {result.get('updated_cells', 0)}")
                    print(f"      Message: {result.get('message', 'N/A')}")
                else:
                    print(f"\n   ❌ Manual copy failed: {result.get('message', 'Unknown error')}")

            print(f"\n✅ Manual copy test completed!")


async def test_multi_row_copy(url, headers):
    """Test multi-row copy mode (single source row to multiple destination rows)"""
    print(f"\n🚀 Testing multi-row copy mode (Stage 2 Feature)")
    print("=" * 80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Copy single source row to multiple destination rows
            print(f"\n📝 Test 1: Multi-row copy from B2:AE2 to B3:AE5 (3 rows)")
            print(f"   This tests the new Stage 2 feature:")
            print(f"   - Single source row (B2:AE2)")
            print(f"   - Multiple destination rows (rows 3-5)")
            print(f"   - Formulas should adapt for each destination row")

            multi_row_res = await session.call_tool("copy_range_with_formulas", {
                "uri": TEST_URI,
                "from_range": "B2:AE2",
                "to_range": "B3:AE5",
                "auto_fill": False,
                "skip_if_exists": False
            })

            print(f"\n   Result: {multi_row_res}")

            if not multi_row_res.isError and multi_row_res.content and multi_row_res.content[0].text:
                result = json.loads(multi_row_res.content[0].text)
                if result.get('success'):
                    print(f"\n   ✅ Multi-row copy succeeded!")
                    print(f"      Updated cells: {result.get('updated_cells', 0)}")
                    print(f"      Message: {result.get('message', 'N/A')}")

                    # Verify expected number of cells updated
                    # 3 rows × 30 columns (B to AE) = 90 cells
                    expected_cells = 3 * 30
                    actual_cells = result.get('updated_cells', 0)
                    if actual_cells == expected_cells:
                        print(f"      ✅ Cell count matches expected: {expected_cells}")
                    else:
                        print(f"      ⚠️  Cell count mismatch: expected {expected_cells}, got {actual_cells}")
                else:
                    print(f"\n   ❌ Multi-row copy failed: {result.get('message', 'Unknown error')}")
            else:
                error_msg = multi_row_res.content[0].text if multi_row_res.content else "Unknown error"
                print(f"\n   ❌ Error: {error_msg}")

            # Test 2: Multi-row copy with skip_if_exists=True
            print(f"\n📝 Test 2: Multi-row copy with skip_if_exists=True")
            print(f"   Copy B2:AE2 to B20:AE25, but skip rows that already have data")

            skip_test_res = await session.call_tool("copy_range_with_formulas", {
                "uri": TEST_URI,
                "from_range": "B2:AE2",
                "to_range": "B20:AE25",
                "auto_fill": False,
                "skip_if_exists": True
            })

            if not skip_test_res.isError and skip_test_res.content and skip_test_res.content[0].text:
                result = json.loads(skip_test_res.content[0].text)
                if result.get('success'):
                    print(f"\n   ✅ Skip test succeeded!")
                    print(f"      Updated cells: {result.get('updated_cells', 0)}")
                    print(f"      Message: {result.get('message', 'N/A')}")
                    print(f"   💡 Note: Rows with existing data were skipped")
                else:
                    print(f"\n   ❌ Skip test failed: {result.get('message', 'Unknown error')}")

            # Test 3: Verify formulas were correctly adapted
            print(f"\n📝 Test 3: Verifying formula adaptation")
            print(f"   Reading back rows 10-15 to check if formulas adapted correctly")

            read_res = await session.call_tool("read_sheet", {
                "uri": TEST_URI
            })

            if not read_res.isError and read_res.content and read_res.content[0].text:
                content = json.loads(read_res.content[0].text)
                if content.get('success'):
                    data = content.get('data', [])
                    print(f"   ✅ Read sheet successfully")
                    print(f"      Total rows: {len(data)}")

                    # Show sample rows
                    if len(data) >= 15:
                        print(f"\n   📊 Sample rows 10-12 (checking first column values):")
                        for i in range(9, 12):  # Rows 10-12 (0-indexed)
                            if i < len(data):
                                row = data[i]
                                first_col = list(row.keys())[1] if len(row.keys()) > 1 else 'N/A'
                                first_val = row.get(first_col, 'N/A')
                                print(f"         Row {i+1}: {first_col} = {first_val}")
                    else:
                        print(f"   ⚠️  Not enough rows to verify (only {len(data)} rows)")

            print(f"\n✅ Multi-row copy test completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test copy_range_with_formulas auto-fill")
    parser.add_argument("--env", choices=["local", "test", "prod"], default="local",
                       help="Environment: local (127.0.0.1:8321), test (datatable-mcp-test.maybe.ai), or prod")
    parser.add_argument("--test", choices=["autofill", "multirow", "all"], default="all",
                       help="Which test to run")
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

    # Run tests
    if args.test == "autofill":
        asyncio.run(test_copy_range_autofill(url=f"{endpoint}/mcp", headers=test_headers))
    # elif args.test == "manual":
    #     asyncio.run(test_manual_copy(url=f"{endpoint}/mcp", headers=test_headers))
    elif args.test == "multirow":
        asyncio.run(test_multi_row_copy(url=f"{endpoint}/mcp", headers=test_headers))
    else:
        asyncio.run(test_copy_range_autofill(url=f"{endpoint}/mcp", headers=test_headers))
        # asyncio.run(test_manual_copy(url=f"{endpoint}/mcp", headers=test_headers))
        asyncio.run(test_multi_row_copy(url=f"{endpoint}/mcp", headers=test_headers))
