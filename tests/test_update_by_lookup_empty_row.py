#!/usr/bin/env python3
"""
Test to reproduce the empty row issue in update_range_by_lookup.

The issue: When update_by_lookup is called and some rows don't match (unmatched rows),
those unmatched rows are appended. However, there appears to be an empty row left between
the updated rows and the appended rows.

Expected behavior:
- Updated rows: rows 2-5 (4 rows matching lookup)
- Appended rows: rows 6-11 (6 unmatched rows)
- NO empty row between them

Actual behavior:
- Updated rows: rows 2-5 (4 rows)
- Empty row: row 6
- Appended rows: rows 7-12 (6 rows)

Usage:
    python test_update_by_lookup_empty_row.py --env=local
    python test_update_by_lookup_empty_row.py --env=test
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession
from datetime import datetime
import os
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Test sheet URL - we'll use write_new_worksheet to add worksheets to an existing spreadsheet
# This avoids creating too many new spreadsheets
TEST_SPREADSHEET_URI = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M"


async def test_update_by_lookup_empty_row_issue(url, headers):
    """
    Test that update_by_lookup doesn't leave empty rows between updated and appended data.

    Steps:
    1. Create a new sheet with initial data (4 rows with SKUs)
    2. Call update_by_lookup with mixed data (some matching, some new)
    3. Verify NO empty rows exist between matched and appended data
    """
    print("🧪 Testing update_by_lookup for empty row issue")
    print("=" * 80)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: Create a new test sheet with initial data
            # IMPORTANT: Include an empty row (row 6) to match the user's screenshot
            print("\n📝 Step 1: Creating new worksheet with initial data (including empty row 6)")

            worksheet_name = f"EmptyRowTest-{datetime.now().strftime('%H%M%S')}"

            initial_data = [
                {"SKU": "BF1D-37768979-04", "仓库库存": "18342", "备注": "Initial row 1"},
                {"SKU": "BF1D-37768979-05", "仓库库存": "4", "备注": "Initial row 2"},
                {"SKU": "BF1D-37768979-06", "仓库库存": "6", "备注": "Initial row 3"},
                {"SKU": "BF1D-37768979-07", "仓库库存": "10", "备注": "Initial row 4"},
                {"SKU": "", "仓库库存": "", "备注": ""}  # Empty row 5 (will become row 6 in sheet)
            ]

            create_res = await session.call_tool("write_new_worksheet", {
                "uri": TEST_SPREADSHEET_URI,
                "data": initial_data,
                "worksheet_name": worksheet_name
            })

            if create_res.isError:
                print(f"❌ Failed to create test sheet: {create_res}")
                return

            result_content = json.loads(create_res.content[0].text)
            test_sheet_url = result_content.get('spreadsheet_url')
            print(f"✅ Created test worksheet: {worksheet_name}")
            print(f"   URL: {test_sheet_url}")
            print(f"   Initial data: 5 rows (4 with data + 1 empty row)")
            print(f"   Rows 2-5: BF1D-37768979-04 through -07")
            print(f"   Row 6: Empty row (SKU='', 仓库库存='', 备注='')")

            # Step 2: Prepare update data with some matching and some new SKUs
            print("\n📝 Step 2: Preparing update data (mixed: some matching, some new)")

            update_data = [
                # Matching row - should update existing row 2 (BF1D-37768979-04)
                {"SKU": "BF1D-37768979-04", "仓库库存": "18342", "备注": "Updated via lookup"},

                # Matching row - should update existing row 5 (BF1D-37768979-07)
                {"SKU": "BF1D-37768979-07", "仓库库存": "10", "备注": "Updated via lookup"},

                # NEW rows (unmatched) - should be APPENDED after row 5, NO GAP
                {"SKU": "5EWR-P0100001-01", "仓库库存": "100", "备注": "New row 1"},
                {"SKU": "5EWR-P0100001-02", "仓库库存": "200", "备注": "New row 2"},
                {"SKU": "5EWR-P0100001-03", "仓库库存": "300", "备注": "New row 3"},
                {"SKU": "5EWR-P0100001-06", "仓库库存": "400", "备注": "New row 4"},
                {"SKU": "5EWR-P0100001-07", "仓库库存": "500", "备注": "New row 5"},
                {"SKU": "5EWR-P0100002-02", "仓库库存": "600", "备注": "New row 6"}
            ]

            print(f"   Update data: 8 rows total")
            print(f"   - 2 matching (will update existing rows)")
            print(f"   - 6 new (should append without gaps)")

            # Step 3: Call update_by_lookup
            print("\n📝 Step 3: Calling update_by_lookup")

            update_res = await session.call_tool("update_range_by_lookup", {
                "uri": test_sheet_url,
                "data": update_data,
                "on": "SKU",
                "override": False
            })

            if update_res.isError:
                print(f"❌ update_by_lookup failed: {update_res}")
                return

            update_result = json.loads(update_res.content[0].text)
            print(f"✅ update_by_lookup completed:")
            print(f"   Message: {update_result.get('message', 'N/A')}")

            # Step 4: Read back the sheet and check for empty rows
            print("\n📝 Step 4: Reading back sheet to verify no empty rows")

            read_res = await session.call_tool("read_sheet", {
                "uri": test_sheet_url
            })

            if read_res.isError:
                print(f"❌ Failed to read sheet: {read_res}")
                return

            read_result = json.loads(read_res.content[0].text)
            data = read_result.get('data', [])

            print(f"\n📊 Final sheet contents ({len(data)} data rows):")
            print(f"   {'Row':<5} {'SKU':<20} {'仓库库存':<10} {'备注':<20}")
            print("   " + "-" * 60)

            empty_rows = []
            for idx, row in enumerate(data):
                row_num = idx + 2  # Row 1 is headers, data starts at row 2
                sku = row.get('SKU', '')
                stock = row.get('仓库库存', '')
                note = row.get('备注', '')

                # Check if row is empty (all values are empty strings)
                is_empty = not sku and not stock and not note

                if is_empty:
                    empty_rows.append(row_num)
                    print(f"   {row_num:<5} {'<EMPTY ROW>':<20} {'<EMPTY>':<10} {'<EMPTY>':<20} ❌")
                else:
                    print(f"   {row_num:<5} {sku:<20} {stock:<10} {note:<20}")

            # Step 5: Verify results
            print("\n📊 Test Results:")
            print("=" * 80)

            if empty_rows:
                print(f"❌ FAIL: Found {len(empty_rows)} empty row(s) at: {empty_rows}")
                print(f"   Expected: NO empty rows between updated and appended data")
                print(f"   Actual: Empty row(s) detected")
                return False
            else:
                print(f"✅ PASS: No empty rows found")
                print(f"   All {len(data)} data rows are contiguous")
                return True


async def main():
    parser = argparse.ArgumentParser(description="Test update_by_lookup empty row issue")
    parser.add_argument("--env", choices=["local", "test"], default="local",
                       help="Environment: local (127.0.0.1:8321) or test (datatable-mcp-test.maybe.ai)")
    args = parser.parse_args()

    # Set endpoint
    if args.env == "test":
        endpoint = "https://datatable-mcp-test.maybe.ai"
    else:
        endpoint = "http://127.0.0.1:8321"

    print(f"🔗 Using {args.env} environment: {endpoint}")

    # OAuth headers
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    # Validate environment variables
    missing_vars = [k for k, v in test_headers.items() if v is None]
    if missing_vars:
        print(f"❌ ERROR: Missing environment variables: {', '.join(missing_vars)}")
        return

    # Run test
    success = await test_update_by_lookup_empty_row_issue(
        url=f"{endpoint}/mcp",
        headers=test_headers
    )

    if success:
        print("\n✅ Test PASSED: No empty rows detected")
        exit(0)
    else:
        print("\n❌ Test FAILED: Empty rows found in sheet")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
