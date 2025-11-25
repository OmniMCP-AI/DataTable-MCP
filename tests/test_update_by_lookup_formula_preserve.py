#!/usr/bin/env python3
"""
Test update_range_by_lookup formula preservation and unmatched row appending.

This test verifies two critical fixes:
1. Formula headers are preserved (not overwritten with evaluated values)
2. Unmatched rows are appended as new data instead of being silently ignored

Test scenario:
- Sheet has 1 row with SKU "BF1D-37768979-04" and a formula header in column B
- Update data has 4 SKUs: 04 (matched), 05, 06, 07 (unmatched)
- Expected: Row 04 is updated, rows 05-07 are appended, formula header is preserved

Usage:
    python test_update_by_lookup_formula_preserve.py --env=local
    python test_update_by_lookup_formula_preserve.py --env=test

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession
import sys
import argparse

# Test sheet URI - replace with your test sheet
# Expected: Has headers in row 1, with column B containing a formula like =MAX(INDEX(...))
# Has one data row (row 2) with SKU "BF1D-37768979-04"
TEST_SHEET_URI = "https://docs.google.com/spreadsheets/d/15jns06J6TZqPds2EZFwVtVwPfpHxKb2_um3q2fCvAEE/edit?gid=2140520008#gid=2140520008"


async def test_formula_preserve_and_append(url, headers):
    """Test update_by_lookup preserves formulas and appends unmatched rows"""
    print(f"🔧 Testing update_by_lookup with formula preservation and row appending")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: Read original sheet with formulas
            print(f"\n📖 Step 1: Reading original sheet with formulas...")
            print(f"   URI: {TEST_SHEET_URI}")

            read_result = await session.call_tool("read_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
            })

            if read_result.isError:
                print(f"❌ Failed to read sheet: {read_result.content[0].text if read_result.content else 'Unknown error'}")
                return False

            original_data = json.loads(read_result.content[0].text)
            print(f"✅ Loaded sheet: {original_data.get('shape', 'unknown')} rows")
            print(f"   Message: {original_data.get('message', '')}")

            # Check for formula headers
            if original_data.get('data'):
                first_row = original_data['data'][0]
                headers = list(first_row.keys())
                formula_headers = [h for h in headers if str(h).startswith('=')]

                if formula_headers:
                    print(f"✅ Found {len(formula_headers)} formula headers")
                    print(f"   Examples: {formula_headers[:3]}")
                else:
                    print("⚠️  No formula headers detected")

            original_row_count = len(original_data.get('data', []))
            print(f"📊 Original row count: {original_row_count}")

            # Step 2: Update by lookup with 4 SKUs (1 matched, 3 unmatched)
            print(f"\n🔄 Step 2: Running update_range_by_lookup...")

            update_data = [
                {"SKU": "BF1D-37768979-04"},  # Matched - should update existing row
                {"SKU": "BF1D-37768979-05"},  # Unmatched - should be appended
                {"SKU": "BF1D-37768979-06"},  # Unmatched - should be appended
                {"SKU": "BF1D-37768979-07"},  # Unmatched - should be appended
            ]

            print(f"📝 Update data: {len(update_data)} rows")
            for row in update_data:
                print(f"   - {row['SKU']}")

            # Convert to proper format
            update_result = await session.call_tool("update_range_by_lookup", {
                "uri": TEST_SHEET_URI,
                "data": update_data,
                "on": "SKU",
                "override": False
            })

            if update_result.isError:
                print(f"❌ Update failed: {update_result.content[0].text if update_result.content else 'Unknown error'}")
                return False

            update_response = json.loads(update_result.content[0].text)
            print(f"✅ Update response: {update_response.get('message', '')}")

            # Step 3: Verify results
            print(f"\n🔍 Step 3: Verifying results...")

            # Read back with formulas
            verify_result = await session.call_tool("read_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
            })

            if verify_result.isError:
                print(f"❌ Failed to verify: {verify_result.content[0].text if verify_result.content else 'Unknown error'}")
                return False

            verify_data = json.loads(verify_result.content[0].text)
            final_row_count = len(verify_data.get('data', []))

            print(f"✅ Sheet now has {final_row_count} data rows")

            # Check 1: Headers still have formulas
            if verify_data.get('data'):
                first_row = verify_data['data'][0]
                headers_after = list(first_row.keys())
                formula_headers_after = [h for h in headers_after if str(h).startswith('=')]

                if formula_headers_after:
                    print(f"✅ PASS: Headers still contain {len(formula_headers_after)} formulas")
                    print(f"   Examples: {formula_headers_after[:3]}")
                    test1_pass = True
                else:
                    print("❌ FAIL: Formula headers were overwritten!")
                    test1_pass = False
            else:
                print("❌ FAIL: No data in verification")
                test1_pass = False

            # Check 2: Row count increased by 3 (appended unmatched rows)
            expected_rows = original_row_count + 3  # 3 unmatched SKUs should be appended

            if final_row_count == expected_rows:
                print(f"✅ PASS: Row count is correct ({final_row_count} rows, +3 appended)")
                test2_pass = True
            else:
                print(f"❌ FAIL: Expected {expected_rows} rows (original {original_row_count} + 3 appended), got {final_row_count}")
                test2_pass = False

            # Check 3: All 4 SKUs are present
            print(f"\n📋 Final SKU list:")
            skus_found = []
            for idx, row in enumerate(verify_data.get('data', []), 1):
                sku = row.get('SKU', 'N/A')
                skus_found.append(sku)
                print(f"   Row {idx}: {sku}")

            expected_skus = ["BF1D-37768979-04", "BF1D-37768979-05", "BF1D-37768979-06", "BF1D-37768979-07"]
            all_present = all(sku in skus_found for sku in expected_skus)

            if all_present:
                print(f"✅ PASS: All 4 SKUs are present in the sheet")
                test3_pass = True
            else:
                missing = [sku for sku in expected_skus if sku not in skus_found]
                print(f"❌ FAIL: Missing SKUs: {missing}")
                test3_pass = False

            # Final verdict
            all_pass = test1_pass and test2_pass and test3_pass
            print("\n" + "=" * 60)
            if all_pass:
                print("✅ ALL TESTS PASSED!")
            else:
                print("❌ SOME TESTS FAILED")
                if not test1_pass:
                    print("   - Formula preservation: FAILED")
                if not test2_pass:
                    print("   - Row append count: FAILED")
                if not test3_pass:
                    print("   - SKU completeness: FAILED")

            print(f"🔗 View sheet: {update_response.get('spreadsheet_url', TEST_SHEET_URI)}")

            return all_pass


async def main():
    """Main entry point"""
    import os

    parser = argparse.ArgumentParser(description='Test formula preservation in update_by_lookup')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                        help='Environment to test against (default: local)')
    args = parser.parse_args()

    # Set endpoint based on environment argument
    if args.env == "test":
        endpoint = "https://datatable-mcp-test.maybe.ai"
    elif args.env == "prod":
        endpoint = "https://datatable-mcp.maybe.ai"
    else:
        endpoint = "http://127.0.0.1:8321"
    endpoint = f"{endpoint}/mcp"
    print(f"🔗 Using {args.env} environment: {endpoint}")
    print(f"💡 Use --env=local for local development or --env=test for test environment")

    # OAuth headers for testing
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    try:
        success = await test_formula_preserve_and_append(endpoint, test_headers)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Test execution failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
