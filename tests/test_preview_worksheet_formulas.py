#!/usr/bin/env python3
"""
Test suite for preview_worksheet_with_formulas functionality

Tests cover:
1. Basic preview with default limit (5 rows)
2. Preview with custom limit (10, 20 rows)
3. Verify only N rows returned (not full data)
4. Compare preview vs full read (performance benefit)
5. Verify formulas are returned correctly

Usage:
    # Run all preview tests
    python test_preview_worksheet_with_formulas.py --env=local --test=all

    # Run specific test
    python test_preview_worksheet_with_formulas.py --env=local --test=basic
    python test_preview_worksheet_with_formulas.py --env=local --test=limits
    python test_preview_worksheet_with_formulas.py --env=local --test=compare

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

# Test configuration
TEST_USER_ID = "68501372a3569b6897673a48"

# Test sheet with formulas
TEST_SHEET_URI = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit?gid=0"


async def test_basic_preview(url, headers):
    """Test 1: Basic preview with default limit (5 rows)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 1: Basic Preview (Default 5 rows)")
    print(f"{'='*60}")
    print(f"Purpose: Preview first 5 rows with formulas")
    print(f"Test URI: {TEST_SHEET_URI}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("\n📋 Step 1: Calling preview_worksheet_with_formulas with default limit...")
            preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI
            })

            if preview_res.isError:
                print(f"❌ Failed: {preview_res.content[0].text if preview_res.content else 'Unknown error'}")
                return

            content = json.loads(preview_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Failed: {content.get('message')}")
                return

            # Extract results
            data = content.get('data', [])
            shape = content.get('shape')
            table_id = content.get('table_id')
            name = content.get('name')
            source_info = content.get('source_info', {})

            print(f"✅ Preview loaded successfully!")
            print(f"\n📊 Preview Information:")
            print(f"   Table ID: {table_id}")
            print(f"   Name: {name}")
            print(f"   Shape: {shape}")
            print(f"   Rows returned: {len(data)}")
            print(f"   Preview limit: {source_info.get('preview_limit')}")
            print(f"   Is preview: {source_info.get('is_preview')}")
            print(f"   Value render option: {source_info.get('value_render_option')}")

            # Display preview data
            if data:
                print(f"\n📝 Preview Data (First {len(data)} rows):")
                print("-" * 60)
                for i, row in enumerate(data, start=1):
                    print(f"\n   Row {i}:")
                    for col, value in row.items():
                        if isinstance(value, str) and value.startswith("="):
                            print(f"      {col}: {value} (formula)")
                        else:
                            print(f"      {col}: {value}")

            # Validation
            print(f"\n🔍 Validation:")
            checks = []

            # Check 1: Returned <= 5 rows
            if len(data) <= 5:
                checks.append(('✅', f'Returned {len(data)} rows (<= 5)'))
            else:
                checks.append(('❌', f'Returned {len(data)} rows (> 5)'))

            # Check 2: Is preview flag
            if source_info.get('is_preview') == True:
                checks.append(('✅', 'is_preview flag is True'))
            else:
                checks.append(('❌', 'is_preview flag missing or False'))

            # Check 3: Value render option is FORMULA
            if source_info.get('value_render_option') == 'FORMULA':
                checks.append(('✅', 'Value render option is FORMULA'))
            else:
                checks.append(('❌', f"Value render option is {source_info.get('value_render_option')}"))

            # Check 4: Table ID has preview suffix
            if '_preview_formulas' in table_id:
                checks.append(('✅', 'Table ID has _preview_formulas suffix'))
            else:
                checks.append(('⚠️', 'Table ID missing _preview_formulas suffix'))

            print("-" * 60)
            for status, message in checks:
                print(f"   {status} {message}")

            # Overall result
            passed = sum(1 for status, _ in checks if status == '✅')
            total = len(checks)
            print(f"\n📊 Result: {passed}/{total} checks passed")

            if passed == total:
                print(f"   ✅ PASS: All checks passed")
            else:
                print(f"   ❌ FAIL: Some checks failed")

            print(f"\n{'='*60}")


async def test_custom_limits(url, headers):
    """Test 2: Test with different limit values"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 2: Custom Limit Values")
    print(f"{'='*60}")
    print(f"Purpose: Test preview with different limit values (10, 20, 100)")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test different limits
            test_limits = [10, 20, 100]

            for limit in test_limits:
                print(f"\n📋 Testing limit={limit}...")

                preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                    "uri": TEST_SHEET_URI,
                    "limit": limit
                })

                if preview_res.isError:
                    print(f"   ❌ Failed: {preview_res.content[0].text if preview_res.content else 'Unknown'}")
                    continue

                content = json.loads(preview_res.content[0].text)
                if not content.get('success'):
                    print(f"   ❌ Failed: {content.get('message')}")
                    continue

                data = content.get('data', [])
                source_info = content.get('source_info', {})
                reported_limit = source_info.get('preview_limit')

                print(f"   ✅ Success: Returned {len(data)} rows")
                print(f"      Requested limit: {limit}")
                print(f"      Reported limit: {reported_limit}")

                # Validate
                if len(data) <= limit:
                    print(f"      ✅ Rows ({len(data)}) <= limit ({limit})")
                else:
                    print(f"      ❌ Rows ({len(data)}) > limit ({limit})")

                if reported_limit == limit:
                    print(f"      ✅ Reported limit matches requested")
                else:
                    print(f"      ⚠️  Reported limit ({reported_limit}) != requested ({limit})")

            print(f"\n{'='*60}")


async def test_preview_vs_full_load(url, headers):
    """Test 3: Compare preview vs full load (verify preview is faster/smaller)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 3: Preview vs Full Load Comparison")
    print(f"{'='*60}")
    print(f"Purpose: Verify preview returns fewer rows than full load")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: Full load
            print("\n📖 Step 1: Loading full worksheet with formulas...")
            full_res = await session.call_tool("read_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI
            })

            full_content = json.loads(full_res.content[0].text)
            full_data = full_content.get('data', []) if full_content.get('success') else []
            print(f"   ✅ Full load: {len(full_data)} rows")

            # Step 2: Preview load
            print("\n📋 Step 2: Loading preview (5 rows)...")
            preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
                "limit": 5
            })

            preview_content = json.loads(preview_res.content[0].text)
            preview_data = preview_content.get('data', []) if preview_content.get('success') else []
            print(f"   ✅ Preview load: {len(preview_data)} rows")

            # Step 3: Compare
            print(f"\n🔍 Comparison:")
            print("-" * 60)

            print(f"   Full load rows: {len(full_data)}")
            print(f"   Preview rows: {len(preview_data)}")

            if len(preview_data) < len(full_data):
                print(f"   ✅ PASS: Preview ({len(preview_data)}) < Full ({len(full_data)})")
            elif len(full_data) <= 5:
                print(f"   ⚠️  WARNING: Sheet has <= 5 rows, preview same as full")
            else:
                print(f"   ❌ FAIL: Preview should be smaller than full load")

            # Compare first row (should be identical)
            if full_data and preview_data:
                print(f"\n   📊 First row comparison:")
                first_full = full_data[0]
                first_preview = preview_data[0]

                if first_full == first_preview:
                    print(f"      ✅ First rows are identical")
                else:
                    print(f"      ❌ First rows differ")
                    print(f"         Full: {first_full}")
                    print(f"         Preview: {first_preview}")

            print(f"\n{'='*60}")


async def test_formula_content(url, headers):
    """Test 4: Verify formulas are correctly returned"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 4: Formula Content Verification")
    print(f"{'='*60}")
    print(f"Purpose: Verify that formulas (starting with =) are returned")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("\n📋 Loading preview with formulas...")
            preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
                "limit": 10
            })

            content = json.loads(preview_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Failed: {content.get('message')}")
                return

            data = content.get('data', [])

            # Count formula cells
            formula_cells = []
            plain_cells = []

            for row_idx, row in enumerate(data, start=1):
                for col_name, cell_value in row.items():
                    if isinstance(cell_value, str) and cell_value.startswith("="):
                        formula_cells.append({
                            'row': row_idx,
                            'column': col_name,
                            'formula': cell_value
                        })
                    elif cell_value:  # Non-empty cell
                        plain_cells.append({
                            'row': row_idx,
                            'column': col_name,
                            'value': cell_value
                        })

            print(f"\n📊 Cell Analysis:")
            print("-" * 60)
            print(f"   Total rows: {len(data)}")
            print(f"   Formula cells: {len(formula_cells)}")
            print(f"   Plain value cells: {len(plain_cells)}")

            if formula_cells:
                print(f"\n   📝 Formula Examples (first 5):")
                for cell in formula_cells[:5]:
                    print(f"      Row {cell['row']}, {cell['column']}: {cell['formula']}")

                print(f"\n   ✅ PASS: Found {len(formula_cells)} formula cells")
            else:
                print(f"\n   ⚠️  WARNING: No formulas found in preview")
                print(f"   Note: This sheet may only contain static values")

            print(f"\n{'='*60}")


async def test_edge_cases(url, headers):
    """Test 5: Edge cases (limit=1, limit=100, invalid limit)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 5: Edge Cases")
    print(f"{'='*60}")
    print(f"Purpose: Test edge cases and boundary conditions")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: limit=1 (minimum)
            print(f"\n📋 Test 5a: limit=1 (minimum)...")
            preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
                "limit": 1
            })
            content = json.loads(preview_res.content[0].text)
            if content.get('success'):
                data = content.get('data', [])
                print(f"   ✅ Success: Returned {len(data)} row(s)")
                if len(data) == 1:
                    print(f"      ✅ Exactly 1 row returned")
                else:
                    print(f"      ⚠️  Expected 1 row, got {len(data)}")
            else:
                print(f"   ❌ Failed: {content.get('message')}")

            # Test 2: limit=100 (maximum)
            print(f"\n📋 Test 5b: limit=100 (maximum)...")
            preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
                "limit": 100
            })
            content = json.loads(preview_res.content[0].text)
            if content.get('success'):
                data = content.get('data', [])
                print(f"   ✅ Success: Returned {len(data)} row(s)")
                if len(data) <= 100:
                    print(f"      ✅ Rows <= 100")
                else:
                    print(f"      ❌ Returned more than 100 rows")
            else:
                print(f"   ❌ Failed: {content.get('message')}")

            # Test 3: limit=0 (should be capped to 1)
            print(f"\n📋 Test 5c: limit=0 (should be capped to 1)...")
            preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
                "limit": 0
            })
            content = json.loads(preview_res.content[0].text)
            if content.get('success'):
                data = content.get('data', [])
                source_info = content.get('source_info', {})
                reported_limit = source_info.get('preview_limit')
                print(f"   ✅ Success: Returned {len(data)} row(s)")
                print(f"      Reported limit: {reported_limit} (should be 1)")
                if reported_limit == 1:
                    print(f"      ✅ Limit correctly capped to 1")
                else:
                    print(f"      ⚠️  Expected limit 1, got {reported_limit}")
            else:
                print(f"   ❌ Failed: {content.get('message')}")

            # Test 4: limit=200 (should be capped to 100)
            print(f"\n📋 Test 5d: limit=200 (should be capped to 100)...")
            preview_res = await session.call_tool("preview_worksheet_with_formulas", {
                "uri": TEST_SHEET_URI,
                "limit": 200
            })
            content = json.loads(preview_res.content[0].text)
            if content.get('success'):
                data = content.get('data', [])
                source_info = content.get('source_info', {})
                reported_limit = source_info.get('preview_limit')
                print(f"   ✅ Success: Returned {len(data)} row(s)")
                print(f"      Reported limit: {reported_limit} (should be 100)")
                if reported_limit == 100:
                    print(f"      ✅ Limit correctly capped to 100")
                else:
                    print(f"      ⚠️  Expected limit 100, got {reported_limit}")
            else:
                print(f"   ❌ Failed: {content.get('message')}")

            print(f"\n{'='*60}")


async def run_all_tests(url, headers):
    """Run all tests"""
    print(f"\n{'#'*60}")
    print(f"# Running All preview_worksheet_with_formulas Tests")
    print(f"# Test Sheet: {TEST_SHEET_URI}")
    print(f"{'#'*60}")

    try:
        await test_basic_preview(url, headers)
        await test_custom_limits(url, headers)
        await test_preview_vs_full_load(url, headers)
        await test_formula_content(url, headers)
        await test_edge_cases(url, headers)

        print(f"\n{'#'*60}")
        print(f"# ✅ All Tests Completed!")
        print(f"{'#'*60}")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(description='Test preview_worksheet_with_formulas functionality')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against')
    parser.add_argument('--test', choices=['all', 'basic', 'limits', 'compare', 'formulas', 'edge'],
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

    # Determine URL based on environment (must include /mcp endpoint)
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
    print(f"   Test Sheet: {TEST_SHEET_URI}")

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": refresh_token,
        "GOOGLE_OAUTH_CLIENT_ID": client_id,
        "GOOGLE_OAUTH_CLIENT_SECRET": client_secret,
    }

    # Run selected test
    test_map = {
        'all': run_all_tests,
        'basic': test_basic_preview,
        'limits': test_custom_limits,
        'compare': test_preview_vs_full_load,
        'formulas': test_formula_content,
        'edge': test_edge_cases,
    }

    asyncio.run(test_map[args.test](url, headers))


if __name__ == "__main__":
    main()
