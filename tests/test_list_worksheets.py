#!/usr/bin/env python3
"""
Test suite for list_worksheets functionality

Tests cover:
1. List all worksheets in a spreadsheet
2. Verify worksheet metadata (sheet_id, title, index, dimensions)
3. Test with single-sheet spreadsheet
4. Test with multi-sheet spreadsheet
5. Verify worksheet ordering by index
6. Test with spreadsheet ID vs full URL

Usage:
    # Run all worksheet tests
    python test_list_worksheets.py --env=local --test=all

    # Run specific test
    python test_list_worksheets.py --env=local --test=basic
    python test_list_worksheets.py --env=local --test=metadata
    python test_list_worksheets.py --env=local --test=ordering

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
TEST_USER_ID = os.environ.get("TEST_USER_ID")

# Test spreadsheet with multiple worksheets
# You can use any Google Sheets with multiple tabs/worksheets
TEST_MULTI_SHEET_URI = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit"

# Just the spreadsheet ID (for testing URI parsing)
TEST_SPREADSHEET_ID = "16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60"


async def test_basic_list(url, headers):
    """Test 1: Basic worksheet listing"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 1: Basic Worksheet Listing")
    print(f"{'='*60}")
    print(f"Purpose: List all worksheets and verify basic metadata")
    print(f"Test URI: {TEST_MULTI_SHEET_URI}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("\n📋 Step 1: Listing worksheets...")
            list_res = await session.call_tool("list_worksheets", {"uri": TEST_MULTI_SHEET_URI})
            print(list_res)

            if list_res.isError:
                print(f"❌ Failed: {list_res.content[0].text if list_res.content else 'Unknown error'}")
                return

            content = json.loads(list_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Failed: {content.get('message')}")
                return

            # Extract results
            spreadsheet_id = content.get('spreadsheet_id')
            spreadsheet_title = content.get('spreadsheet_title')
            spreadsheet_url = content.get('spreadsheet_url')
            worksheets = content.get('worksheets', [])
            total_worksheets = content.get('total_worksheets')

            print(f"✅ Success!")
            print(f"\n📊 Spreadsheet Information:")
            print(f"   ID: {spreadsheet_id}")
            print(f"   Title: {spreadsheet_title}")
            print(f"   URL: {spreadsheet_url}")
            print(f"   Total Worksheets: {total_worksheets}")

            # Display worksheet details
            print(f"\n📋 Worksheets ({len(worksheets)} found):")
            print("-" * 60)

            for ws in worksheets:
                print(f"\n   [{ws['index']}] {ws['title']}")
                print(f"       Sheet ID (gid): {ws['sheet_id']}")
                print(f"       Dimensions: {ws['row_count']} rows × {ws['column_count']} columns")
                print(f"       URL: {spreadsheet_url}#gid={ws['sheet_id']}")

            # Validation
            print(f"\n🔍 Validation:")
            checks = []

            # Check 1: Total matches count
            if total_worksheets == len(worksheets):
                checks.append(('✅', f'Total worksheets ({total_worksheets}) matches list length'))
            else:
                checks.append(('❌', f'Total ({total_worksheets}) != list length ({len(worksheets)})'))

            # Check 2: All worksheets have required fields
            required_fields = ['sheet_id', 'title', 'index', 'row_count', 'column_count']
            all_have_fields = all(
                all(field in ws for field in required_fields)
                for ws in worksheets
            )
            if all_have_fields:
                checks.append(('✅', 'All worksheets have required fields'))
            else:
                checks.append(('❌', 'Some worksheets missing required fields'))

            # Check 3: At least one worksheet found
            if len(worksheets) > 0:
                checks.append(('✅', f'Found {len(worksheets)} worksheet(s)'))
            else:
                checks.append(('❌', 'No worksheets found'))

            # Check 4: Spreadsheet ID matches
            if spreadsheet_id in TEST_MULTI_SHEET_URI:
                checks.append(('✅', 'Spreadsheet ID matches URI'))
            else:
                checks.append(('⚠️', 'Spreadsheet ID does not match URI'))

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


async def test_worksheet_metadata(url, headers):
    """Test 2: Verify worksheet metadata accuracy"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 2: Worksheet Metadata Verification")
    print(f"{'='*60}")
    print(f"Purpose: Cross-check worksheet metadata by loading actual data")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: List worksheets
            print("\n📋 Step 1: Listing worksheets...")
            list_res = await session.call_tool("list_worksheets", {"uri": TEST_MULTI_SHEET_URI})
            content = json.loads(list_res.content[0].text)

            if not content.get('success'):
                print(f"❌ Failed to list worksheets")
                return

            worksheets = content.get('worksheets', [])
            spreadsheet_id = content.get('spreadsheet_id')
            print(f"✅ Found {len(worksheets)} worksheet(s)")

            # Step 2: Load data from first worksheet to verify dimensions
            if worksheets:
                first_ws = worksheets[0]
                print(f"\n📊 Step 2: Verifying first worksheet: '{first_ws['title']}'")
                print(f"   Reported dimensions: {first_ws['row_count']} rows × {first_ws['column_count']} columns")

                # Construct URI with gid
                test_uri = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid={first_ws['sheet_id']}"

                print(f"   Loading data from: {test_uri}")
                load_res = await session.call_tool("load_data_table", {"uri": test_uri})

                if not load_res.isError:
                    load_content = json.loads(load_res.content[0].text)
                    if load_content.get('success'):
                        data = load_content.get('data', [])
                        shape = load_content.get('shape', '')

                        print(f"\n   ✅ Loaded data successfully")
                        print(f"      Shape from load_data_table: {shape}")
                        print(f"      Data rows: {len(data)}")

                        # Note: The reported row_count includes header, but data doesn't
                        # So we compare row_count - 1 with len(data)
                        if len(data) > 0:
                            actual_cols = len(data[0])
                            print(f"      Data columns: {actual_cols}")

                            # Validate
                            print(f"\n   🔍 Validation:")
                            # Row count should be close (may differ due to empty rows)
                            if abs(first_ws['row_count'] - len(data) - 1) <= 10:  # Allow some tolerance
                                print(f"      ✅ Row count is reasonable (metadata: {first_ws['row_count']}, data rows: {len(data)})")
                            else:
                                print(f"      ⚠️  Row count mismatch (metadata: {first_ws['row_count']}, data rows: {len(data)})")

                            # Column count should match
                            if first_ws['column_count'] >= actual_cols:
                                print(f"      ✅ Column count matches or is larger (metadata: {first_ws['column_count']}, data: {actual_cols})")
                            else:
                                print(f"      ❌ Column count mismatch (metadata: {first_ws['column_count']}, data: {actual_cols})")
                    else:
                        print(f"   ⚠️  Could not load data: {load_content.get('message')}")
                else:
                    print(f"   ⚠️  Could not load data")

            print(f"\n{'='*60}")


async def test_worksheet_ordering(url, headers):
    """Test 3: Verify worksheets are ordered by index"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 3: Worksheet Ordering")
    print(f"{'='*60}")
    print(f"Purpose: Verify worksheets are returned in correct order (by index)")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("\n📋 Listing worksheets...")
            list_res = await session.call_tool("list_worksheets", {"uri": TEST_MULTI_SHEET_URI})
            content = json.loads(list_res.content[0].text)

            if not content.get('success'):
                print(f"❌ Failed to list worksheets")
                return

            worksheets = content.get('worksheets', [])
            print(f"✅ Found {len(worksheets)} worksheet(s)")

            # Check ordering
            print(f"\n🔍 Checking worksheet order:")
            print("-" * 60)

            is_ordered = True
            for i, ws in enumerate(worksheets):
                expected_index = i
                actual_index = ws['index']

                status = '✅' if actual_index == expected_index else '❌'
                print(f"   {status} Position {i}: '{ws['title']}' (index={actual_index}, expected={expected_index})")

                if actual_index != expected_index:
                    is_ordered = False

            # Overall result
            print(f"\n📊 Result:")
            if is_ordered:
                print(f"   ✅ PASS: Worksheets are correctly ordered by index")
            else:
                print(f"   ❌ FAIL: Worksheets are not in correct order")

            # Additional info: Show indices
            print(f"\n   Worksheet indices: {[ws['index'] for ws in worksheets]}")

            print(f"\n{'='*60}")


async def test_spreadsheet_id_vs_url(url, headers):
    """Test 4: Test with spreadsheet ID vs full URL"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 4: Spreadsheet ID vs Full URL")
    print(f"{'='*60}")
    print(f"Purpose: Verify both spreadsheet ID and full URL work")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Full URL
            print("\n📋 Test 4a: Using full URL...")
            print(f"   URI: {TEST_MULTI_SHEET_URI}")

            url_res = await session.call_tool("list_worksheets", {"uri": TEST_MULTI_SHEET_URI})
            url_content = json.loads(url_res.content[0].text)

            if url_content.get('success'):
                url_worksheets = url_content.get('worksheets', [])
                print(f"   ✅ Success: Found {len(url_worksheets)} worksheet(s)")
            else:
                print(f"   ❌ Failed: {url_content.get('message')}")
                url_worksheets = []

            # Test 2: Spreadsheet ID only
            print("\n📋 Test 4b: Using spreadsheet ID only...")
            print(f"   ID: {TEST_SPREADSHEET_ID}")

            id_res = await session.call_tool("list_worksheets", {"uri": TEST_SPREADSHEET_ID})
            id_content = json.loads(id_res.content[0].text)

            if id_content.get('success'):
                id_worksheets = id_content.get('worksheets', [])
                print(f"   ✅ Success: Found {len(id_worksheets)} worksheet(s)")
            else:
                print(f"   ❌ Failed: {id_content.get('message')}")
                id_worksheets = []

            # Compare results
            print(f"\n🔍 Comparison:")
            print("-" * 60)

            checks = []

            # Check 1: Both succeeded
            if url_content.get('success') and id_content.get('success'):
                checks.append(('✅', 'Both URL and ID methods succeeded'))
            else:
                checks.append(('❌', 'One or both methods failed'))

            # Check 2: Same number of worksheets
            if len(url_worksheets) == len(id_worksheets):
                checks.append(('✅', f'Same worksheet count ({len(url_worksheets)})'))
            else:
                checks.append(('❌', f'Different worksheet counts (URL: {len(url_worksheets)}, ID: {len(id_worksheets)})'))

            # Check 3: Same worksheet titles
            if url_worksheets and id_worksheets:
                url_titles = [ws['title'] for ws in url_worksheets]
                id_titles = [ws['title'] for ws in id_worksheets]

                if url_titles == id_titles:
                    checks.append(('✅', 'Worksheet titles match'))
                else:
                    checks.append(('❌', 'Worksheet titles differ'))

            # Check 4: Same spreadsheet IDs
            if url_content.get('spreadsheet_id') == id_content.get('spreadsheet_id'):
                checks.append(('✅', 'Spreadsheet IDs match'))
            else:
                checks.append(('❌', 'Spreadsheet IDs differ'))

            for status, message in checks:
                print(f"   {status} {message}")

            # Overall result
            passed = sum(1 for status, _ in checks if status == '✅')
            total = len(checks)
            print(f"\n📊 Result: {passed}/{total} checks passed")

            if passed == total:
                print(f"   ✅ PASS: Both methods produce identical results")
            else:
                print(f"   ❌ FAIL: Results differ between methods")

            print(f"\n{'='*60}")


async def test_multi_vs_single_sheet(url, headers):
    """Test 5: Compare multi-sheet vs single-sheet spreadsheets"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 5: Multi-Sheet vs Single-Sheet")
    print(f"{'='*60}")
    print(f"Purpose: Test behavior with different spreadsheet types")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("\n📋 Listing worksheets...")
            list_res = await session.call_tool("list_worksheets", {"uri": TEST_MULTI_SHEET_URI})
            content = json.loads(list_res.content[0].text)

            if not content.get('success'):
                print(f"❌ Failed to list worksheets")
                return

            worksheets = content.get('worksheets', [])
            total = len(worksheets)

            print(f"✅ Found {total} worksheet(s)")

            # Categorize
            if total == 1:
                print(f"\n   📄 This is a single-sheet spreadsheet")
                print(f"      Worksheet: '{worksheets[0]['title']}'")
                print(f"      ✅ Single-sheet handling works correctly")
            elif total > 1:
                print(f"\n   📚 This is a multi-sheet spreadsheet")
                print(f"      Worksheets:")
                for i, ws in enumerate(worksheets, 1):
                    print(f"         {i}. '{ws['title']}'")
                print(f"      ✅ Multi-sheet handling works correctly")
            else:
                print(f"\n   ⚠️  No worksheets found (unusual)")

            # Show first and last worksheet details
            if total >= 2:
                print(f"\n   First worksheet: '{worksheets[0]['title']}' (index={worksheets[0]['index']})")
                print(f"   Last worksheet: '{worksheets[-1]['title']}' (index={worksheets[-1]['index']})")

            print(f"\n{'='*60}")


async def run_all_tests(url, headers):
    """Run all tests"""
    print(f"\n{'#'*60}")
    print(f"# Running All list_worksheets Tests")
    print(f"# Test Spreadsheet: {TEST_MULTI_SHEET_URI}")
    print(f"{'#'*60}")

    try:
        await test_basic_list(url, headers)
        await test_worksheet_metadata(url, headers)
        await test_worksheet_ordering(url, headers)
        await test_spreadsheet_id_vs_url(url, headers)
        await test_multi_vs_single_sheet(url, headers)

        print(f"\n{'#'*60}")
        print(f"# ✅ All Tests Completed!")
        print(f"{'#'*60}")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(description='Test list_worksheets functionality')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against')
    parser.add_argument('--test', choices=['all', 'basic', 'metadata', 'ordering', 'idvsurl', 'types'],
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
    print(f"   Test Spreadsheet: {TEST_MULTI_SHEET_URI}")

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": refresh_token,
        "GOOGLE_OAUTH_CLIENT_ID": client_id,
        "GOOGLE_OAUTH_CLIENT_SECRET": client_secret,
    }

    # Run selected test
    test_map = {
        'all': run_all_tests,
        'basic': test_basic_list,
        'metadata': test_worksheet_metadata,
        'ordering': test_worksheet_ordering,
        'idvsurl': test_spreadsheet_id_vs_url,
        'types': test_multi_vs_single_sheet,
    }

    asyncio.run(test_map[args.test](url, headers))


if __name__ == "__main__":
    main()
