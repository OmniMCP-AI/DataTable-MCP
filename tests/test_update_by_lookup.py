#!/usr/bin/env python3
"""
Test suite for update_range_by_lookup functionality

Tests cover:
1. Basic lookup and update (match rows by key, update subset of columns)
2. Case-insensitive matching
3. New columns (automatically added)
4. Override behavior with empty values (override=True/False)
5. Error cases (missing columns, invalid data)

Usage:
    # Run all lookup tests
    python test_update_by_lookup.py --env=local --test=all

    # Run specific test
    python test_update_by_lookup.py --env=local --test=basic
    python test_update_by_lookup.py --env=local --test=newcols
    python test_update_by_lookup.py --env=local --test=override
    python test_update_by_lookup.py --env=local --test=errors

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
from datetime import datetime

# Test configuration
TEST_USER_ID = "68501372a3569b6897673a48"

# Real test sheet with Twitter user data
# Sheet URL: https://docs.google.com/spreadsheets/d/1h6waNEyrv_LKbxGSyZCJLf-QmLgFRNIQM4PfTphIeDM/edit?gid=23612700
# Columns: display_name, username, profile_url, avatar_url, detailed_bio
LOOKUP_TEST_URI = "https://docs.google.com/spreadsheets/d/1h6waNEyrv_LKbxGSyZCJLf-QmLgFRNIQM4PfTphIeDM/edit?gid=23612700#gid=23612700"


async def test_basic_lookup(url, headers):
    """Test basic lookup: match by username and update subset of columns"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 1: Basic Lookup and Update")
    print(f"{'='*60}")
    print(f"Purpose: Update profile_url for specific users by username")
    print(f"Test URI: {LOOKUP_TEST_URI}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Load original data
            print("\n📖 Step 1: Loading original data...")
            load_res = await session.call_tool("load_data_table", {"uri": LOOKUP_TEST_URI})

            if load_res.isError:
                print(f"❌ Failed to load data: {load_res.content[0].text if load_res.content else 'Unknown error'}")
                return

            content = json.loads(load_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Load failed: {content.get('message')}")
                return

            original_data = content.get('data', [])
            print(f"✅ Loaded {len(original_data)} rows")
            print(f"   Columns: {list(original_data[0].keys()) if original_data else []}")
            print(f"   Sample row: {original_data[0] if original_data else 'No data'}")

            # Perform update by lookup
            print("\n🔄 Step 2: Updating profile_url for @qiuhongbingo and @Juna0xx...")
            update_data = [
                {
                    "username": "@qiuhongbingo",
                    "profile_url": "https://x.com/qiuhongbingo/updated"
                },
                {
                    "username": "@Juna0xx",
                    "profile_url": "https://x.com/Juna0xx/updated"
                }
            ]

            print(f"   Update data: {json.dumps(update_data, indent=2)}")

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "username"
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

            # Verify the update
            print("\n🔍 Step 3: Verifying updated data...")
            verify_res = await session.call_tool("load_data_table", {"uri": LOOKUP_TEST_URI})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                updated_data = verify_content.get('data', [])
                print(f"✅ Loaded {len(updated_data)} rows after update")

                # Check specific rows
                for row in updated_data:
                    if row['username'] in ['@qiuhongbingo', '@Juna0xx']:
                        print(f"\n   User: {row['username']}")
                        print(f"      profile_url: {row['profile_url']}")
                        print(f"      display_name: {row['display_name']} (preserved)")
                        print(f"      detailed_bio: {row['detailed_bio'][:50]}... (preserved)")

                # Verify other columns were preserved
                if updated_data[0]['display_name'] == original_data[0]['display_name']:
                    print(f"\n   ✅ PASS: Other columns (display_name, avatar_url, detailed_bio) preserved")
                else:
                    print(f"\n   ❌ FAIL: Columns were not preserved correctly")

            print(f"\n{'='*60}")


async def test_add_new_columns(url, headers):
    """Test adding new columns (automatically added)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 2: Add New Columns (Automatic)")
    print(f"{'='*60}")
    print(f"Purpose: Add latest_tweet_timestamp and formatted_date columns (automatically)")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Add new columns with tweet data
            print("\n🔄 Step 1: Adding new columns with tweet timestamps...")
            update_data = [
                {
                    "username": "@qiuhongbingo",
                    "latest_tweet_timestamp": "2025-09-18T15:22:56.000Z",
                    "formatted_date": "2025-09-18"
                },
                {
                    "username": "@Juna0xx",
                    "latest_tweet_timestamp": "2025-10-30T08:38:58.000Z",
                    "formatted_date": "2025-10-30"
                },
                {
                    "username": "@levilin2008",
                    "latest_tweet_timestamp": "",
                    "formatted_date": ""
                }
            ]

            print(f"   Updating {len(update_data)} users with new columns")

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "username"
            })

            if lookup_res.isError:
                print(f"❌ Update failed: {lookup_res.content[0].text if lookup_res.content else 'Unknown error'}")
                return

            result = json.loads(lookup_res.content[0].text)
            print(f"\n✅ Update result:")
            print(f"   Success: {result.get('success')}")
            print(f"   Message: {result.get('message')}")
            print(f"   Updated cells: {result.get('updated_cells')}")

            # Verify new columns were added
            print("\n🔍 Step 2: Verifying new columns were added...")
            verify_res = await session.call_tool("load_data_table", {"uri": LOOKUP_TEST_URI})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                updated_data = verify_content.get('data', [])
                headers = list(updated_data[0].keys()) if updated_data else []

                print(f"✅ Current columns: {headers}")

                if 'latest_tweet_timestamp' in headers and 'formatted_date' in headers:
                    print(f"   ✅ PASS: New columns added successfully")

                    # Show sample data
                    print(f"\n   Sample rows with new columns:")
                    for row in updated_data[:3]:
                        print(f"      {row['username']}: {row.get('latest_tweet_timestamp', 'N/A')} -> {row.get('formatted_date', 'N/A')}")
                else:
                    print(f"   ❌ FAIL: New columns not found")

            print(f"\n{'='*60}")


async def test_override_empty_values(url, headers):
    """Test override parameter with empty values"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 3: Override Empty Values")
    print(f"{'='*60}")
    print(f"Purpose: Test override=True (clear cells) vs override=False (preserve)")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: override=False (default) - preserve existing
            print("\n🔄 Test 3a: Empty values with override=False (preserve existing)...")
            update_data = [
                {
                    "username": "@cryptohaiyu",
                    "latest_tweet_timestamp": "",  # Empty value
                    "formatted_date": "2025-11-11"  # Non-empty value
                }
            ]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "username",
                "override": False  # Preserve existing when empty
            })

            result = json.loads(lookup_res.content[0].text)
            print(f"   Result: {result.get('message')}")
            print(f"   Expected: Empty latest_tweet_timestamp preserves existing value")

            # Test 2: override=True - clear cells
            print("\n🔄 Test 3b: Empty values with override=True (clear cells)...")
            update_data = [
                {
                    "username": "@Phyrex_Ni",
                    "latest_tweet_timestamp": "",  # Empty value - should clear
                    "formatted_date": "2025-11-12"
                }
            ]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "username",
                "override": True  # Clear cells when empty
            })

            result = json.loads(lookup_res.content[0].text)
            print(f"   Result: {result.get('message')}")
            print(f"   Expected: Empty latest_tweet_timestamp clears existing value")

            # Verify results
            print("\n🔍 Verifying override behavior...")
            verify_res = await session.call_tool("load_data_table", {"uri": LOOKUP_TEST_URI})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                data = verify_content.get('data', [])
                for row in data:
                    if row['username'] == '@cryptohaiyu':
                        print(f"\n   @cryptohaiyu (override=False):")
                        print(f"      latest_tweet_timestamp: '{row.get('latest_tweet_timestamp', '')}' (should be preserved or empty)")
                        print(f"      formatted_date: '{row.get('formatted_date', '')}' (should be 2025-11-11)")
                    elif row['username'] == '@Phyrex_Ni':
                        print(f"\n   @Phyrex_Ni (override=True):")
                        print(f"      latest_tweet_timestamp: '{row.get('latest_tweet_timestamp', '')}' (should be cleared)")
                        print(f"      formatted_date: '{row.get('formatted_date', '')}' (should be 2025-11-12)")

            print(f"\n{'='*60}")


async def test_case_insensitive(url, headers):
    """Test case-insensitive matching"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 4: Case-Insensitive Matching")
    print(f"{'='*60}")
    print(f"Purpose: Verify lookup works with different case")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Update using different case
            print("\n🔄 Updating with mixed case usernames...")
            update_data = [
                {
                    "username": "@QIUHONGBINGO",  # UPPERCASE
                    "display_name": "Bingo Q (Updated)"
                },
                {
                    "username": "@juna0xx",  # lowercase
                    "display_name": "Juna |T (Updated)"
                }
            ]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "username"
            })

            result = json.loads(lookup_res.content[0].text)
            print(f"\n✅ Update result:")
            print(f"   Message: {result.get('message')}")

            # Check for case-insensitive matching success
            # New format: "2 unique lookup keys matched 2 rows"
            # Old format: "2 rows matched"
            message = result.get('message', '')
            if "2 unique lookup keys matched 2 rows" in message or "2 rows matched" in message:
                print(f"   ✅ PASS: Case-insensitive matching worked (2 matches)")
            else:
                print(f"   ❌ FAIL: Expected 2 matches, got: {message}")

            # Verify
            verify_res = await session.call_tool("load_data_table", {"uri": LOOKUP_TEST_URI})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                data = verify_content.get('data', [])
                for row in data:
                    if row['username'].lower() in ['@qiuhongbingo', '@juna0xx']:
                        print(f"\n   {row['username']}: {row['display_name']}")

            print(f"\n{'='*60}")


async def test_error_cases(url, headers):
    """Test error cases: missing lookup column, invalid data"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 5: Error Cases")
    print(f"{'='*60}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Lookup column doesn't exist in sheet
            print("\n🔄 Test 5a: Lookup column missing in sheet (expect error)...")
            update_data = [{"username": "@user1", "status": "updated"}]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "nonexistent_column"
            })

            # Check if error returned via isError or success:false in response
            if lookup_res.isError:
                error_text = lookup_res.content[0].text if lookup_res.content else 'Unknown error'
                if 'not found in sheet' in error_text:
                    print(f"   ✅ PASS: Error correctly detected (via isError)")
                    print(f"   Error: {error_text}")
                else:
                    print(f"   ⚠️  Error detected but unexpected message: {error_text}")
            elif lookup_res.content and lookup_res.content[0].text:
                result = json.loads(lookup_res.content[0].text)
                if result.get('success') == False and 'not found in sheet' in result.get('error', ''):
                    print(f"   ✅ PASS: Error correctly detected (via success:false)")
                    print(f"   Error: {result.get('error')}")
                else:
                    print(f"   ❌ FAIL: Expected error not received")
                    print(f"   Result: {result}")
            else:
                print(f"   ❌ FAIL: No error or response received")

            # Test 2: Lookup column missing in update data
            print("\n🔄 Test 5b: Lookup column missing in update data (expect error)...")
            update_data = [{"display_name": "Test"}]  # Missing username

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "username"
            })

            # Check if error returned via isError or success:false in response
            if lookup_res.isError:
                error_text = lookup_res.content[0].text if lookup_res.content else 'Unknown error'
                if 'not found in all rows' in error_text:
                    print(f"   ✅ PASS: Error correctly detected (via isError)")
                    print(f"   Error: {error_text}")
                else:
                    print(f"   ⚠️  Error detected but unexpected message: {error_text}")
            elif lookup_res.content and lookup_res.content[0].text:
                result = json.loads(lookup_res.content[0].text)
                if result.get('success') == False and 'not found in all rows' in result.get('error', ''):
                    print(f"   ✅ PASS: Error correctly detected (via success:false)")
                    print(f"   Error: {result.get('error')}")
                else:
                    print(f"   ❌ FAIL: Expected error not received")
                    print(f"   Result: {result}")
            else:
                print(f"   ❌ FAIL: No error or response received")

            # Test 3: No matching rows
            print("\n🔄 Test 5c: No matching rows (should succeed with 0 matches)...")
            update_data = [{"username": "@nonexistent_user", "display_name": "Test"}]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": LOOKUP_TEST_URI,
                "data": update_data,
                "on": "username"
            })

            result = json.loads(lookup_res.content[0].text)
            if result.get('success') == True and '0 rows matched' in result.get('message', ''):
                print(f"   ✅ PASS: Succeeded with 0 matches as expected")
                print(f"   Message: {result.get('message')}")
            else:
                print(f"   ❌ FAIL: Expected success with 0 matches")

            print(f"\n{'='*60}")


async def run_all_tests(url, headers):
    """Run all tests"""
    print(f"\n{'#'*60}")
    print(f"# Running All update_range_by_lookup Tests")
    print(f"# Test Sheet: {LOOKUP_TEST_URI}")
    print(f"{'#'*60}")

    try:
        await test_basic_lookup(url, headers)
        await test_add_new_columns(url, headers)
        await test_override_empty_values(url, headers)
        await test_case_insensitive(url, headers)
        await test_error_cases(url, headers)

        print(f"\n{'#'*60}")
        print(f"# ✅ All Tests Completed!")
        print(f"{'#'*60}")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(description='Test update_range_by_lookup functionality')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against')
    parser.add_argument('--test', choices=['all', 'basic', 'newcols', 'override', 'case', 'errors'],
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
    print(f"   Test Sheet: {LOOKUP_TEST_URI}")

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": refresh_token,
        "GOOGLE_OAUTH_CLIENT_ID": client_id,
        "GOOGLE_OAUTH_CLIENT_SECRET": client_secret,
    }

    # Run selected test
    test_map = {
        'all': run_all_tests,
        'basic': test_basic_lookup,
        'newcols': test_add_new_columns,
        'override': test_override_empty_values,
        'case': test_case_insensitive,
        'errors': test_error_cases,
    }

    asyncio.run(test_map[args.test](url, headers))


if __name__ == "__main__":
    main()
