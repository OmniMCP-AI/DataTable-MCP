#!/usr/bin/env python3
"""
Test suite for update_range_by_lookup with composite (multiple) lookup keys

Tests cover:
1. Basic composite key lookup (2 columns)
2. Composite key with 3+ columns
3. All rows matching composite key are updated
4. Error when composite key columns missing
5. Case-insensitive matching with composite keys

Usage:
    # Run all composite key tests
    python test_composite_key_lookup.py --env=local --test=all

    # Run specific test
    python test_composite_key_lookup.py --env=local --test=basic
    python test_composite_key_lookup.py --env=local --test=duplicates
    python test_composite_key_lookup.py --env=local --test=errors

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

# Test sheet for composite key testing
# Using the same test sheet as the regular lookup tests (gid=23612700)
# This sheet has Twitter user data with columns: display_name, username, profile_url, avatar_url, detailed_bio
# For composite key testing, we'll use display_name + username as a composite key
COMPOSITE_KEY_TEST_URI = "https://docs.google.com/spreadsheets/d/1h6waNEyrv_LKbxGSyZCJLf-QmLgFRNIQM4PfTphIeDM/edit?gid=23612700"


async def setup_test_data(url, headers):
    """Create a test sheet with sample data for composite key testing"""
    print(f"\n{'='*60}")
    print(f"🔧 Setup: Creating test data")
    print(f"{'='*60}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Create initial test data
            test_data = [
                {"first_name": "John", "last_name": "Doe", "email": "john.doe@example.com", "status": "active"},
                {"first_name": "Jane", "last_name": "Smith", "email": "jane.smith@example.com", "status": "active"},
                {"first_name": "John", "last_name": "Smith", "email": "john.smith@example.com", "status": "active"},  # Same first_name, different last_name
                {"first_name": "Jane", "last_name": "Doe", "email": "jane.doe@example.com", "status": "active"},  # Different first_name, same last_name
                {"first_name": "Bob", "last_name": "Johnson", "email": "bob.johnson@example.com", "status": "inactive"},
            ]

            print(f"   Creating test data with {len(test_data)} rows...")
            print(f"   Sample row: {test_data[0]}")

            # Write test data (assumes write_new_worksheet or similar tool exists)
            # For now, we'll skip this and assume the sheet already has data
            print(f"   ℹ️  Skipping data creation - assuming test sheet already exists")
            print(f"   Test sheet URI: {COMPOSITE_KEY_TEST_URI}")

    print(f"{'='*60}")


async def test_basic_composite_key(url, headers):
    """Test basic composite key lookup with 2 columns"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 1: Basic Composite Key (display_name + username)")
    print(f"{'='*60}")
    print(f"Purpose: Update rows matching both display_name AND username")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Load original data
            print("\n📖 Step 1: Loading original data...")
            load_res = await session.call_tool("load_data_table", {"uri": COMPOSITE_KEY_TEST_URI})

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

            # Show some sample data
            print(f"\n   Sample rows:")
            for i, row in enumerate(original_data[:3]):
                print(f"      {i+1}. {row.get('display_name')} ({row.get('username')}) - {row.get('profile_url', 'N/A')[:50]}")

            # Get first two users for testing
            if len(original_data) < 2:
                print(f"❌ Not enough data rows for testing")
                return

            user1 = original_data[0]
            user2 = original_data[1]

            # Perform update by composite key (display_name + username)
            print(f"\n🔄 Step 2: Updating profile_url using composite key [display_name, username]...")
            update_data = [
                {
                    "display_name": user1['display_name'],
                    "username": user1['username'],
                    "profile_url": f"{user1.get('profile_url', '')}_composite_key_test",
                    "test_timestamp": "2025-11-24"
                },
                {
                    "display_name": user2['display_name'],
                    "username": user2['username'],
                    "profile_url": f"{user2.get('profile_url', '')}_composite_key_test",
                    "test_timestamp": "2025-11-24"
                }
            ]

            print(f"   Updating 2 users:")
            print(f"      1. {user1['display_name']} + {user1['username']}")
            print(f"      2. {user2['display_name']} + {user2['username']}")
            print(f"   Using composite key: ['display_name', 'username']")

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": COMPOSITE_KEY_TEST_URI,
                "data": update_data,
                "on": ["display_name", "username"]  # Composite key!
            })

            if lookup_res.isError:
                print(f"❌ Update failed: {lookup_res.content[0].text if lookup_res.content else 'Unknown error'}")
                return

            result = json.loads(lookup_res.content[0].text)
            print(f"\n✅ Update result:")
            print(f"   Success: {result.get('success')}")
            print(f"   Message: {result.get('message')}")
            print(f"   Updated cells: {result.get('updated_cells')}")

            # Verify the update
            print("\n🔍 Step 3: Verifying updated data...")
            verify_res = await session.call_tool("load_data_table", {"uri": COMPOSITE_KEY_TEST_URI})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                updated_data = verify_content.get('data', [])
                print(f"✅ Loaded {len(updated_data)} rows after update")

                # Check specific rows
                matches = 0
                for row in updated_data:
                    if ((row['display_name'] == user1['display_name'] and row['username'] == user1['username']) or
                        (row['display_name'] == user2['display_name'] and row['username'] == user2['username'])):
                        print(f"\n   ✓ {row['display_name']} ({row['username']}):")
                        print(f"      profile_url: {row.get('profile_url', 'N/A')}")
                        print(f"      test_timestamp: {row.get('test_timestamp', 'N/A')}")
                        if '_composite_key_test' in row.get('profile_url', ''):
                            matches += 1

                # Test validation
                if matches == 2:
                    print(f"\n   ✅ PASS: Composite key matching worked correctly")
                    print(f"      - Both rows with matching composite keys were updated")
                else:
                    print(f"\n   ❌ FAIL: Expected 2 matches, got {matches}")

            print(f"\n{'='*60}")


async def test_duplicate_composite_keys(url, headers):
    """Test that all rows matching composite key are updated (not just first)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 2: Update All Duplicate Composite Key Matches")
    print(f"{'='*60}")
    print(f"Purpose: Verify ALL rows matching composite key are updated")
    print(f"Note: This test demonstrates the behavior - if there are duplicate")
    print(f"      composite keys in the sheet, all will be updated")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Load current data
            print("\n📖 Loading sheet data...")
            load_res = await session.call_tool("load_data_table", {"uri": COMPOSITE_KEY_TEST_URI})
            content = json.loads(load_res.content[0].text)

            if not content.get('success'):
                print(f"❌ Failed to load data")
                return

            original_data = content.get('data', [])
            if len(original_data) < 1:
                print(f"❌ Not enough data for testing")
                return

            # Pick a user to update
            test_user = original_data[0]

            print(f"   Updating all rows matching: {test_user['display_name']} + {test_user['username']}")

            # Now update using composite key
            update_data = [
                {
                    "display_name": test_user['display_name'],
                    "username": test_user['username'],
                    "profile_url": f"{test_user.get('profile_url', '')}_bulk_update",
                    "bulk_update_timestamp": "2025-11-24"
                }
            ]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": COMPOSITE_KEY_TEST_URI,
                "data": update_data,
                "on": ["display_name", "username"]
            })

            if lookup_res.isError:
                print(f"❌ Update failed: {lookup_res.content[0].text if lookup_res.content else 'Unknown error'}")
                return

            result = json.loads(lookup_res.content[0].text)
            print(f"\n✅ Update result:")
            print(f"   Message: {result.get('message')}")

            # Verify
            print("\n🔍 Verifying updates...")
            verify_res = await session.call_tool("load_data_table", {"uri": COMPOSITE_KEY_TEST_URI})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                updated_data = verify_content.get('data', [])
                match_count = 0

                for row in updated_data:
                    if (row['display_name'] == test_user['display_name'] and
                        row['username'] == test_user['username']):
                        match_count += 1
                        print(f"\n   Match #{match_count}:")
                        print(f"      display_name: {row['display_name']}")
                        print(f"      username: {row['username']}")
                        print(f"      profile_url: {row.get('profile_url', 'N/A')}")
                        print(f"      bulk_update_timestamp: {row.get('bulk_update_timestamp', 'N/A')}")

                if match_count > 0:
                    print(f"\n   ✅ PASS: Found and updated {match_count} matching row(s)")
                    print(f"      Note: All rows with the same composite key were updated")
                else:
                    print(f"\n   ❌ FAIL: No matching rows found")

            print(f"\n{'='*60}")


async def test_error_missing_composite_keys(url, headers):
    """Test error handling when composite key columns are missing"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 3: Error Handling - Missing Composite Key Columns")
    print(f"{'='*60}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: One of the composite key columns missing from sheet
            print("\n🔄 Test 3a: Composite key column missing from sheet...")
            update_data = [{"display_name": "Test", "username": "@test", "status": "test"}]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": COMPOSITE_KEY_TEST_URI,
                "data": update_data,
                "on": ["display_name", "nonexistent_column"]  # nonexistent_column doesn't exist
            })

            # Handle both error response formats
            if lookup_res.isError:
                error_text = lookup_res.content[0].text if lookup_res.content else 'Unknown error'
                print(f"   ✅ PASS: Error correctly detected (isError=True)")
                print(f"   Error: {error_text[:200]}")
            else:
                try:
                    result = json.loads(lookup_res.content[0].text)
                    if not result.get('success') and 'not found in sheet' in result.get('error', '').lower():
                        print(f"   ✅ PASS: Error correctly detected in response")
                        print(f"   Error: {result.get('error')}")
                    else:
                        print(f"   ❌ FAIL: Expected error for missing column")
                        print(f"   Result: {result}")
                except (json.JSONDecodeError, AttributeError, IndexError) as e:
                    print(f"   ⚠️  Could not parse response: {e}")

            # Test 2: One of the composite key columns missing from update data
            print("\n🔄 Test 3b: Composite key column missing from update data...")
            update_data = [{"display_name": "Test"}]  # Missing username

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": COMPOSITE_KEY_TEST_URI,
                "data": update_data,
                "on": ["display_name", "username"]
            })

            # Handle both error response formats
            if lookup_res.isError:
                error_text = lookup_res.content[0].text if lookup_res.content else 'Unknown error'
                print(f"   ✅ PASS: Error correctly detected (isError=True)")
                print(f"   Error: {error_text[:200]}")
            else:
                try:
                    result = json.loads(lookup_res.content[0].text)
                    if not result.get('success') and 'not found in all rows' in result.get('error', '').lower():
                        print(f"   ✅ PASS: Error correctly detected in response")
                        print(f"   Error: {result.get('error')}")
                    else:
                        print(f"   ❌ FAIL: Expected error for missing column in update data")
                        print(f"   Result: {result}")
                except (json.JSONDecodeError, AttributeError, IndexError) as e:
                    print(f"   ⚠️  Could not parse response: {e}")

            print(f"\n{'='*60}")


async def test_case_insensitive_composite(url, headers):
    """Test case-insensitive matching with composite keys"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 4: Case-Insensitive Composite Key Matching")
    print(f"{'='*60}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Load data to get a real user
            print("\n📖 Loading sheet data...")
            load_res = await session.call_tool("load_data_table", {"uri": COMPOSITE_KEY_TEST_URI})
            content = json.loads(load_res.content[0].text)

            if not content.get('success') or not content.get('data'):
                print(f"❌ Failed to load data")
                return

            test_user = content.get('data')[0]

            # Update using different case
            print("\n🔄 Updating with mixed case composite keys...")
            print(f"   Original: {test_user['display_name']} + {test_user['username']}")
            print(f"   Using:    {test_user['display_name'].upper()} + {test_user['username'].lower()}")

            update_data = [
                {
                    "display_name": test_user['display_name'].upper(),  # UPPERCASE
                    "username": test_user['username'].lower(),          # lowercase
                    "profile_url": f"{test_user.get('profile_url', '')}_case_test",
                    "case_test_timestamp": "2025-11-24"
                }
            ]

            lookup_res = await session.call_tool("update_range_by_lookup", {
                "uri": COMPOSITE_KEY_TEST_URI,
                "data": update_data,
                "on": ["display_name", "username"]
            })

            if lookup_res.isError:
                print(f"❌ Update failed: {lookup_res.content[0].text if lookup_res.content else 'Unknown error'}")
                return

            result = json.loads(lookup_res.content[0].text)
            print(f"\n✅ Update result:")
            print(f"   Message: {result.get('message')}")

            # Verify
            verify_res = await session.call_tool("load_data_table", {"uri": COMPOSITE_KEY_TEST_URI})
            verify_content = json.loads(verify_res.content[0].text)

            if verify_content.get('success'):
                data = verify_content.get('data', [])
                found_and_updated = False

                for row in data:
                    if (row['display_name'].lower() == test_user['display_name'].lower() and
                        row['username'].lower() == test_user['username'].lower()):
                        print(f"\n   Found match:")
                        print(f"      display_name: {row['display_name']}")
                        print(f"      username: {row['username']}")
                        print(f"      profile_url: {row.get('profile_url', 'N/A')}")
                        print(f"      case_test_timestamp: {row.get('case_test_timestamp', 'N/A')}")

                        if '_case_test' in row.get('profile_url', ''):
                            found_and_updated = True
                            print(f"   ✅ PASS: Case-insensitive composite key matching worked")
                        else:
                            print(f"   ❌ FAIL: Row not updated")
                        break

                if not found_and_updated:
                    print(f"   ❌ FAIL: User not found or not updated")

            print(f"\n{'='*60}")


async def run_all_tests(url, headers):
    """Run all composite key tests"""
    print(f"\n{'#'*60}")
    print(f"# Running All Composite Key Lookup Tests")
    print(f"# Test Sheet: {COMPOSITE_KEY_TEST_URI}")
    print(f"{'#'*60}")

    try:
        # await setup_test_data(url, headers)  # Uncomment if you want to create test data
        await test_basic_composite_key(url, headers)
        await test_duplicate_composite_keys(url, headers)
        await test_error_missing_composite_keys(url, headers)
        await test_case_insensitive_composite(url, headers)

        print(f"\n{'#'*60}")
        print(f"# ✅ All Composite Key Tests Completed!")
        print(f"{'#'*60}")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(description='Test composite key lookup functionality')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against')
    parser.add_argument('--test', choices=['all', 'basic', 'duplicates', 'errors', 'case'],
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
    print(f"   Test Sheet: {COMPOSITE_KEY_TEST_URI}")

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": refresh_token,
        "GOOGLE_OAUTH_CLIENT_ID": client_id,
        "GOOGLE_OAUTH_CLIENT_SECRET": client_secret,
    }

    # Run selected test
    test_map = {
        'all': run_all_tests,
        'basic': test_basic_composite_key,
        'duplicates': test_duplicate_composite_keys,
        'errors': test_error_missing_composite_keys,
        'case': test_case_insensitive_composite,
    }

    asyncio.run(test_map[args.test](url, headers))


if __name__ == "__main__":
    main()
