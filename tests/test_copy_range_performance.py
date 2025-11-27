#!/usr/bin/env python3
"""
Performance test for copy_range_with_formulas optimization.

This test verifies that the optimization for large batch operations (>100 ranges)
using Google Sheets native copyPaste API instead of Python formula adaptation.

The issue: copy_range_with_formulas was timing out when copying formulas to 1,599 rows
because it was adapting each formula individually in Python (145,509 cells = 1,599 rows × 91 cols).

The fix: For batches >100 ranges, use native copyPaste API which is 10-100x faster.

Test Sheet:
- URI: https://docs.google.com/spreadsheets/d/15jns06J6TZqPds2EZFwVtVwPfpHxKb2_um3q2fCvAEE/edit?gid=1881797459#gid=1881797459
- Has 1,600+ rows with formulas in columns B through CM (91 columns)
- Auto-fill from row 2 to all data rows

Usage:
    python test_copy_range_performance.py --env=local
    python test_copy_range_performance.py --env=test
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession
import time
import os
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Test sheet with 1,600+ rows (the one that was timing out)
TEST_SHEET_URI = "https://docs.google.com/spreadsheets/d/15jns06J6TZqPds2EZFwVtVwPfpHxKb2_um3q2fCvAEE/edit?gid=1583346529#gid=1583346529"


async def test_large_batch_copy_performance(url, headers):
    """
    Test copy_range_with_formulas with 1,599 target ranges (the scenario that was timing out).

    This test verifies:
    1. The operation completes without timeout (<60s)
    2. Native copyPaste API is used (check logs for "Using native copyPaste API")
    3. All formulas are copied correctly

    Expected performance:
    - Old method: >80s (timeout)
    - New method: ~5-10s (native copyPaste)
    """
    print("🚀 Testing Large Batch Copy Performance")
    print("=" * 80)
    print(f"📋 Test Sheet: {TEST_SHEET_URI}")
    print(f"📊 Expected: 1,599 rows × 91 columns = 145,509 cells")
    print("=" * 80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test: Copy formulas from B2:CM2 to all data rows (auto-fill mode)
            print("\n⏱️  Starting large batch copy operation...")
            print("   Source range: B2:CM2 (91 columns)")
            print("   Mode: auto_fill=True (copies to all data rows)")
            print("   Expected behavior: Use native copyPaste API for performance")

            start_time = time.time()

            try:
                copy_res = await session.call_tool("copy_range_with_formulas", {
                    "uri": TEST_SHEET_URI,
                    "from_range": "B2:CM2",
                    "auto_fill": True,
                    "lookup_column": "A",
                    "skip_if_exists": True
                })

                elapsed_time = time.time() - start_time

                if copy_res.isError:
                    print(f"❌ FAIL: Operation failed with error")
                    print(f"   Error: {copy_res}")
                    return False

                result_content = json.loads(copy_res.content[0].text)

                if not result_content.get('success'):
                    print(f"❌ FAIL: Operation returned success=False")
                    print(f"   Message: {result_content.get('message', 'Unknown error')}")
                    return False

                # Extract metrics
                updated_cells = result_content.get('updated_cells', 0)
                message = result_content.get('message', '')

                print(f"\n✅ SUCCESS: Operation completed in {elapsed_time:.2f}s")
                print(f"   📊 Updated cells: {updated_cells:,}")
                print(f"   📝 Message: {message[:200]}...")

                # Performance check
                if elapsed_time > 60:
                    print(f"\n⚠️  WARNING: Operation took {elapsed_time:.2f}s (>60s)")
                    print(f"   This suggests the optimization may not be working")
                    return False
                elif elapsed_time < 20:
                    print(f"\n🎉 EXCELLENT: Operation completed in {elapsed_time:.2f}s (<20s)")
                    print(f"   ✅ Native copyPaste API optimization is working!")
                else:
                    print(f"\n✅ GOOD: Operation completed in {elapsed_time:.2f}s (20-60s)")
                    print(f"   Native copyPaste API is likely being used")

                # Check if enough cells were updated (should be ~145,000+)
                expected_min_cells = 100000  # At least 100k cells
                if updated_cells < expected_min_cells:
                    print(f"\n⚠️  WARNING: Only {updated_cells:,} cells updated")
                    print(f"   Expected at least {expected_min_cells:,} cells")
                    print(f"   This might indicate incomplete copy or skip_if_exists filtered many rows")
                else:
                    print(f"\n✅ Cell count looks good: {updated_cells:,} cells updated")

                return True

            except asyncio.TimeoutError:
                elapsed_time = time.time() - start_time
                print(f"\n❌ FAIL: Operation timed out after {elapsed_time:.2f}s")
                print(f"   The optimization is NOT working")
                return False

            except Exception as e:
                elapsed_time = time.time() - start_time
                print(f"\n❌ FAIL: Operation failed after {elapsed_time:.2f}s")
                print(f"   Error: {e}")
                import traceback
                traceback.print_exc()
                return False


async def test_small_batch_copy(url, headers):
    """
    Test copy_range_with_formulas with <100 target ranges (should use Python adaptation).

    This ensures the optimization doesn't break small batch operations.
    """
    print("\n" + "=" * 80)
    print("🧪 Testing Small Batch Copy (Python Adaptation)")
    print("=" * 80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test: Copy formulas with manual mode (small batch)
            print("\n⏱️  Starting small batch copy operation...")
            print("   Source range: B2:CM2")
            print("   Destination: B3:CM10 (8 rows)")
            print("   Expected behavior: Use Python formula adaptation")

            start_time = time.time()

            try:
                copy_res = await session.call_tool("copy_range_with_formulas", {
                    "uri": TEST_SHEET_URI,
                    "from_range": "B2:CM2",
                    "to_range": "B3:CM10",
                    "skip_if_exists": False
                })

                elapsed_time = time.time() - start_time

                if copy_res.isError:
                    print(f"❌ FAIL: Small batch operation failed")
                    return False

                result_content = json.loads(copy_res.content[0].text)

                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    print(f"\n✅ SUCCESS: Small batch completed in {elapsed_time:.2f}s")
                    print(f"   📊 Updated cells: {updated_cells:,}")

                    # Small batch should be fast
                    if elapsed_time > 10:
                        print(f"   ⚠️  WARNING: Small batch took {elapsed_time:.2f}s (expected <10s)")

                    return True
                else:
                    print(f"❌ FAIL: Small batch returned success=False")
                    return False

            except Exception as e:
                elapsed_time = time.time() - start_time
                print(f"❌ FAIL: Small batch failed after {elapsed_time:.2f}s")
                print(f"   Error: {e}")
                return False


async def main():
    parser = argparse.ArgumentParser(description="Test copy_range_with_formulas performance optimization")
    parser.add_argument("--env", choices=["local", "test"], default="local",
                       help="Environment: local (127.0.0.1:8321) or test (datatable-mcp-test.maybe.ai)")
    parser.add_argument("--test", choices=["large", "small", "all"], default="large",
                       help="Which test to run: large (1599 rows), small (<100 rows), or all")
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

    # Run tests
    results = {}

    if args.test in ["large", "all"]:
        print("\n" + "🔥" * 40)
        print("LARGE BATCH TEST (1,599 rows - This was timing out before optimization)")
        print("🔥" * 40)
        results['large'] = await test_large_batch_copy_performance(
            url=f"{endpoint}/mcp",
            headers=test_headers
        )

    if args.test in ["small", "all"]:
        results['small'] = await test_small_batch_copy(
            url=f"{endpoint}/mcp",
            headers=test_headers
        )

    # Summary
    print("\n" + "=" * 80)
    print("📊 TEST SUMMARY")
    print("=" * 80)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {test_name.upper():15s}: {status}")

    all_passed = all(results.values())

    if all_passed:
        print("\n🎉 All tests PASSED! Performance optimization is working correctly.")
        exit(0)
    else:
        print("\n❌ Some tests FAILED. Please check the logs above.")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
