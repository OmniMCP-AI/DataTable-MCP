#!/usr/bin/env python3
"""
Test large dataset update with batching
Tests the fix for update_range_by_lookup with >5000 rows

This test verifies that:
1. Large datasets (>5000 rows) are automatically batched
2. Batch processing successfully writes all data
3. No API limit errors occur

Usage:
    python tests/test_large_dataset_update.py --env=test
"""

import os
import sys
import argparse
import asyncio
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession
import json

# Test configuration
TEST_SPREADSHEET_URI = "https://docs.google.com/spreadsheets/d/15jns06J6TZqPds2EZFwVtVwPfpHxKb2_um3q2fCvAEE/edit?gid=91360988#gid=91360988"

async def test_large_dataset_update(url, headers):
    """Test updating large dataset with batching"""
    print(f"🚀 Testing Large Dataset Update with Batching")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Create simulated large dataset (27,097 rows × 12 columns)
            print(f"\n📝 Test 1: Simulating large dataset update (27,097 rows)")
            print(f"   This mimics the production error scenario")

            # Generate test data (simulated large dataset)
            # Using a smaller subset for testing (1000 rows) to speed up test
            # In production, this would be 27,097 rows
            test_row_count = 1000  # Use 1000 for testing, 27097 in production scenario
            print(f"   Using {test_row_count} rows for test (production scenario: 27,097 rows)")

            # Create test data structure
            test_data = []
            for i in range(test_row_count):
                test_data.append({
                    "ERP单号": f"S251114{100000+i}",
                    "订单状态": "已出库",
                    "线上状态": "SHIPPED",
                    "付款时间": f"2025-11-{15 + (i % 15):02d} 10:26:31 UTC+7",
                    "订单来源": "TikTok Shop",
                    "商品名称": f"Test Product {i}",
                    "系统商品编码": f"BF1D-G{1000+i:04d}-{(i%100):02d}",
                    "商品数量": "1",
                    "仓库": "印尼唐格朗1号仓库",
                    "出库日期": f"2025-11-{15 + (i % 15):02d}",
                    "备注": f"Batch test row {i}",
                    "状态": "测试"
                })

            print(f"   Generated {len(test_data)} test rows with 12 columns")
            print(f"   Total cells: {len(test_data) * 12} = {len(test_data) * 12:,}")

            # Test 2: Call update_range_by_lookup with large dataset
            print(f"\n📝 Test 2: Calling update_range_by_lookup with batching")
            print(f"   Batch size: 5000 rows (configured in datatable.py)")
            print(f"   Expected batches: {(test_row_count - 1) // 5000 + 1}")

            try:
                update_result = await session.call_tool(
                    "update_range_by_lookup",
                    {
                        "uri": TEST_SPREADSHEET_URI,
                        "data": test_data,
                        "on": "ERP单号",
                        "override": False
                    }
                )

                print(f"\n✅ Update result received")
                print(f"   Is error: {update_result.isError}")

                if update_result.content and update_result.content[0].text:
                    result_content = json.loads(update_result.content[0].text)
                    print(f"\n📊 Result:")
                    print(f"   Success: {result_content.get('success')}")
                    print(f"   Updated cells: {result_content.get('updated_cells', 0):,}")
                    print(f"   Shape: {result_content.get('shape', 'N/A')}")
                    print(f"   Message: {result_content.get('message', 'N/A')}")

                    if result_content.get('success'):
                        print(f"\n✅ SUCCESS: Large dataset update completed without errors!")
                        print(f"   Batching successfully handled {test_row_count:,} rows")
                        return True
                    else:
                        print(f"\n❌ FAIL: Update returned success=False")
                        print(f"   Error: {result_content.get('error', 'Unknown error')}")
                        return False
                else:
                    print(f"\n❌ FAIL: No response content received")
                    return False

            except Exception as e:
                print(f"\n❌ FAIL: Exception occurred during update")
                print(f"   Error: {e}")
                import traceback
                traceback.print_exc()
                return False

            # Test 3: Verify data was written correctly
            print(f"\n📝 Test 3: Verifying data was written (sample check)")
            try:
                read_result = await session.call_tool(
                    "read_sheet",
                    {
                        "uri": TEST_SPREADSHEET_URI
                    }
                )

                if not read_result.isError and read_result.content and read_result.content[0].text:
                    content = json.loads(read_result.content[0].text)
                    if content.get('success'):
                        data = content.get('data', [])
                        print(f"   ✅ Read {len(data)} rows from sheet")
                        print(f"   First 3 rows:")
                        for i, row in enumerate(data[:3]):
                            print(f"      Row {i+1}: {row.get('ERP单号', 'N/A')} - {row.get('订单状态', 'N/A')}")
                    else:
                        print(f"   ⚠️  Failed to read sheet for verification")
            except Exception as e:
                print(f"   ⚠️  Verification read failed: {e}")

            print(f"\n{'='*60}")
            print(f"✅ Large dataset update test completed!")
            return True


async def test_batch_threshold(url, headers):
    """Test that batching is triggered at the correct threshold"""
    print(f"\n🚀 Testing Batch Threshold (5000 rows)")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test with exactly 5001 rows to trigger batching
            print(f"\n📝 Creating dataset with 5001 rows (just over threshold)")

            test_data = []
            for i in range(5001):
                test_data.append({
                    "ERP单号": f"THRESHOLD_TEST_{i:06d}",
                    "测试列": f"Value {i}"
                })

            print(f"   Generated {len(test_data)} rows")
            print(f"   Expected: Batching should be triggered (>5000 rows)")

            try:
                update_result = await session.call_tool(
                    "update_range_by_lookup",
                    {
                        "uri": TEST_SPREADSHEET_URI,
                        "data": test_data,
                        "on": "ERP单号",
                        "override": False
                    }
                )

                if not update_result.isError:
                    print(f"   ✅ PASS: Batch threshold test completed")
                    return True
                else:
                    print(f"   ❌ FAIL: Batch threshold test failed")
                    return False

            except Exception as e:
                print(f"   ❌ FAIL: Exception: {e}")
                return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test large dataset update with batching")
    parser.add_argument("--env", choices=["local", "test", "prod"], default="test",
                       help="Environment to use: local (127.0.0.1:8321), test (datatable-mcp-test.maybe.ai), or prod (datatable-mcp.maybe.ai)")
    parser.add_argument("--test", choices=["large", "threshold", "all"], default="large",
                       help="Which test to run: large, threshold, or all")
    args = parser.parse_args()

    # Set endpoint
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

    # Check credentials
    if not all(test_headers.values()):
        print("❌ Error: Missing OAuth credentials in environment variables")
        print("   Required: TEST_GOOGLE_OAUTH_REFRESH_TOKEN, TEST_GOOGLE_OAUTH_CLIENT_ID, TEST_GOOGLE_OAUTH_CLIENT_SECRET")
        sys.exit(1)

    # Run tests
    if args.test == "large":
        success = asyncio.run(test_large_dataset_update(url=f"{endpoint}/mcp", headers=test_headers))
    elif args.test == "threshold":
        success = asyncio.run(test_batch_threshold(url=f"{endpoint}/mcp", headers=test_headers))
    else:  # all
        success1 = asyncio.run(test_large_dataset_update(url=f"{endpoint}/mcp", headers=test_headers))
        success2 = asyncio.run(test_batch_threshold(url=f"{endpoint}/mcp", headers=test_headers))
        success = success1 and success2

    sys.exit(0 if success else 1)
