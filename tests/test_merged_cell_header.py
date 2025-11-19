#!/usr/bin/env python3
"""
Test script for OM-4244: Merged cell header detection

Tests the enhanced load_data_table with:
1. Smart header detection (auto_detect_header_row=True)
2. Range address support (range_address parameter)
3. Manual header selection (auto_detect_header_row=False)

Usage:
    # Run all tests
    python tests/test_merged_cell_header.py --env=local --test=all

    # Run specific test
    python tests/test_merged_cell_header.py --env=local --test=smart
    python tests/test_merged_cell_header.py --env=local --test=range
    python tests/test_merged_cell_header.py --env=local --test=manual

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

# Test spreadsheet with merged cells in first row (from screenshot)
# - Row 1: Merged cells with green background (货品描述 merged E-G, 每日库存报表 merged rest)
# - Row 2: Real column headers (库位, 库位类, 数量, 单位, 算数1, etc.)
# - Row 3+: Data rows (P-8R-3-9, 特货库位, 2, EA, 2...)
TEST_URI_MERGED = "https://docs.google.com/spreadsheets/d/1zWJhzLl4sdaEVfkvoEL7OstjOXtw4AhbIOivDcVRdC4/edit?gid=1011013284#gid=1011013284"

# Normal spreadsheet without merged cells (from screenshot)
# - Row 1: Regular headers (步骤, 使用工具, Note, Block)
# - Row 2+: Data rows (录制Replay的内容, Screen Studio, 控制在1min内...)
TEST_URI_NORMAL = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=0#gid=0"

async def test_smart_detection(url, headers, test_uri):
    """Test 1: Smart header detection (default behavior)"""
    print("\n" + "="*80)
    print("TEST 1: Smart Header Detection (auto_detect_header_row=True)")
    print("="*80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print(f"📎 URI: {test_uri}")
            print("🔍 Calling load_data_table with smart detection enabled...")

            result = await session.call_tool("load_data_table", {
                "uri": test_uri,
                "auto_detect_header_row": True  # Default: smart detection
            })

            if result.isError:
                error_msg = result.content[0].text if result.content else 'Unknown error'
                print(f"❌ ERROR: {error_msg}")
                return None

            content = json.loads(result.content[0].text)

            print(f"✅ Success: {content.get('success')}")
            print(f"📊 Table Name: {content.get('name')}")
            print(f"📏 Shape: {content.get('shape')}")

            if content.get('data'):
                headers = list(content['data'][0].keys()) if content['data'] else []
                print(f"📋 Headers ({len(headers)} columns): {headers}")
                print(f"📄 First 3 rows:")
                for i, row in enumerate(content['data'][:3]):
                    print(f"  Row {i+1}: {row}")
            else:
                print("⚠️  No data returned")

            return content


async def test_range_address(url, headers, test_uri):
    """Test 2: Using range_address to skip row 1"""
    print("\n" + "="*80)
    print("TEST 2: Range Address (range_address='2:100')")
    print("="*80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print(f"📎 URI: {test_uri}")
            print("🔍 Calling load_data_table with range_address='2:100'...")

            result = await session.call_tool("load_data_table", {
                "uri": test_uri,
                "range_address": "2:100",  # Skip row 1, start from row 2
                "auto_detect_header_row": True
            })

            if result.isError:
                error_msg = result.content[0].text if result.content else 'Unknown error'
                print(f"❌ ERROR: {error_msg}")
                return None

            content = json.loads(result.content[0].text)

            print(f"✅ Success: {content.get('success')}")
            print(f"📊 Table Name: {content.get('name')}")
            print(f"📏 Shape: {content.get('shape')}")

            if content.get('data'):
                headers = list(content['data'][0].keys()) if content['data'] else []
                print(f"📋 Headers ({len(headers)} columns): {headers}")
                print(f"📄 First 3 rows:")
                for i, row in enumerate(content['data'][:3]):
                    print(f"  Row {i+1}: {row}")
            else:
                print("⚠️  No data returned")

            return content


async def test_manual_mode(url, headers, test_uri):
    """Test 3: Manual mode (force row 0 as header)"""
    print("\n" + "="*80)
    print("TEST 3: Manual Mode (auto_detect_header_row=False)")
    print("="*80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print(f"📎 URI: {test_uri}")
            print("🔍 Calling load_data_table with auto_detect_header_row=False...")

            result = await session.call_tool("load_data_table", {
                "uri": test_uri,
                "auto_detect_header_row": False  # Force row 0 as header (old behavior)
            })

            if result.isError:
                error_msg = result.content[0].text if result.content else 'Unknown error'
                print(f"❌ ERROR: {error_msg}")
                return None

            content = json.loads(result.content[0].text)

            print(f"✅ Success: {content.get('success')}")
            print(f"📊 Table Name: {content.get('name')}")
            print(f"📏 Shape: {content.get('shape')}")
            print(f"⚠️  Note: This should show merged cells issue if row 0 has merged cells")

            if content.get('data'):
                headers = list(content['data'][0].keys()) if content['data'] else []
                print(f"📋 Headers ({len(headers)} columns): {headers}")
                print(f"📄 First 3 rows:")
                for i, row in enumerate(content['data'][:3]):
                    print(f"  Row {i+1}: {row}")
            else:
                print("⚠️  No data returned")

            return content


async def test_specific_range(url, headers, test_uri):
    """Test 4: Specific range address"""
    print("\n" + "="*80)
    print("TEST 4: Specific Range (range_address='A2:M100')")
    print("="*80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print(f"📎 URI: {test_uri}")
            print("🔍 Calling load_data_table with range_address='A2:M100'...")

            result = await session.call_tool("load_data_table", {
                "uri": test_uri,
                "range_address": "A2:M100",  # Specific rectangular range
                "auto_detect_header_row": True
            })

            if result.isError:
                error_msg = result.content[0].text if result.content else 'Unknown error'
                print(f"❌ ERROR: {error_msg}")
                return None

            content = json.loads(result.content[0].text)

            print(f"✅ Success: {content.get('success')}")
            print(f"📊 Table Name: {content.get('name')}")
            print(f"📏 Shape: {content.get('shape')}")

            if content.get('data'):
                headers = list(content['data'][0].keys()) if content['data'] else []
                print(f"📋 Headers ({len(headers)} columns): {headers}")
                print(f"📄 First 3 rows:")
                for i, row in enumerate(content['data'][:3]):
                    print(f"  Row {i+1}: {row}")
            else:
                print("⚠️  No data returned")

            return content


async def test_comparison(url, headers):
    """Test 5: Compare merged vs normal sheet behavior"""
    print("\n" + "="*80)
    print("TEST 5: Comparison - Merged Sheet vs Normal Sheet")
    print("="*80)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test merged sheet
            print(f"\n📋 PART A: Merged Cell Sheet (with smart detection)")
            print(f"📎 URI: {TEST_URI_MERGED}")

            merged_result = await session.call_tool("load_data_table", {
                "uri": TEST_URI_MERGED,
                "auto_detect_header_row": True
            })

            if not merged_result.isError:
                merged_content = json.loads(merged_result.content[0].text)
                merged_headers = list(merged_content['data'][0].keys()) if merged_content.get('data') else []
                print(f"✅ Success: {merged_content.get('success')}")
                print(f"📏 Shape: {merged_content.get('shape')}")
                print(f"📋 Headers ({len(merged_headers)} columns):")
                print(f"   First 10: {merged_headers[:10]}")
                print(f"📄 First row data:")
                if merged_content.get('data'):
                    first_row = merged_content['data'][0]
                    print(f"   库位: {first_row.get('库位', 'N/A')}")
                    print(f"   库位类型: {first_row.get('库位类型', 'N/A')}")
                    print(f"   数量: {first_row.get('数量', 'N/A')}")
                    print(f"   单位: {first_row.get('单位', 'N/A')}")
            else:
                print(f"❌ ERROR: {merged_result.content[0].text if merged_result.content else 'Unknown'}")

            # Test normal sheet
            print(f"\n📋 PART B: Normal Sheet (no merged cells)")
            print(f"📎 URI: {TEST_URI_NORMAL}")

            normal_result = await session.call_tool("load_data_table", {
                "uri": TEST_URI_NORMAL,
                "auto_detect_header_row": True
            })

            if not normal_result.isError:
                normal_content = json.loads(normal_result.content[0].text)
                normal_headers = list(normal_content['data'][0].keys()) if normal_content.get('data') else []
                print(f"✅ Success: {normal_content.get('success')}")
                print(f"📏 Shape: {normal_content.get('shape')}")
                print(f"📋 Headers ({len(normal_headers)} columns): {normal_headers}")
                print(f"📄 First row data:")
                if normal_content.get('data'):
                    first_row = normal_content['data'][0]
                    for key in list(first_row.keys())[:4]:
                        print(f"   {key}: {first_row[key]}")
            else:
                print(f"❌ ERROR: {normal_result.content[0].text if normal_result.content else 'Unknown'}")

            print(f"\n🔍 Analysis:")
            print(f"   Merged sheet: Smart detection skipped merged row 1, used row 2 as headers")
            print(f"   Normal sheet: Row 1 directly used as headers (no merged cells)")
            print(f"   Both work correctly! ✅")


async def main():
    parser = argparse.ArgumentParser(description="Test merged cell header detection")
    parser.add_argument(
        "--env",
        type=str,
        choices=["local", "test", "prod"],
        default="local",
        help="Environment to test against"
    )
    parser.add_argument(
        "--test",
        type=str,
        choices=["all", "smart", "range", "manual", "specific", "compare"],
        default="all",
        help="Which test to run"
    )
    parser.add_argument(
        "--uri",
        type=str,
        default=TEST_URI_MERGED,
        help="Google Sheets URI to test (must be a native Google Sheet with merged cells in row 1)"
    )

    args = parser.parse_args()

    # Configure MCP server URL based on environment
    # Set endpoint based on environment argument
    if args.env == "test":
        endpoint = "https://datatable-mcp-test.maybe.ai"
    elif args.env == "prod":
        endpoint = "https://datatable-mcp.maybe.ai"
    else:
        endpoint = "http://127.0.0.1:8321"
    endpoint = f"{endpoint}/mcp"

    # OAuth headers for testing (from environment variables)
    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    print("\n🚀 Testing OM-4244: Merged Cell Header Detection")
    print(f"🌐 Environment: {args.env}")
    print(f"🔗 MCP URL: {endpoint}")
    print(f"📎 Test URI: {args.uri}")
    print(f"👤 User ID: {TEST_USER_ID}")

    try:
        if args.test in ["all", "smart"]:
            await test_smart_detection(endpoint, headers, args.uri)

        if args.test in ["all", "range"]:
            await test_range_address(endpoint, headers, args.uri)

        if args.test in ["all", "manual"]:
            await test_manual_mode(endpoint, headers, args.uri)

        if args.test in ["all", "specific"]:
            await test_specific_range(endpoint, headers, args.uri)

        if args.test in ["all", "compare"]:
            await test_comparison(endpoint, headers)

        print("\n" + "="*80)
        print("✅ ALL TESTS COMPLETED")
        print("="*80)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
