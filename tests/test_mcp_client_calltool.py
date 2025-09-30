#!/usr/bin/env python3
"""
Basic Google Sheets Read/Write Test using DataTable MCP
Updated to use latest function signatures and parameters:
- load_data_table with URI-based approach
- export_table_to_range instead of export_table
- update_range for cell/range updates
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession
from datetime import datetime

async def test_google_sheets_mcp(url, headers):
    """Test Google Sheets operations using DataTable MCP tools"""
    async with streamablehttp_client(url=url, headers=headers) as (
        read,
        write,
        _,
    ):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test user ID - Wade's user ID from existing tests
            TEST_USER_ID = "68501372a3569b6897673a48"

            # Real spreadsheet IDs from previous tests
            read_only_uri = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit?gid=0#gid=0"  # Public demo sheet
            #uri = 
            read_write_uri = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=265933634#gid=265933634"

            print(f"🚀 Testing Google Sheets MCP Integration")
            print(f"📋 User ID: {TEST_USER_ID}")
            print("=" * 60)

            # Test 0: List available tools
            print(f"\n🛠️  Test 0: Listing available MCP tools")
            tools = await session.list_tools()
            print(f"✅ Found {len(tools.tools)} available tools:")
            for i, tool in enumerate(tools.tools, 1):
                print(f"   {i:2d}. {tool.name}: {tool.description[:80]}...")
            print()

            # Test 1: Load table from Google Sheets using URI-based approach
            print(f"\n📘 Test 1: Loading data from Google Sheets")
            sheets_uri = read_only_uri
            print(f"   URI: {sheets_uri}")

            load_res = await session.call_tool("load_data_table", {
                "uri": sheets_uri,
                "name": "Class Data Demo"
            })
            print(f"✅ Load result: {load_res}")

            # Extract table ID for further operations
            if load_res.content and load_res.content[0].text:
                content = json.loads(load_res.content[0].text)
                if content.get('success'):
                    table_id = content.get('table_id')
                    print(f"✅ Table loaded successfully: {table_id}")

            # Test 2: Load table with invalid URI (expect error)
            print(f"\n📘 Test 2: Loading data with invalid URI (expect error)")
            invalid_uri = "https://invalid-uri-format"
            print(f"   URI: {invalid_uri}")

            invalid_load_res = await session.call_tool("load_data_table", {
                "uri": invalid_uri,
                "name": "Invalid Test"
            })
            print(f"Result: {invalid_load_res}")

            # Check if error was properly returned
            if invalid_load_res.isError:
                print(f"✅ Expected error received: isError = True")
                if invalid_load_res.content and invalid_load_res.content[0].text:
                    print(f"   Error message: {invalid_load_res.content[0].text}")
            else:
                print(f"❌ Expected isError = True, but got isError = False")

            write_uri = read_write_uri
            

            # Test 5: Update specific range using update_range function
            print(f"\n📝 Test 5: Updating specific range")

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cell_update_res = await session.call_tool("update_range", {
                "uri": write_uri,
                "data": f"Updated: {timestamp}",
                "range_address": "F1"
            })
            print(f"✅ Cell update result: {cell_update_res}")

            # Test 6: Update row data using update_range
            print(f"\n📝 Test 6: Updating row data")

            new_row_data = [["New Product", 49.99, "Gadgets", 100, timestamp, "updated whole row"]]

            row_update_res = await session.call_tool("update_range", {
                "uri": write_uri,
                "data": new_row_data,
                "range_address": "A5:F5"
            })
            print(f"✅ Row update result: {row_update_res}")

            # Test 7: Append rows using append_rows
            print(f"\n📝 Test 7: Appending new rows")

            new_rows = [
                ["Appended Product 1", 29.99, "Electronics", 50, timestamp, "appended row 1"],
                ["Appended Product 2", 39.99, "Books", 75, timestamp, "appended row 2"]
            ]

            append_rows_res = await session.call_tool("append_rows", {
                "uri": write_uri,
                "data": new_rows
            })
            print(f"✅ Append rows result: {append_rows_res}")

            # Test 8: Append columns using append_columns
            print(f"\n📝 Test 8: Appending new columns")

            new_columns = [
                ["Status", "Active", "Active", "Active", "Active", "Active"],
                ["Rating", 4.5, 4.0, 5.0, 4.2, 4.8]
            ]

            append_columns_res = await session.call_tool("append_columns", {
                "uri": write_uri,
                "data": new_columns,
                "headers": ["Status", "Rating"]
            })
            print(f"✅ Append columns result: {append_columns_res}")

            print("\n" + "=" * 60)
            print("🎉 All Google Sheets MCP tests completed!")

if __name__ == "__main__":
    import os
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test Google Sheets MCP Integration")
    parser.add_argument("--env", choices=["local", "prod"], default="local",
                       help="Environment to use: local (127.0.0.1:8321) or prod (datatable-mcp.maybe.ai)")
    args = parser.parse_args()

    # Set endpoint based on environment argument
    if args.env == "local":
        endpoint = "http://127.0.0.1:8321"
    else:
        endpoint = "https://datatable-mcp.maybe.ai"

    print(f"🔗 Using {args.env} environment: {endpoint}")
    print(f"💡 Use --env=local for local development or --env=prod for production")
    # Mock OAuth headers for testing (you need to provide real ones)
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN" : os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
		"GOOGLE_OAUTH_CLIENT_ID" : os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
		"GOOGLE_OAUTH_CLIENT_SECRET" : os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    asyncio.run(test_google_sheets_mcp(url=f"{endpoint}/mcp", headers=test_headers))