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

            # # Test 2: Get sample data from loaded table
            # print(f"\n📊 Test 2: Getting loaded table data")

            # data_res = await session.call_tool("get_table_data", {
            #     "table_id": table_id,
            #     "output_format": "records",
            #     "max_rows": 3
            # })
            # print(f"✅ Sample data: {data_res}")

            # Test 3: Create a new table with test data
            print(f"\n📝 Test 3: Creating new table for export")

            from datetime import datetime
            
            # Generate dynamic timestamps for each run
            now = datetime.now()
            test_data = [
                ["Product", "Price", "Category", "Stock", "Updated"],
                ["Laptop", 999.99, "Electronics", 25, now.strftime("%Y-%m-%d %H:%M:%S")],
                ["Mouse", 29.99, "Electronics", 150, now.strftime("%Y-%m-%d %H:%M:%S")],
                ["Book", 19.99, "Education", 75, now.strftime("%Y-%m-%d %H:%M:%S")]
            ]

            create_res = await session.call_tool("create_table", {
                "data": test_data[1:],  # Data without headers
                "headers": test_data[0],  # Headers
                "name": "MCP Test Products"
            })
            print(f"✅ Create table result: {create_res}")

            # Extract new table ID
            if create_res.content and create_res.content[0].text:
                content = json.loads(create_res.content[0].text)
                if content.get('success'):
                    new_table_id = content.get('table_id')
                    print(f"✅ New table created: {new_table_id}")

            # Test 4: Export table to Google Sheets using export_table_to_range
            print(f"\n📗 Test 4: Writing table to Google Sheets")
            write_uri = read_write_uri
            print(f"   URI: {write_uri}")

            if 'new_table_id' in locals():
                export_res = await session.call_tool("export_table_to_range", {
                    "table_id": new_table_id,
                    "uri": write_uri,
                    "start_cell": "A1",
                    "include_headers": True
                })
                print(f"✅ Export result: {export_res}")

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