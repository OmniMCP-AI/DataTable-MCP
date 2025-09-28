#!/usr/bin/env python3
"""
Basic Google Sheets Read/Write Test using DataTable MCP
Similar to test_google_sheets_mongodb.py but using MCP client
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
            read_only_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"  # Public demo sheet
            read_write_id = "1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M"  # Wade's test sheet

            print(f"🚀 Testing Google Sheets MCP Integration")
            print(f"📋 User ID: {TEST_USER_ID}")
            print("=" * 60)

            # # Test 0: List available tools
            # print(f"\n🛠️  Test 0: Listing available MCP tools")
            # tools = await session.list_tools()
            # print(f"✅ Found {len(tools.tools)} available tools:")
            # for i, tool in enumerate(tools.tools, 1):
            #     print(f"   {i:2d}. {tool.name}: {tool.description[:80]}...")
            # print()

            # Test 1: Load table from Google Sheets (READ)
            print(f"\n📘 Test 1: Loading data from Google Sheets")
            print(f"   URL: https://docs.google.com/spreadsheets/d/{read_only_id}/edit")

            load_res = await session.call_tool("load_table_google_sheets", {
                "source_path": read_only_id,
                "name": "Class Data Demo",
                "sheet_name": "Class Data"
            })
            print(f"✅ Load result: {load_res}")

            # # Extract table ID for further operations
            # if load_res.content and load_res.content[0].text:
            #     content = json.loads(load_res.content[0].text)
            #     if content.get('success'):
            #         table_id = content.get('table_id')
            #         print(f"✅ Table loaded successfully: {table_id}")

            # # Test 2: Get sample data from loaded table
            # print(f"\n📊 Test 2: Getting loaded table data")

            # data_res = await session.call_tool("get_table_data", {
            #     "table_id": table_id,
            #     "output_format": "records",
            #     "max_rows": 3
            # })
            # print(f"✅ Sample data: {data_res}")

            # # Test 3: Create a new table with test data
            # print(f"\n📝 Test 3: Creating new table for export")

            # from datetime import datetime
            
            # # Generate dynamic timestamps for each run
            # now = datetime.now()
            # test_data = [
            #     ["Product", "Price", "Category", "Stock", "Updated"],
            #     ["Laptop", 999.99, "Electronics", 25, now.strftime("%Y-%m-%d %H:%M:%S")],
            #     ["Mouse", 29.99, "Electronics", 150, now.strftime("%Y-%m-%d %H:%M:%S")],
            #     ["Book", 19.99, "Education", 75, now.strftime("%Y-%m-%d %H:%M:%S")]
            # ]

            # create_res = await session.call_tool("create_table", {
            #     "data": test_data[1:],  # Data without headers
            #     "headers": test_data[0],  # Headers
            #     "name": "MCP Test Products"
            # })
            # print(f"✅ Create table result: {create_res}")

            # # Extract new table ID
            # if create_res.content and create_res.content[0].text:
            #     content = json.loads(create_res.content[0].text)
            #     if content.get('success'):
            #         new_table_id = content.get('table_id')
            #         print(f"✅ New table created: {new_table_id}")

            # # Test 4: Export table to Google Sheets (WRITE)
            # print(f"\n📗 Test 4: Writing table to Google Sheets")
            # print(f"   URL: https://docs.google.com/spreadsheets/d/{read_write_id}/edit")

            # export_res = await session.call_tool("export_table", {
            #     "table_id": new_table_id,
            #     "export_format": "google_sheets",
            #     "spreadsheet_id": read_write_id,
            #     "spreadsheet_name": "test-worksheet",  # Use spreadsheet_name instead
            #     "user_id": TEST_USER_ID
            # })
            # print(f"✅ Export result: {export_res}")

            # # Test 5: Update a specific cell (use proper worksheet name)
            # print(f"\n📝 Test 5: Updating individual cell")

            # import datetime
            # timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # cell_res = await session.call_tool("update_spreadsheet_cell", {
            #     "spreadsheet_id": read_write_id,
            #     "worksheet": "Sheet1",  # Use standard worksheet name
            #     "cell_address": "F1",
            #     "value": "Last Updated",
            #     "user_id": TEST_USER_ID
            # })
            # print(f"✅ Cell update result: {cell_res}")

            # # Test 6: Update cell with timestamp
            # timestamp_res = await session.call_tool("update_spreadsheet_cell", {
            #     "spreadsheet_id": read_write_id,
            #     "worksheet": "Sheet1",  # Use standard worksheet name
            #     "cell_address": "F2",
            #     "value": timestamp,
            #     "user_id": TEST_USER_ID
            # })
            # print(f"✅ Timestamp update result: {timestamp_res}")

            print("\n" + "=" * 60)
            print("🎉 All Google Sheets MCP tests completed!")

if __name__ == "__main__":
    print("🔗 Make sure MCP server is running on http://127.0.0.1:8321")
    import os
    # Mock OAuth headers for testing (you need to provide real ones)
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN" : os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
		"GOOGLE_OAUTH_CLIENT_ID" : os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
		"GOOGLE_OAUTH_CLIENT_SECRET" : os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    asyncio.run(test_google_sheets_mcp(url="http://localhost:8321/mcp", headers=test_headers))