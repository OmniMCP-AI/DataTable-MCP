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
            # read_only_uri = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit?gid=0#gid=0"  # Public demo sheet
            read_only_uri = "https://docs.google.com/spreadsheets/d/1DpaI7L4yfYptsv6X2TL0InhVbeFfe2TpZPPoY98llR0/edit?gid=1411021775#gid=1411021775"
            #uri = 
            read_write_uri = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=265933634#gid=265933634"
            write_uri2 = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=1852099269#gid=1852099269"

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
                # "name": "Class Data Demo"
            })
            print(f"✅ Load result: {load_res}")

            # Extract table ID for further operations
            if load_res.content and load_res.content[0].text:
                content = json.loads(load_res.content[0].text)
                if content.get('success'):
                    table_id = content.get('table_id')
                    print(f"✅ Table loaded successfully: {table_id}")

            # # Test 2: Load table with invalid URI (expect error)
            # print(f"\n📘 Test 2: Loading data with invalid URI (expect error)")
            # invalid_uri = "https://invalid-uri-format"
            # print(f"   URI: {invalid_uri}")

            # invalid_load_res = await session.call_tool("load_data_table", {
            #     "uri": invalid_uri
            # })
            # print(f"Result: {invalid_load_res}")

            # # Check if error was properly returned
            # if invalid_load_res.isError:
            #     print(f"✅ Expected error received: isError = True")
            #     if invalid_load_res.content and invalid_load_res.content[0].text:
            #         print(f"   Error message: {invalid_load_res.content[0].text}")
            # else:
            #     print(f"❌ Expected isError = True, but got isError = False")

            # write_uri = read_write_uri
            

            # # Test 5: Update specific range using update_range function
            # print(f"\n📝 Test 5: Updating specific range")

            # timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # # Note: data must be a list, even for single cell updates
            # cell_update_res = await session.call_tool("update_range", {
            #     "uri": write_uri,
            #     "data": [[f"Updated: {timestamp}"]],  # Must be list[list] format
            #     "range_address": "F1"
            # })
            # print(f"✅ Cell update result: {cell_update_res}")

            # # Test 6: Update row data using update_range
            # print(f"\n📝 Test 6: Updating row data")

            # new_row_data = [["New Product", 49.99, "Gadgets", 100, timestamp, "updated whole row"]]

            # row_update_res = await session.call_tool("update_range", {
            #     "uri": write_uri,
            #     "data": new_row_data,
            #     "range_address": "A5:F5"
            # })
            # print(f"✅ Row update result: {row_update_res}")

            # # Test 7: Append rows using append_rows
            # print(f"\n📝 Test 7: Appending new rows")

            # new_rows = [
            #     ["Appended Product 1", 29.99, "Electronics", 50, timestamp, "appended row 1"],
            #     ["Appended Product 2", 39.99, "Books", 75, timestamp, "appended row 2"]
            # ]

            # append_rows_res = await session.call_tool("append_rows", {
            #     "uri": write_uri,
            #     "data": new_rows
            # })
            # print(f"✅ Append rows result: {append_rows_res}")

            # # Test 8: Append columns using append_columns
            # print(f"\n📝 Test 8: Appending new columns")

            # new_columns = [
            #     ["Status", "Active", "Active", "Active", "Active", "Active"],
            #     ["Rating", 4.5, 4.0, 5.0, 4.2, 4.8]
            # ]

            # append_columns_res = await session.call_tool("append_columns", {
            #     "uri": write_uri,
            #     "data": new_columns,
            #     "headers": ["Status", "Rating"]
            # })
            # print(f"✅ Append columns result: {append_columns_res}")

            # # Test 9: Create new sheet (New functionality)
            # print(f"\n📝 Test 9: Creating a new Google Sheets spreadsheet")

            # new_sheet_data = [
            #     ["Product Name", "Price", "Category", "Stock"],
            #     ["Laptop", 999.99, "Electronics", 15],
            #     ["Mouse", 25.99, "Electronics", 50],
            #     ["Notebook", 5.99, "Office", 100]
            # ]

            # create_sheet_res = await session.call_tool("write_new_sheet", {
            #     "data": new_sheet_data,
            #     "sheet_name": f"Test Sheet {timestamp}"
            # })
            # print(f"✅ Create new sheet result: {create_sheet_res}")

            # # Verify the result includes spreadsheet URL
            # if create_sheet_res.content and create_sheet_res.content[0].text:
            #     result_content = json.loads(create_sheet_res.content[0].text)
            #     if result_content.get('success'):
            #         new_spreadsheet_url = result_content.get('spreadsheet_url')
            #         print(f"   ✅ New spreadsheet created:")
            #         print(f"      URL: {new_spreadsheet_url}")
            #         print(f"      Rows: {result_content.get('rows_created')}")
            #         print(f"      Columns: {result_content.get('columns_created')}")

            # # Test 10: Verify list[list] format for complex data (Bug fix verification)
            # print(f"\n📝 Test 10: Verifying correct list[list] format for complex data")

            # # This test verifies the bug fix where data should be list[list[Any]] not a JSON string
            # complex_data = [
            #     ["Username", "Display Name", "Followers", "Published At", "Content"],
            #     ["elonmusk", "Elon Musk", "226889664", "2025-09-30 05:45:44", "RT @mazemoore: Test tweet"],
            #     ["testuser", "Test User", "1000", "2025-09-30 06:00:00", "Another test tweet"]
            # ]

            # complex_data_res = await session.call_tool("append_rows", {
            #     "uri": write_uri,
            #     "data": complex_data  # This should work as list[list] format
            # })
            # print(f"✅ Complex data append result: {complex_data_res}")

            # # Verify the result shows multiple cells were updated, not just 1 cell
            # if complex_data_res.content and complex_data_res.content[0].text:
            #     result_content = json.loads(complex_data_res.content[0].text)
            #     if result_content.get('success'):
            #         updated_cells = result_content.get('updated_cells', 0)
            #         data_shape = result_content.get('data_shape', [0, 0])
            #         print(f"   ✅ Updated {updated_cells} cells with shape {data_shape}")
            #         print(f"   Expected: 15 cells (3 rows × 5 columns)")

            #         if updated_cells == 15 and data_shape == [3, 5]:
            #             print(f"   ✅ PASS: Data correctly formatted as list[list]")
            #         else:
            #             print(f"   ❌ FAIL: Expected 15 cells [3, 5], got {updated_cells} cells {data_shape}")

            # Test 11: Automatic header detection with embedded headers (like comparison tables)
            print(f"\n📝 Test 11: Automatic header detection with embedded headers")
            
            # This simulates the exact scenario from your log where headers are in the data
            # and the LLM doesn't pass a separate headers parameter
            comparison_table_data = [
                ["Dimension", "Agent Kit (OpenAI)", "n8n", "make.com"],
                ["Primary Purpose", "Fast, visual, chat-first agents inside OpenAI's ecosystem.", "General-purpose workflow automation + AI agents; code-optional but dev-friendly.", "Visual workflow automation platform with no-code/low-code approach for connecting apps and services."],
                ["Pricing Model", "Free while in beta; pay only for token usage.", "Open-source free self-host; cloud plans metered by workflow executions.", "Freemium model with operation-based pricing tiers; free tier with limited operations."]
            ]

            # Call update_range with NO headers parameter - just uri, data, range_address
            # This tests the simplified API and automatic header detection
            header_detection_res = await session.call_tool("update_range", {
                "uri": write_uri2,
                "data": comparison_table_data,
                "range_address": "A1:D3"  # Place it below existing data
            })
            print(f"✅ Automatic header detection result: {header_detection_res}")
            
            # Verify the result
            if header_detection_res.content and header_detection_res.content[0].text:
                result_content = json.loads(header_detection_res.content[0].text)
                if result_content.get('success'):
                    print(f"   ✅ Headers automatically detected and processed!")
                    print(f"      Range updated: {result_content.get('range')}")
                    print(f"      Data shape: {result_content.get('data_shape')}")
                    print(f"      Updated cells: {result_content.get('updated_cells')}")

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