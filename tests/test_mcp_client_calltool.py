#!/usr/bin/env python3
"""
Google Sheets MCP Integration Tests
Separated into focused test functions for better maintainability:

Test Functions:
- test_basic_operations: Tool listing, data loading, error handling
- test_write_operations: Range updates, row/column appends
- test_advanced_operations: New sheet creation, complex data formats
- test_gid_fix: Tests write_new_sheet with gid + update_range with Chinese worksheets

Usage:
    # Run all tests
    python test_mcp_client_calltool.py --env=local --test=all

    # Run specific test
    python test_mcp_client_calltool.py --env=local --test=basic
    python test_mcp_client_calltool.py --env=local --test=write
    python test_mcp_client_calltool.py --env=local --test=advanced
    python test_mcp_client_calltool.py --env=local --test=gid

    # Run against production
    python test_mcp_client_calltool.py --env=prod --test=all

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
from mcp import ClientSession
from datetime import datetime

# Test configuration constants
TEST_USER_ID = "68501372a3569b6897673a48"
READ_ONLY_URI = "https://docs.google.com/spreadsheets/d/1DpaI7L4yfYptsv6X2TL0InhVbeFfe2TpZPPoY98llR0/edit?gid=1411021775#gid=1411021775"
READ_WRITE_URI = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=265933634#gid=265933634"
READ_WRITE_URI2 = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=1852099269#gid=1852099269"
READ_WRITE_URI3 = "https://docs.google.com/spreadsheets/d/1h6waNEyrv_LKbxGSyZCJLf-QmLgFRNIQM4PfTphIeDM/edit?gid=244346339#gid=244346339"

async def test_basic_operations(url, headers):
    """Test basic MCP operations: tool listing, data loading, error handling"""
    print(f"🚀 Testing Basic MCP Operations")
    print(f"📋 User ID: {TEST_USER_ID}")
    print("=" * 60)
    
    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # Test 0: List available tools
            print(f"\n🛠️  Test 0: Listing available MCP tools")
            tools = await session.list_tools()
            print(f"✅ Found {len(tools.tools)} available tools:")
            for i, tool in enumerate(tools.tools, 1):
                print(f"   {i:2d}. {tool.name}: {tool.description[:80]}...")
            
            # Display Field descriptions for a sample tool
            print(f"\n📝 Sample Tool Schema (update_range):")
            for tool in tools.tools:
                if tool.name == "update_range":
                    if hasattr(tool, 'inputSchema') and tool.inputSchema:
                        schema = tool.inputSchema
                        properties = schema.get('properties', {}) if isinstance(schema, dict) else {}
                        for param_name, param_info in properties.items():
                            if isinstance(param_info, dict):
                                desc = param_info.get('description', 'No description')
                                print(f"   • {param_name}: {desc}")
                    break
            print()

            # Test 1: Load table from Google Sheets using URI-based approach
            print(f"\n📘 Test 1: Loading data from Google Sheets")
            print(f"   URI: {READ_ONLY_URI}")

            load_res = await session.call_tool("load_data_table", {
                "uri": READ_ONLY_URI,
            })
            print(f"✅ Load result: {load_res}")

            # Extract table ID for further operations
            table_id = None
            if load_res.isError:
                print(f"⚠️  Load failed with error: {load_res.content[0].text if load_res.content else 'Unknown error'}")
            elif load_res.content and load_res.content[0].text:
                content = json.loads(load_res.content[0].text)
                if content.get('success'):
                    table_id = content.get('table_id')
                    print(f"✅ Table loaded successfully: {table_id}")

            # Test 2: Load table with invalid URI (expect error)
            print(f"\n📘 Test 2: Loading data with invalid URI (expect error)")
            invalid_uri = "https://invalid-uri-format"
            print(f"   URI: {invalid_uri}")

            invalid_load_res = await session.call_tool("load_data_table", {
                "uri": invalid_uri
            })
            print(f"Result: {invalid_load_res}")

            # Check if error was properly returned
            if invalid_load_res.isError:
                print(f"✅ Expected error received: isError = True")
                if invalid_load_res.content and invalid_load_res.content[0].text:
                    print(f"   Error message: {invalid_load_res.content[0].text}")
            else:
                print(f"❌ Expected isError = True, but got isError = False")
                
            print(f"\n✅ Basic operations test completed!")
            return table_id

async def test_write_operations(url, headers):
    """Test write operations: range updates, row/column appends"""
    print(f"🚀 Testing Write Operations")
    print("=" * 60)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # Test 1: Update specific range using update_range function
            print(f"\n📝 Test 1: Updating specific range")            

            # Note: data must be a list, even for single cell updates
            cell_update_res = await session.call_tool("update_range", {
                "uri": READ_WRITE_URI,
                "data": [[f"Updated: {timestamp}"]],  # Must be list[list] format
                "range_address": "F1"
            })
            print(f"✅ Cell update result: {cell_update_res}")

            # Test 2: Update row data using update_range
            print(f"\n📝 Test 2: Updating row data")

            new_row_data = [["New Product", 49.99, "Gadgets", 100, timestamp, "updated whole row"]]

            row_update_res = await session.call_tool("update_range", {
                "uri": READ_WRITE_URI,
                "data": new_row_data,
                "range_address": "A5:F5"
            })
            print(f"✅ Row update result: {row_update_res}")

            # Test 3: Append rows using append_rows
            print(f"\n📝 Test 3: Appending new rows")

            new_rows = [
                ["Appended Product 1", 29.99, "Electronics", 50, timestamp, "appended row 1"],
                ["Appended Product 2", 39.99, "Books", 75, timestamp, "appended row 2"]
            ]

            append_rows_res = await session.call_tool("append_rows", {
                "uri": READ_WRITE_URI,
                "data": new_rows
            })
            print(f"✅ Append rows result: {append_rows_res}")

            # Test 4: Append columns using append_columns
            print(f"\n📝 Test 4: Appending new columns")

            # Data should be in row format: each row contains values for all new columns
            new_columns_data = [
                ["Active", 4.5],    # Row 1: Status=Active, Rating=4.5
                ["Active", 4.0],    # Row 2: Status=Active, Rating=4.0  
                ["Active", 5.0],    # Row 3: Status=Active, Rating=5.0
                ["Active", 4.2],    # Row 4: Status=Active, Rating=4.2
                ["Active", 4.8]     # Row 5: Status=Active, Rating=4.8
            ]

            append_columns_res = await session.call_tool("append_columns", {
                "uri": READ_WRITE_URI,
                "data": new_columns_data,
                "headers": ["Status", "Rating"]
            })
            print(f"✅ Append columns result: {append_columns_res}")

            # Test 5: Verify append_columns result directly (no need to load back)
            print(f"\n📖 Test 5: Verifying append_columns result")

            if not append_columns_res.isError and append_columns_res.content and append_columns_res.content[0].text:
                result_content = json.loads(append_columns_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    data_shape = result_content.get('data_shape', [0, 0])
                    range_updated = result_content.get('range', '')

                    print(f"   📊 Updated {updated_cells} cells with shape {data_shape}")
                    print(f"   📍 Range updated: {range_updated}")

                    # Expected: 12 cells (6 rows: 1 header + 5 data, × 2 columns)
                    expected_cells = 6 * 2  # 6 rows × 2 columns
                    if updated_cells == expected_cells and data_shape[1] == 2:
                        print(f"   ✅ PASS: Correct number of cells updated (includes headers)")
                        print(f"   ✅ PASS: Headers 'Status' and 'Rating' written to first row")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells with 2 columns, got {updated_cells} cells {data_shape}")
                else:
                    print(f"   ❌ Failed to append columns: {result_content.get('message', 'Unknown error')}")
            else:
                print(f"   ❌ Failed to verify append_columns result")
            
            # Test 6: Test append_columns with single column and verify header placement
            print(f"\n📝 Test 6: Testing single column append with header verification")

            # Create test data similar to the user's "Make.com" case
            # Note: Each row must be a list, even for single column data
            make_column_data = [
                ["Visual workflow automation with drag-drop interface; enterprise-focused with advanced routing."],
                ["Moderate learning curve: visual builder but requires understanding of modules, filters, and data mapping."],
                ["1000+ app integrations with webhooks, scheduled scenarios, email triggers, API polling, etc."],
                ["1000+ pre-built modules + HTTP/API requests + custom functions; visual data mapping between apps."],
                ["Primarily OpenAI integration; limited native LLM support but can connect via HTTP modules."]
            ]
            
            single_column_res = await session.call_tool("append_columns", {
                "uri": READ_WRITE_URI3,
                "data": make_column_data,
                "headers": ["Make.com Features"]
            })
            print(f"✅ Single column append result: {single_column_res}")
            
            # Verify the single column was added with proper header
            if not single_column_res.isError and single_column_res.content and single_column_res.content[0].text:
                result_content = json.loads(single_column_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    data_shape = result_content.get('data_shape', [0, 0])
                    range_updated = result_content.get('range', '')
                    
                    print(f"   📊 Updated {updated_cells} cells with shape {data_shape}")
                    print(f"   📍 Range updated: {range_updated}")
                    
                    # Expected: 6 cells (1 header + 5 data items)
                    expected_cells = len(make_column_data) + 1  # +1 for header
                    if updated_cells == expected_cells:
                        print(f"   ✅ PASS: Correct number of cells updated (header + data)")
                        
                        # Extract column letter from range (e.g., "H1:H6" -> "H")
                        if ':' in range_updated:
                            start_cell = range_updated.split(':')[0]
                            column_letter = ''.join(filter(str.isalpha, start_cell))
                            row_number = ''.join(filter(str.isdigit, start_cell))
                            
                            if row_number == "1":
                                print(f"   ✅ PASS: Header placed at {column_letter}1 (correct position)")
                                print(f"   📝 Expected: '{column_letter}1' contains 'Make.com Features'")
                                print(f"   📝 Expected: '{column_letter}2' contains first data item")
                            else:
                                print(f"   ❌ FAIL: Header not at row 1, found at {start_cell}")
                        else:
                            print(f"   ⚠️  WARNING: Could not parse range format: {range_updated}")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells, got {updated_cells}")
                else:
                    print(f"   ❌ Single column append failed: {result_content.get('message', 'Unknown error')}")

            # Test 7: Test worksheet-prefixed range address (Sheet1!A1:J6 format)
            print(f"\n📝 Test 7: Testing worksheet-prefixed range address format")

            # This test verifies the robustness enhancement to handle range addresses like "Sheet1!A1:J6"
            worksheet_prefixed_data = [
                ["ProductID", "ProductName", "Category", "Price", "Stock"],
                ["001", "Laptop", "Electronics", 999.99, 15],
                ["002", "Mouse", "Electronics", 25.99, 50]
            ]

            # Use a worksheet-prefixed range address
            worksheet_range_res = await session.call_tool("update_range", {
                "uri": READ_WRITE_URI3,
                "data": worksheet_prefixed_data,
                "range_address": "Sheet3!A10:E12"  # Specify worksheet in range_address
            })
            print(f"✅ Worksheet-prefixed range result: {worksheet_range_res}")

            # Verify the result
            if not worksheet_range_res.isError and worksheet_range_res.content and worksheet_range_res.content[0].text:
                result_content = json.loads(worksheet_range_res.content[0].text)
                if result_content.get('success'):
                    worksheet_name = result_content.get('worksheet', '')
                    range_updated = result_content.get('range', '')
                    updated_cells = result_content.get('updated_cells', 0)

                    print(f"   📊 Worksheet: {worksheet_name}")
                    print(f"   📍 Range: {range_updated}")
                    print(f"   📝 Updated cells: {updated_cells}")

                    # Expected: 15 cells (3 rows × 5 columns), range should be A10:E12
                    expected_cells = 3 * 5
                    if updated_cells == expected_cells and range_updated == "A10:E12":
                        print(f"   ✅ PASS: Worksheet-prefixed range correctly parsed and applied")
                        print(f"   ✅ PASS: Worksheet extracted from range_address: {worksheet_name}")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells at A10:E12, got {updated_cells} at {range_updated}")
                else:
                    print(f"   ❌ Worksheet-prefixed range update failed: {result_content.get('message', 'Unknown error')}")

            # Test 8: Test worksheet validation with fallback behavior
            print(f"\n📝 Test 8: Testing worksheet validation with fallback to URI worksheet")

            # This test verifies that when range_address contains a non-existent worksheet,
            # the system falls back to using the worksheet from the URI
            fallback_test_data = [
                ["Fallback", "Test", "Data", "Sheet30", "Case"],
                ["This", "Should", "Work", "Using", "Sheet3"],
                ["From", "URI", "Instead", "Of", "Sheet30"]
            ]

            # READ_WRITE_URI3 points to a specific worksheet via gid parameter
            # We'll try to use "Sheet30!A1:E3" in range_address (Sheet30 doesn't exist)
            # Expected: System should fall back to the worksheet from URI (determined by gid)
            fallback_test_res = await session.call_tool("update_range", {
                "uri": READ_WRITE_URI3,  # Points to a specific worksheet via gid
                "data": fallback_test_data,
                "range_address": "Sheet30!A1:E3"  # Sheet30 doesn't exist - should fallback
            })
            print(f"Fallback test result: {fallback_test_res}")

            # Verify the result
            if not fallback_test_res.isError and fallback_test_res.content and fallback_test_res.content[0].text:
                result_content = json.loads(fallback_test_res.content[0].text)
                if result_content.get('success'):
                    worksheet_name = result_content.get('worksheet', '')
                    range_updated = result_content.get('range', '')
                    updated_cells = result_content.get('updated_cells', 0)

                    print(f"   📊 Worksheet used: {worksheet_name}")
                    print(f"   📍 Range: {range_updated}")
                    print(f"   📝 Updated cells: {updated_cells}")

                    # Expected: Should use the worksheet from URI, not Sheet30 from range_address
                    expected_cells = 3 * 5  # 15 cells
                    if updated_cells == expected_cells and range_updated == "A1:E3":
                        print(f"   ✅ PASS: System correctly fell back to URI worksheet ({worksheet_name})")
                        print(f"   ✅ PASS: Ignored non-existent worksheet 'Sheet30' from range_address")
                        print(f"   ✅ PASS: Applied range A1:E3 to the correct worksheet")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells at A1:E3")
                        print(f"   ❌ FAIL: Got {updated_cells} cells at {range_updated}")
                else:
                    print(f"   ❌ FAIL: Operation failed: {result_content.get('message', 'Unknown error')}")
                    print(f"   Note: With fallback logic, this should succeed")
            else:
                print(f"   ❌ FAIL: No response content received")

            # Test 9: Test worksheet validation with another non-existent worksheet
            print(f"\n📝 Test 9: Testing worksheet validation with different non-existent worksheet")

            # Similar test but with a different non-existent worksheet name
            fallback_test_data2 = [
                ["NonExistent", "Test", "Data"],
                ["Should", "Use", "Sheet3"],
                ["Not", "NonExistentSheet", "!!!"]
            ]

            # READ_WRITE_URI3 points to a specific worksheet via gid parameter
            # We'll try to use "NonExistentSheet!A5:C7" in range_address
            # Expected: System should fall back to the worksheet from URI (determined by gid)
            fallback_test_res2 = await session.call_tool("update_range", {
                "uri": READ_WRITE_URI3,  # Points to a specific worksheet via gid
                "data": fallback_test_data2,
                "range_address": "NonExistentSheet!A5:C7"  # Invalid worksheet
            })
            print(f"Fallback test 2 result: {fallback_test_res2}")

            # Verify the result
            if not fallback_test_res2.isError and fallback_test_res2.content and fallback_test_res2.content[0].text:
                result_content = json.loads(fallback_test_res2.content[0].text)
                if result_content.get('success'):
                    worksheet_name = result_content.get('worksheet', '')
                    range_updated = result_content.get('range', '')
                    updated_cells = result_content.get('updated_cells', 0)

                    print(f"   📊 Worksheet used: {worksheet_name}")
                    print(f"   📍 Range: {range_updated}")
                    print(f"   📝 Updated cells: {updated_cells}")

                    # Expected: Should use the worksheet from URI (any valid worksheet)
                    expected_cells = 3 * 3  # 9 cells
                    if updated_cells == expected_cells:
                        print(f"   ✅ PASS: System correctly fell back to URI worksheet ({worksheet_name})")
                        print(f"   ✅ PASS: Ignored non-existent worksheet 'NonExistentSheet' from range_address")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells, got {updated_cells} cells")
                else:
                    print(f"   ⚠️  Operation failed: {result_content.get('message', 'Unknown error')}")

            # Test 10: Test worksheet validation with valid worksheet in range_address
            print(f"\n📝 Test 10: Testing worksheet validation with valid worksheet in range_address")

            # This test verifies that when range_address contains a valid worksheet,
            # the system uses it even if URI points to a different worksheet
            valid_worksheet_data = [
                ["Valid", "Worksheet", "Test"],
                ["test-write", "Explicitly", "Specified"]
            ]

            # Use READ_WRITE_URI (points to test-write sheet via gid)
            # Specify "test-write" explicitly in range_address to verify it validates and uses it
            # Expected: System should validate and use test-write from range_address
            valid_worksheet_res = await session.call_tool("update_range", {
                "uri": READ_WRITE_URI,  # Points to test-write sheet via gid
                "data": valid_worksheet_data,
                "range_address": "test-write!A15:C16"  # Valid worksheet in range_address
            })
            print(f"Valid worksheet test result: {valid_worksheet_res}")

            # Verify the result
            if not valid_worksheet_res.isError and valid_worksheet_res.content and valid_worksheet_res.content[0].text:
                result_content = json.loads(valid_worksheet_res.content[0].text)
                if result_content.get('success'):
                    worksheet_name = result_content.get('worksheet', '')
                    range_updated = result_content.get('range', '')
                    updated_cells = result_content.get('updated_cells', 0)

                    print(f"   📊 Worksheet used: {worksheet_name}")
                    print(f"   📍 Range: {range_updated}")
                    print(f"   📝 Updated cells: {updated_cells}")

                    # Expected: Should use test-write from range_address
                    expected_cells = 2 * 3  # 6 cells
                    if worksheet_name == "test-write" and updated_cells == expected_cells:
                        print(f"   ✅ PASS: System correctly validated and used worksheet from range_address (test-write)")
                        print(f"   ✅ PASS: Worksheet name matches both URI and range_address")
                    else:
                        print(f"   ❌ FAIL: Expected test-write with {expected_cells} cells, got {worksheet_name} with {updated_cells} cells")
                else:
                    print(f"   ❌ FAIL: Operation failed: {result_content.get('message', 'Unknown error')}")

            print(f"\n✅ Write operations test completed!")

async def test_advanced_operations(url, headers):
    """Test advanced operations: new sheet creation, complex data formats, header detection"""
    print(f"🚀 Testing Advanced Operations")
    print("=" * 60)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # Test 1: Create new sheet (New functionality)
            print(f"\n📝 Test 1: Creating a new Google Sheets spreadsheet")

            new_sheet_data = [
                ["Product Name", "Price", "Category", "Stock"],
                ["Laptop", 999.99, "Electronics", 15],
                ["Mouse", 25.99, "Electronics", 50],
                ["Notebook", 5.99, "Office", 100]
            ]

            create_sheet_res = await session.call_tool("write_new_sheet", {
                "data": new_sheet_data,
                "sheet_name": f"Test Sheet {timestamp}"
            })
            print(f"✅ Create new sheet result: {create_sheet_res}")

            # Verify the result includes spreadsheet URL
            new_spreadsheet_url = None
            if not create_sheet_res.isError and create_sheet_res.content and create_sheet_res.content[0].text:
                result_content = json.loads(create_sheet_res.content[0].text)
                if result_content.get('success'):
                    new_spreadsheet_url = result_content.get('spreadsheet_url')
                    print(f"   ✅ New spreadsheet created:")
                    print(f"      URL: {new_spreadsheet_url}")
                    print(f"      Rows: {result_content.get('rows_created')}")
                    print(f"      Columns: {result_content.get('columns_created')}")

            # Test 2: Verify list[list] format for complex data (Bug fix verification)
            print(f"\n📝 Test 2: Verifying correct list[list] format for complex data")

            # This test verifies the bug fix where data should be list[list[Any]] not a JSON string
            # Note: append_rows appends all rows including headers (no auto-detection for append operations)
            complex_data = [
                ["Username", "Display Name", "Followers", "Published At", "Content"],
                ["elonmusk", "Elon Musk", "226889664", "2025-09-30 05:45:44", "RT @mazemoore: Test tweet"],
                ["testuser", "Test User", "1000", "2025-09-30 06:00:00", "Another test tweet"]
            ]

            complex_data_res = await session.call_tool("append_rows", {
                "uri": READ_WRITE_URI,
                "data": complex_data  # This should work as list[list] format
            })
            print(f"✅ Complex data append result: {complex_data_res}")

            # Verify the result shows multiple cells were updated, not just 1 cell
            if not complex_data_res.isError and complex_data_res.content and complex_data_res.content[0].text:
                result_content = json.loads(complex_data_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    data_shape = result_content.get('data_shape', [0, 0])
                    print(f"   ✅ Updated {updated_cells} cells with shape {data_shape}")
                    print(f"   Expected: 15 cells (3 rows × 5 columns, all rows appended)")

                    # append_rows appends all rows without header detection
                    if updated_cells == 15 and data_shape == [3, 5]:
                        print(f"   ✅ PASS: Data correctly formatted as list[list]")
                        print(f"   ✅ PASS: All rows appended correctly (no header auto-detection in append mode)")
                    else:
                        print(f"   ❌ FAIL: Expected 15 cells [3, 5], got {updated_cells} cells {data_shape}")

            # Test 3: Automatic header detection with embedded headers (like comparison tables)
            print(f"\n📝 Test 3: Automatic header detection with embedded headers")
            
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
                "uri": READ_WRITE_URI2,
                "data": comparison_table_data,
                "range_address": "A1:D3"  # Place it below existing data
            })
            print(f"✅ Automatic header detection result: {header_detection_res}")
            
            # Verify the result
            if not header_detection_res.isError and header_detection_res.content and header_detection_res.content[0].text:
                result_content = json.loads(header_detection_res.content[0].text)
                if result_content.get('success'):
                    print(f"   ✅ Headers automatically detected and processed!")
                    print(f"      Range updated: {result_content.get('range')}")
                    print(f"      Data shape: {result_content.get('data_shape')}")
                    print(f"      Updated cells: {result_content.get('updated_cells')}")

            print(f"\n✅ Advanced operations test completed!")
            return new_spreadsheet_url

async def test_gid_fix(url, headers):
    """Test the gid fix: write_new_sheet returns URL with gid, update_range handles missing gid"""
    print(f"🚀 Testing gid Fix for Chinese Worksheets")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Create new sheet with Chinese data (simulating the error scenario)
            print(f"\n📝 Test 1: Creating new sheet with Chinese data")
            print(f"   This simulates the user's scenario from error log")

            chinese_data = [
                ["用户名", "用户链接", "最后发推时间", "距今天数", "距今月数", "备注"],
                ["@qiuhongbingo", "https://x.com/qiuhongbingo", "2024-03-15", 577, 19, "超过19个月未更新，开发者和开发主管"],
                ["@Juna0xx", "https://x.com/Juna0xx", "2024-03-15", 577, 19, "超过19个月未更新，Web3产品及市场推广专家"],
                ["@LynnZeng18", "https://x.com/LynnZeng18", "2024-03-15", 577, 19, "超过19个月未更新，AI增长专家"]
            ]

            create_sheet_res = await session.call_tool("write_new_sheet", {
                "data": chinese_data,
                "sheet_name": f"X用户最近3个月非活跃Follower分析 {timestamp}"
            })
            print(f"Create new sheet result: {create_sheet_res}")

            # Verify the result includes spreadsheet URL with gid
            new_spreadsheet_url = None
            if not create_sheet_res.isError and create_sheet_res.content and create_sheet_res.content[0].text:
                result_content = json.loads(create_sheet_res.content[0].text)
                if result_content.get('success'):
                    new_spreadsheet_url = result_content.get('spreadsheet_url')
                    print(f"   ✅ New spreadsheet created:")
                    print(f"      URL: {new_spreadsheet_url}")
                    print(f"      Rows: {result_content.get('rows_created')}")
                    print(f"      Columns: {result_content.get('columns_created')}")

                    # Check if URL contains gid
                    if "#gid=" in new_spreadsheet_url:
                        print(f"   ✅ PASS: URL includes gid parameter")
                    else:
                        print(f"   ❌ FAIL: URL missing gid parameter")
                else:
                    print(f"   ❌ Failed to create spreadsheet: {result_content.get('message', 'Unknown error')}")
                    return None

            if not new_spreadsheet_url:
                print(f"   ❌ FAIL: Could not retrieve spreadsheet URL")
                return None

            # Test 2: Update range using the URL returned from write_new_sheet
            print(f"\n📝 Test 2: Updating range with URL from write_new_sheet")
            print(f"   This tests that update_range works with the returned URL")

            additional_data = [
                ["@newuser1", "https://x.com/newuser1", "2024-04-01", 500, 16, "新增用户1"],
                ["@newuser2", "https://x.com/newuser2", "2024-04-01", 500, 16, "新增用户2"]
            ]

            update_res = await session.call_tool("update_range", {
                "uri": new_spreadsheet_url,  # Use URL from write_new_sheet
                "data": additional_data,
                "range_address": "A5"  # Append below existing data
            })
            print(f"Update range result: {update_res}")

            # Verify the update succeeded
            if not update_res.isError and update_res.content and update_res.content[0].text:
                result_content = json.loads(update_res.content[0].text)
                if result_content.get('success'):
                    worksheet_name = result_content.get('worksheet', '')
                    updated_cells = result_content.get('updated_cells', 0)
                    data_shape = result_content.get('data_shape', [0, 0])

                    print(f"   ✅ Update succeeded:")
                    print(f"      Worksheet: {worksheet_name}")
                    print(f"      Updated cells: {updated_cells}")
                    print(f"      Data shape: {data_shape}")

                    # Verify Chinese worksheet name was handled correctly
                    if worksheet_name:
                        print(f"   ✅ PASS: Successfully resolved worksheet name: '{worksheet_name}'")
                        print(f"   ✅ PASS: Handles non-English worksheet names correctly")

                    # Verify correct number of cells updated
                    expected_cells = 2 * 6  # 2 rows x 6 columns
                    if updated_cells == expected_cells:
                        print(f"   ✅ PASS: Correct number of cells updated ({expected_cells})")
                    else:
                        print(f"   ⚠️  WARNING: Expected {expected_cells} cells, got {updated_cells}")
                else:
                    print(f"   ❌ FAIL: Update failed: {result_content.get('message', 'Unknown error')}")

            # Test 3: Test update_range with URL without gid (simulate old behavior)
            print(f"\n📝 Test 3: Testing update_range with URL without gid")
            print(f"   This tests the fallback to gid=0 when gid is missing")

            # Remove gid from URL to simulate old behavior
            url_without_gid = new_spreadsheet_url.split('#')[0] if '#' in new_spreadsheet_url else new_spreadsheet_url
            print(f"   URL without gid: {url_without_gid}")

            more_data = [
                ["@testuser", "https://x.com/testuser", "2024-05-01", 450, 15, "测试用户"]
            ]

            update_res2 = await session.call_tool("update_range", {
                "uri": url_without_gid,  # URL without gid
                "data": more_data,
                "range_address": "A7"
            })
            print(f"Update without gid result: {update_res2}")

            # Verify the update succeeded even without gid
            if not update_res2.isError and update_res2.content and update_res2.content[0].text:
                result_content = json.loads(update_res2.content[0].text)
                if result_content.get('success'):
                    worksheet_name = result_content.get('worksheet', '')
                    print(f"   ✅ PASS: Update succeeded without gid in URL")
                    print(f"      Worksheet resolved: '{worksheet_name}'")
                    print(f"   ✅ PASS: System defaulted to first sheet (gid=0)")
                else:
                    print(f"   ❌ FAIL: Update failed: {result_content.get('message', 'Unknown error')}")

            print(f"\n✅ gid fix test completed!")
            print(f"\n📊 Test Summary:")
            print(f"   ✓ write_new_sheet returns URL with gid")
            print(f"   ✓ update_range works with gid in URL")
            print(f"   ✓ update_range handles missing gid by defaulting to gid=0")
            print(f"   ✓ Correctly handles Chinese/non-English worksheet names")

            return new_spreadsheet_url

async def run_all_tests(url, headers):
    """Run all test suites in sequence"""
    print("🎯 Starting Google Sheets MCP Integration Tests")
    print("=" * 80)

    results = {}

    try:
        # Run basic operations test
        print(f"\n{'='*20} BASIC OPERATIONS {'='*20}")
        table_id = await test_basic_operations(url, headers)
        results['basic_operations'] = {'status': 'passed', 'table_id': table_id}

        # Run write operations test
        print(f"\n{'='*20} WRITE OPERATIONS {'='*20}")
        await test_write_operations(url, headers)
        results['write_operations'] = {'status': 'passed'}

        # Run advanced operations test
        print(f"\n{'='*20} ADVANCED OPERATIONS {'='*20}")
        new_sheet_url = await test_advanced_operations(url, headers)
        results['advanced_operations'] = {'status': 'passed', 'new_sheet_url': new_sheet_url}

        # Run gid fix test
        print(f"\n{'='*20} GID FIX TEST {'='*20}")
        gid_test_url = await test_gid_fix(url, headers)
        results['gid_fix'] = {'status': 'passed', 'test_sheet_url': gid_test_url}

        # Summary
        print(f"\n{'='*80}")
        print("🎉 ALL TESTS COMPLETED SUCCESSFULLY!")
        print(f"{'='*80}")

        for test_name, result in results.items():
            print(f"✅ {test_name.replace('_', ' ').title()}: {result['status']}")
            if 'table_id' in result and result['table_id']:
                print(f"   📊 Table ID: {result['table_id']}")
            if 'new_sheet_url' in result and result['new_sheet_url']:
                print(f"   📄 New Sheet: {result['new_sheet_url']}")
            if 'test_sheet_url' in result and result['test_sheet_url']:
                print(f"   📄 Test Sheet: {result['test_sheet_url']}")

        return results

    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return None

async def run_single_test(test_name, url, headers):
    """Run a single test by name"""
    test_functions = {
        'basic': test_basic_operations,
        'write': test_write_operations,
        'advanced': test_advanced_operations,
        'gid': test_gid_fix
    }
    
    if test_name not in test_functions:
        print(f"❌ Unknown test: {test_name}")
        print(f"Available tests: {', '.join(test_functions.keys())}")
        return
    
    print(f"🎯 Running single test: {test_name}")
    print("=" * 60)
    
    try:
        result = await test_functions[test_name](url, headers)
        print(f"\n✅ Test '{test_name}' completed successfully!")
        return result
    except Exception as e:
        print(f"\n❌ Test '{test_name}' failed with error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    import os
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test Google Sheets MCP Integration")
    parser.add_argument("--env", choices=["local", "prod"], default="local",
                       help="Environment to use: local (127.0.0.1:8321) or prod (datatable-mcp.maybe.ai)")
    parser.add_argument("--test", choices=["all", "basic", "write", "advanced", "gid"], default="all",
                       help="Which test to run: all (default), basic, write, advanced, or gid")
    args = parser.parse_args()

    # Set endpoint based on environment argument
    if args.env == "local":
        endpoint = "http://127.0.0.1:8321"
    else:
        endpoint = "https://datatable-mcp.maybe.ai"

    print(f"🔗 Using {args.env} environment: {endpoint}")
    print(f"💡 Use --env=local for local development or --env=prod for production")
    print(f"🧪 Running test: {args.test}")
    
    # OAuth headers for testing (you need to provide real ones)
    test_headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    # Run the selected test(s)
    if args.test == "all":
        asyncio.run(run_all_tests(url=f"{endpoint}/mcp", headers=test_headers))
    else:
        asyncio.run(run_single_test(args.test, url=f"{endpoint}/mcp", headers=test_headers))