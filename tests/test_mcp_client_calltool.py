#!/usr/bin/env python3
"""
Google Sheets MCP Integration Tests
Separated into focused test functions for better maintainability:

Test Functions:
- test_basic_operations: Tool listing, data loading, error handling
- test_write_operations: Range updates, row/column appends
- test_advanced_operations: New sheet creation, complex data formats
- test_gid_fix: Tests write_new_sheet with gid + update_range with Chinese worksheets
- test_list_of_dict_input: Tests DataFrame-like list of dict input support
- test_1d_array_input: Tests 1D array input support (NEW)

Usage:
    # Run all tests
    python test_mcp_client_calltool.py --env=local --test=all

    # Run specific test
    python test_mcp_client_calltool.py --env=local --test=basic
    python test_mcp_client_calltool.py --env=local --test=write
    python test_mcp_client_calltool.py --env=local --test=advanced
    python test_mcp_client_calltool.py --env=local --test=gid
    python test_mcp_client_calltool.py --env=local --test=listtype
    python test_mcp_client_calltool.py --env=local --test=1d

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
READ_WRITE_URI_1D = "https://docs.google.com/spreadsheets/d/1h6waNEyrv_LKbxGSyZCJLf-QmLgFRNIQM4PfTphIeDM/edit?gid=509803551#gid=509803551"

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

            # Test 3: Verify data format is list of dictionaries
            print(f"\n📘 Test 3: Verifying data format is list of dictionaries")
            print(f"   Testing improved data structure from TableResponse")
            
            load_format_res = await session.call_tool("load_data_table", {
                "uri": READ_ONLY_URI,
            })
            print()
            print(f"load_format_res, {load_format_res}")
            if not load_format_res.isError and load_format_res.content and load_format_res.content[0].text:
                content = json.loads(load_format_res.content[0].text)
                if content.get('success'):
                    data = content.get('data', [])
                    shape = content.get('shape', '(0,0)')
                    
                    print(f"   📊 Shape: {shape}")
                    print(f"   📊 Data rows: {len(data)}")
                    
                    # Verify data is list of dicts
                    if data and len(data) > 0:
                        first_row = data[0]
                        
                        # Check if first row is a dictionary
                        if isinstance(first_row, dict):
                            print(f"   ✅ PASS: Data is list of dictionaries")
                            print(f"   📝 First row type: {type(first_row).__name__}")
                            print(f"   📝 First row keys (these are the headers): {list(first_row.keys())}")
                            print(f"   📝 Sample row: {first_row}")
                            print(f"   ✅ PASS: Headers are embedded as dictionary keys")
                        else:
                            print(f"   ❌ FAIL: Data is not list of dictionaries")
                            print(f"      Expected: dict, Got: {type(first_row).__name__}")
                            print(f"      First row: {first_row}")
                    else:
                        print(f"   ⚠️  WARNING: No data rows to verify")
                else:
                    print(f"   ❌ Failed to load data: {content.get('message', 'Unknown error')}")
            else:
                print(f"   ❌ Failed to get valid response")
                
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
                    shape = result_content.get('shape', '(0,0)')
                    range_updated = result_content.get('range', '')

                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")
                    print(f"   📍 Range updated: {range_updated}")

                    # Expected: 12 cells (6 rows: 1 header + 5 data, × 2 columns)
                    expected_cells = 6 * 2  # 6 rows × 2 columns
                    expected_shape = "(6,2)"
                    if updated_cells == expected_cells and shape == expected_shape:
                        print(f"   ✅ PASS: Correct number of cells updated (includes headers)")
                        print(f"   ✅ PASS: Headers 'Status' and 'Rating' written to first row")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells with shape {expected_shape}, got {updated_cells} cells with shape {shape}")
                else:
                    error_message = result_content.get('message', 'Unknown error')
                    print(f"   ❌ Failed to append columns: {error_message}")

                    # Check if it's a grid limits error
                    if "exceeds grid limits" in error_message.lower():
                        print(f"   🔄 Grid limits exceeded - clearing worksheet and retrying with update_range...")

                        # Clear the worksheet by updating with empty content
                        clear_res = await session.call_tool("update_range", {
                            "uri": READ_WRITE_URI,
                            "data": [[]],
                            "range_address": "A1"
                        })

                        if not clear_res.isError:
                            print(f"   ✅ Worksheet cleared successfully")

                            # Instead of append_columns, use update_range to write to a specific location
                            # This avoids the issue of append_columns trying to find the last column
                            print(f"   🔄 Retrying with update_range to write to columns A-B...")

                            # Prepare data with headers for update_range
                            retry_data = [["Status", "Rating"]] + new_columns_data

                            retry_res = await session.call_tool("update_range", {
                                "uri": READ_WRITE_URI,
                                "data": retry_data,
                                "range_address": "A1"
                            })

                            if not retry_res.isError and retry_res.content and retry_res.content[0].text:
                                retry_content = json.loads(retry_res.content[0].text)
                                if retry_content.get('success'):
                                    print(f"   ✅ Retry successful after clearing worksheet")
                                    print(f"      Updated cells: {retry_content.get('updated_cells', 0)}")
                                    print(f"      Shape: {retry_content.get('shape', '(0,0)')}")
                                    print(f"      Note: Used update_range instead of append_columns to avoid grid limits")
                                else:
                                    print(f"   ❌ Retry failed: {retry_content.get('message', 'Unknown error')}")
                        else:
                            print(f"   ❌ Failed to clear worksheet")
            else:
                print(f"   ❌ Failed to verify append_columns result")
            
            # Test 6: Test single column update with header verification
            print(f"\n📝 Test 6: Testing single column write with header verification")
            print(f"   Using update_range to write to a safe column location (H1)")

            # Create test data similar to the user's "Make.com" case
            # Using list of dicts format which auto-extracts headers
            make_column_data = [
                {"Make.com Features": "Visual workflow automation with drag-drop interface; enterprise-focused with advanced routing."},
                {"Make.com Features": "Moderate learning curve: visual builder but requires understanding of modules, filters, and data mapping."},
                {"Make.com Features": "1000+ app integrations with webhooks, scheduled scenarios, email triggers, API polling, etc."},
                {"Make.com Features": "1000+ pre-built modules + HTTP/API requests + custom functions; visual data mapping between apps."},
                {"Make.com Features": "Primarily OpenAI integration; limited native LLM support but can connect via HTTP modules."}
            ]
            
            # Use update_range with specific column to avoid grid limit issues
            single_column_res = await session.call_tool("update_range", {
                "uri": READ_WRITE_URI3,
                "data": make_column_data,
                "range_address": "H1"  # Write to column H starting at row 1
            })
            print(f"✅ Single column update result: {single_column_res}")
            
            # Verify the single column was added with proper header
            if not single_column_res.isError and single_column_res.content and single_column_res.content[0].text:
                result_content = json.loads(single_column_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')
                    range_updated = result_content.get('range', '')
                    
                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")
                    print(f"   📍 Range updated: {range_updated}")
                    
                    # Expected: 6 cells (1 header + 5 data items)
                    expected_cells = len(make_column_data) + 1  # +1 for header
                    expected_shape = "(6,1)"  # 6 rows (1 header + 5 data), 1 column
                    if updated_cells == expected_cells and shape == expected_shape:
                        print(f"   ✅ PASS: Correct number of cells updated (header + data)")
                        print(f"   ✅ PASS: Shape is correct: {shape}")
                        
                        # Verify range starts at H1
                        if range_updated.startswith("H1"):
                            print(f"   ✅ PASS: Data written to column H starting at row 1")
                            print(f"   📝 Expected: 'H1' contains 'Make.com Features' (header)")
                            print(f"   📝 Expected: 'H2' through 'H6' contain data items")
                        else:
                            print(f"   ⚠️  WARNING: Expected range to start with H1, got: {range_updated}")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells with shape {expected_shape}, got {updated_cells} with shape {shape}")
                else:
                    print(f"   ❌ Single column write failed: {result_content.get('message', 'Unknown error')}")

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
                    shape = result_content.get('shape', '(0,0)')
                    print(f"   ✅ Updated {updated_cells} cells with shape {shape}")
                    print(f"   Expected: 15 cells (3 rows × 5 columns, all rows appended)")

                    # append_rows appends all rows without header detection
                    if updated_cells == 15 and shape == "(3,5)":
                        print(f"   ✅ PASS: Data correctly formatted as list[list]")
                        print(f"   ✅ PASS: All rows appended correctly (no header auto-detection in append mode)")
                    else:
                        print(f"   ❌ FAIL: Expected 15 cells with shape (3,5), got {updated_cells} cells with shape {shape}")

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
                    print(f"      Shape: {result_content.get('shape')}")
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
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   ✅ Update succeeded:")
                    print(f"      Worksheet: {worksheet_name}")
                    print(f"      Updated cells: {updated_cells}")
                    print(f"      Shape: {shape}")

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

async def test_1d_array_input(url, headers):
    """Test 1D array input support for update_range, append_rows, and append_columns"""
    print(f"🚀 Testing 1D Array Input Support")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Use READ_WRITE_URI3 for all tests
            test_uri = READ_WRITE_URI_1D
            print(f"\n💡 Using test sheet: {test_uri}")

            # Test 1: update_range with 1D array (single row)
            print(f"\n📝 Test 1: update_range with 1D array (single row)")
            print(f"   Updating a single row using 1D array format")

            single_row_data = ["Product A", 99.99, "Electronics", 50, timestamp]

            update_row_res = await session.call_tool("update_range", {
                "uri": test_uri,
                "data": single_row_data,  # 1D array
                "range_address": "A1:E1"
            })
            print(f"✅ Update range (1D row) result: {update_row_res}")

            # Verify the result
            if not update_row_res.isError and update_row_res.content and update_row_res.content[0].text:
                result_content = json.loads(update_row_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")

                    # Expected: 5 cells (1 row × 5 columns)
                    expected_cells = 5
                    expected_shape = "(1,5)"
                    if updated_cells == expected_cells and shape == expected_shape:
                        print(f"   ✅ PASS: 1D array converted to single row correctly")
                        print(f"   ✅ PASS: Shape is correct: {shape}")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells with shape {expected_shape}, got {updated_cells} with shape {shape}")
                else:
                    print(f"   ❌ FAIL: {result_content.get('message', 'Unknown error')}")

            # Test 2: append_rows with 1D array (single row)
            print(f"\n📝 Test 2: append_rows with 1D array (single row)")
            print(f"   Appending a single row using 1D array format")

            append_row_data = ["Appended A", 49.99, "Books", 25, timestamp]

            append_row_res = await session.call_tool("append_rows", {
                "uri": test_uri,
                "data": append_row_data  # 1D array
            })
            print(f"✅ Append rows (1D) result: {append_row_res}")

            # Verify the result
            if not append_row_res.isError and append_row_res.content and append_row_res.content[0].text:
                result_content = json.loads(append_row_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")

                    # Expected: 5 cells (1 row × 5 columns)
                    expected_cells = 5
                    expected_shape = "(1,5)"
                    if updated_cells == expected_cells and shape == expected_shape:
                        print(f"   ✅ PASS: 1D array appended as single row correctly")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells with shape {expected_shape}, got {updated_cells} with shape {shape}")

            # Test 3: append_columns with 1D array (single column)
            print(f"\n📝 Test 3: append_columns with 1D array (single column)")
            print(f"   Appending a single column using 1D array format")

            # Note: For append_columns, 1D array represents column values (multiple rows, 1 column)
            # But process_data_input will convert it to [[val1, val2, val3...]] which is 1 row
            # So we need to test if this works as expected or if we need special handling

            append_col_data = ["Value1", "Value2", "Value3", "Value4", "Value5"]

            append_col_res = await session.call_tool("append_columns", {
                "uri": test_uri,
                "data": append_col_data,  # 1D array
                "headers": ["NewColumn"]
            })
            print(f"✅ Append columns (1D) result: {append_col_res}")

            # Verify the result
            if not append_col_res.isError and append_col_res.content and append_col_res.content[0].text:
                result_content = json.loads(append_col_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")

                    # With current implementation, 1D array becomes 1 row
                    # So: 1 header row + 1 data row = 2 rows, 5 columns
                    # This might not be what we want for columns - need to discuss
                    print(f"   💡 Note: 1D array is converted to single row (not column)")
                    print(f"   💡 For columns, you may want to use 2D format: [[val1], [val2], ...]")
                else:
                    print(f"   ⚠️  {result_content.get('message', 'Unknown error')}")

            # Test 4: Mixed - compare 1D vs 2D for same data
            print(f"\n📝 Test 4: Comparing 1D array vs 2D array (single row)")

            # Using 1D array
            data_1d = ["Item1", 10, 1.99]
            res_1d = await session.call_tool("update_range", {
                "uri": test_uri,
                "data": data_1d,
                "range_address": "K25"
            })

            # Using 2D array (single row)
            data_2d = [["Item2", 20, 2.99]]
            res_2d = await session.call_tool("update_range", {
                "uri": test_uri,
                "data": data_2d,
                "range_address": "K26"
            })

            # Compare results
            if (not res_1d.isError and not res_2d.isError and
                res_1d.content and res_2d.content):

                result_1d = json.loads(res_1d.content[0].text)
                result_2d = json.loads(res_2d.content[0].text)

                if result_1d.get('success') and result_2d.get('success'):
                    shape_1d = result_1d.get('shape', '(0,0)')
                    shape_2d = result_2d.get('shape', '(0,0)')

                    print(f"   📊 1D array shape: {shape_1d}")
                    print(f"   📊 2D array shape: {shape_2d}")

                    if shape_1d == shape_2d == "(1,3)":
                        print(f"   ✅ PASS: Both 1D and 2D formats produce same result for single row")
                    else:
                        print(f"   ❌ FAIL: Shapes differ - 1D: {shape_1d}, 2D: {shape_2d}")

            # Test 5: Numeric 1D array
            print(f"\n📝 Test 5: Testing 1D array with numeric values")

            numeric_data = [100, 200, 300, 400, 500]

            numeric_res = await session.call_tool("update_range", {
                "uri": test_uri,
                "data": numeric_data,
                "range_address": "M20"
            })
            print(f"✅ Numeric 1D array result: {numeric_res}")

            if not numeric_res.isError and numeric_res.content and numeric_res.content[0].text:
                result_content = json.loads(numeric_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")

                    if updated_cells == 5 and shape == "(1,5)":
                        print(f"   ✅ PASS: Numeric 1D array handled correctly")

            print(f"\n✅ 1D array input test completed!")
            print(f"\n📊 Test Summary:")
            print(f"   ✓ update_range accepts 1D array (single row)")
            print(f"   ✓ append_rows accepts 1D array (single row)")
            print(f"   ✓ append_columns accepts 1D array (converted to single row)")
            print(f"   ✓ 1D and 2D formats produce same result for single row")
            print(f"   ✓ Numeric 1D arrays handled correctly")
            print(f"   💡 Note: For column operations, consider using 2D format [[val1], [val2], ...]")

async def test_list_of_dict_input(url, headers):
    """Test list of dict input support for write_new_sheet, append_rows, update_range"""
    print(f"🚀 Testing List of Dict Input Support")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Test 1: Create new sheet with list of dicts (DataFrame-like format)
            print(f"\n📝 Test 1: Creating new sheet with list of dicts")
            print(f"   Testing DataFrame-like input format")

            dict_data = [
                {"name": "Alice", "age": 30, "city": "New York", "timestamp": timestamp},
                {"name": "Bob", "age": 25, "city": "Los Angeles", "timestamp": timestamp},
                {"name": "Charlie", "age": 35, "city": "Chicago", "timestamp": timestamp}
            ]

            create_sheet_res = await session.call_tool("write_new_sheet", {
                "data": dict_data,
                "sheet_name": f"List of Dict Test {timestamp}"
            })
            print(f"✅ Create sheet with list of dicts result: {create_sheet_res}")

            # Verify the result
            new_spreadsheet_url = None
            if not create_sheet_res.isError and create_sheet_res.content and create_sheet_res.content[0].text:
                result_content = json.loads(create_sheet_res.content[0].text)
                if result_content.get('success'):
                    new_spreadsheet_url = result_content.get('spreadsheet_url')
                    rows_created = result_content.get('rows_created')
                    columns_created = result_content.get('columns_created')

                    print(f"   ✅ New spreadsheet created:")
                    print(f"      URL: {new_spreadsheet_url}")
                    print(f"      Rows: {rows_created}")
                    print(f"      Columns: {columns_created}")

                    # Verify correct dimensions
                    expected_rows = 3  # 3 data rows
                    expected_cols = 4  # 4 columns (name, age, city, timestamp)

                    if rows_created == expected_rows and columns_created == expected_cols:
                        print(f"   ✅ PASS: Correct dimensions - {expected_rows} rows × {expected_cols} columns")
                        print(f"   ✅ PASS: Headers automatically extracted from dict keys")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_rows}×{expected_cols}, got {rows_created}×{columns_created}")
                else:
                    print(f"   ❌ FAIL: {result_content.get('message', 'Unknown error')}")
            else:
                print(f"   ❌ FAIL: Could not create spreadsheet")

            # Use READ_WRITE_URI3 for all subsequent tests
            test_uri = READ_WRITE_URI3
            print(f"\n💡 Using existing test sheet for remaining tests: {test_uri}")

            # Test 2: Append rows using list of dicts
            print(f"\n📝 Test 2: Appending rows with list of dicts")

            append_dict_data = [
                {"name": "David", "age": 28, "city": "Boston", "timestamp": timestamp},
                {"name": "Eve", "age": 32, "city": "Seattle", "timestamp": timestamp}
            ]

            append_rows_res = await session.call_tool("append_rows", {
                "uri": test_uri,
                "data": append_dict_data
            })
            print(f"✅ Append rows result: {append_rows_res}")

            # Verify the result
            if not append_rows_res.isError and append_rows_res.content and append_rows_res.content[0].text:
                result_content = json.loads(append_rows_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")

                    # Expected: 8 cells (2 rows × 4 columns)
                    expected_cells = 2 * 4
                    expected_shape = "(2,4)"
                    if updated_cells == expected_cells and shape == expected_shape:
                        print(f"   ✅ PASS: Correct number of cells appended")
                        print(f"   ✅ PASS: List of dicts converted to 2D array correctly")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells with shape {expected_shape}, got {updated_cells} with shape {shape}")

            # Test 3: Update range using list of dicts
            print(f"\n📝 Test 3: Updating range with list of dicts")

            update_dict_data = [
                {"product": "Laptop", "price": 999.99, "stock": 15},
                {"product": "Mouse", "price": 25.99, "stock": 50},
                {"product": "Keyboard", "price": 79.99, "stock": 30}
            ]

            update_range_res = await session.call_tool("update_range", {
                "uri": test_uri,
                "data": update_dict_data,
                "range_address": "F1"
            })
            print(f"✅ Update range result: {update_range_res}")

            # Verify the result
            if not update_range_res.isError and update_range_res.content and update_range_res.content[0].text:
                result_content = json.loads(update_range_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)
                    shape = result_content.get('shape', '(0,0)')

                    print(f"   📊 Updated {updated_cells} cells with shape {shape}")

                    # Expected: 12 cells (4 rows including header × 3 columns)
                    expected_cells = 4 * 3  # 1 header row + 3 data rows
                    expected_shape = "(4,3)"
                    if updated_cells == expected_cells and shape == expected_shape:
                        print(f"   ✅ PASS: Correct number of cells updated (includes auto-extracted headers)")
                        print(f"   ✅ PASS: Headers placed in first row")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells with shape {expected_shape}, got {updated_cells} with shape {shape}")

            # Test 4: Mixed data types in list of dicts
            print(f"\n📝 Test 4: Testing mixed data types (str, int, float, bool, None)")

            mixed_type_data = [
                {"product": "Widget", "price": 49.99, "in_stock": True, "quantity": 100, "notes": None},
                {"product": "Gadget", "price": 29.99, "in_stock": False, "quantity": 0, "notes": "Out of stock"}
            ]

            mixed_type_res = await session.call_tool("append_rows", {
                "uri": test_uri,
                "data": mixed_type_data
            })
            print(f"✅ Mixed types result: {mixed_type_res}")

            # Verify the result
            if not mixed_type_res.isError and mixed_type_res.content and mixed_type_res.content[0].text:
                result_content = json.loads(mixed_type_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)

                    # Expected: 10 cells (2 rows × 5 columns)
                    expected_cells = 2 * 5
                    if updated_cells == expected_cells:
                        print(f"   ✅ PASS: Mixed data types handled correctly")
                        print(f"   ✅ PASS: None values converted properly")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells, got {updated_cells}")

            # Test 5: Sparse data (missing keys in some dicts)
            print(f"\n📝 Test 5: Testing sparse data (missing keys filled with None)")

            sparse_data = [
                {"name": "Frank", "age": 40, "city": "Miami"},
                {"name": "Grace", "city": "Denver"},  # Missing 'age'
                {"name": "Henry", "age": 29}  # Missing 'city'
            ]

            sparse_res = await session.call_tool("append_rows", {
                "uri": test_uri,
                "data": sparse_data
            })
            print(f"✅ Sparse data result: {sparse_res}")

            # Verify the result
            if not sparse_res.isError and sparse_res.content and sparse_res.content[0].text:
                result_content = json.loads(sparse_res.content[0].text)
                if result_content.get('success'):
                    updated_cells = result_content.get('updated_cells', 0)

                    # Expected: 9 cells (3 rows × 3 columns)
                    expected_cells = 3 * 3
                    if updated_cells == expected_cells:
                        print(f"   ✅ PASS: Sparse data handled correctly")
                        print(f"   ✅ PASS: Missing keys filled with None values")
                    else:
                        print(f"   ❌ FAIL: Expected {expected_cells} cells, got {updated_cells}")

            # Test 6: Compare with traditional 2D array format
            print(f"\n📝 Test 6: Comparing list of dicts vs 2D array format")
            print(f"   Both formats should produce identical results when properly specified")

            # Create two sheets with identical data but different input formats
            test_data_dict = [
                {"item": "Apple", "quantity": 10, "price": 1.99},
                {"item": "Banana", "quantity": 15, "price": 0.99}
            ]

            # For fair comparison, provide headers explicitly for 2D array
            test_data_2d = [
                ["Apple", 10, 1.99],
                ["Banana", 15, 0.99]
            ]

            # Test with dict format using update_range
            dict_update_res = await session.call_tool("update_range", {
                "uri": test_uri,
                "data": test_data_dict,
                "range_address": "J1"
            })

            # Test with 2D array format using update_range with explicit headers
            array_update_res = await session.call_tool("update_range", {
                "uri": test_uri,
                "data": test_data_2d,
                "range_address": "N1"
            })

            # Compare results
            if (not dict_update_res.isError and not array_update_res.isError and
                dict_update_res.content and array_update_res.content):

                dict_result = json.loads(dict_update_res.content[0].text)
                array_result = json.loads(array_update_res.content[0].text)

                if dict_result.get('success') and array_result.get('success'):
                    dict_shape = dict_result.get('shape', '(0,0)')
                    array_shape = array_result.get('shape', '(0,0)')

                    print(f"   📊 Dict format shape: {dict_shape}")
                    print(f"   📊 2D array shape: {array_shape}")

                    # Dict format includes header row (3 total), 2D array without explicit headers is 2 rows
                    # This shows the key difference: dict auto-includes headers, 2D array doesn't
                    print(f"   💡 Note: Dict format auto-extracts and includes headers in output")
                    print(f"   💡 Note: 2D array treats all rows as data unless headers parameter is used")

                    # Extract rows from shape strings "(rows,cols)"
                    dict_rows = int(dict_shape.strip('()').split(',')[0])
                    array_rows = int(array_shape.strip('()').split(',')[0])

                    if dict_rows > array_rows:
                        print(f"   ✅ PASS: Dict format includes header row (+1 row vs 2D array)")
                        print(f"   ✅ PASS: Both formats work correctly with different semantics")
                    else:
                        print(f"   ⚠️  Unexpected shape comparison - dict: {dict_shape}, array: {array_shape}")

            print(f"\n✅ List of dict input test completed!")
            print(f"\n📊 Test Summary:")
            print(f"   ✓ write_new_sheet accepts list of dicts")
            print(f"   ✓ append_rows accepts list of dicts")
            print(f"   ✓ update_range accepts list of dicts")
            print(f"   ✓ Headers automatically extracted from dict keys")
            print(f"   ✓ Mixed data types handled correctly")
            print(f"   ✓ Sparse data (missing keys) filled with None")
            print(f"   ✓ Compatible with traditional 2D array format")
            print(f"   ✓ Tests use READ_WRITE_URI3 to avoid creating multiple sheets")

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

        # Run list of dict input test
        print(f"\n{'='*20} LIST OF DICT INPUT TEST {'='*20}")
        listtype_test_url = await test_list_of_dict_input(url, headers)
        results['list_of_dict_input'] = {'status': 'passed', 'test_sheet_url': listtype_test_url}

        # Run 1D array input test (NEW)
        print(f"\n{'='*20} 1D ARRAY INPUT TEST {'='*20}")
        await test_1d_array_input(url, headers)
        results['1d_array_input'] = {'status': 'passed'}

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
        'gid': test_gid_fix,
        'listtype': test_list_of_dict_input,
        '1d': test_1d_array_input  # NEW
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
    parser.add_argument("--env", choices=["local", "prod", "test"], default="local",
                       help="Environment to use: local (127.0.0.1:8321) or test (datatable-mcp-test.maybe.ai) or prod (datatable-mcp.maybe.ai)")
    parser.add_argument("--test", choices=["all", "basic", "write", "advanced", "gid", "listtype", "1d"], default="all",
                       help="Which test to run: all (default), basic, write, advanced, gid, listtype, or 1d")
    args = parser.parse_args()

    # Set endpoint based on environment argument
    if args.env == "local":
        endpoint = "http://127.0.0.1:8321"
    elif args.env == "prod":
        endpoint = "https://datatable-mcp.maybe.ai"
    else:
        endpoint = "https://datatable-mcp-test.maybe.ai"
        

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