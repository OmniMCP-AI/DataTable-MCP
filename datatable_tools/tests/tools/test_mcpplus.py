#!/usr/bin/env python3
"""
MCP++ Bridge Layer Test - DataFrame to Google Sheets Integration

Tests the mcpplus function's ability to convert pandas DataFrames to list[list]
format for MCP tool calls.

Usage:
    python test_mcpplus.py
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
import polars as pl
from datetime import datetime
import os
import sys

# Add parent directories to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from datatable_tools.tools.mcp_tools import MCPPlus, query_user_oauth_info, query_user_oauth_info_by_sse, call_tool_by_sse

# Test configuration constants
TEST_USER_ID = "685a7a18ec9b2c667f66d4bd"
TEST_MCP_ENDPOINT = "https://datatable-mcp.maybe.ai/mcp" 
# TEST_MCP_ENDPOINT = "http://127.0.0.1:8321/mcp"
TEST_SSE_URL = "https://be-dev.omnimcp.ai/api/v1/mcp/a6ebdc49-50e7-4c54-8d2a-639f10098a63/68d688ee3bced208d241bef6/sse"
READ_WRITE_URI = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=265933634#gid=265933634"
TWITTER_CASE_URI = "https://docs.google.com/spreadsheets/d/11vMSi4drpuUBFwlfwpytKSuJjZU8tUhbphtUFNnN13U/edit?gid=0#gid=0"

async def test_mcpplus_dataframe_conversion(url, headers):
    """Test MCP++ bridge with DataFrame input to write_new_sheet"""
    print(f"🚀 Testing MCP++ DataFrame Conversion")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with MCPPlus(read, write) as session:
            await session.initialize()

            # Test 1: Create DataFrame and convert via mcpplus
            print(f"\n📝 Test 1: DataFrame to write_new_sheet via MCP++")

            # Create a pandas DataFrame
            df = pl.DataFrame({
                'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
                'department': ['Engineering', 'Sales', 'Marketing', 'HR'],
                'salary': [120000, 85000, 75000, 90000],
                'start_date': ['2020-01-15', '2021-03-22', '2019-07-01', '2022-05-10']
            })

            print(f"   📊 Input DataFrame:")
            print(f"      Shape: {df.shape}")
            print(f"      Columns: {df.columns.tolist()}")
            print(f"      Data preview:")
            print(df.to_string(index=False))

            # Call write_new_sheet through MCP++ bridge
            # The bridge should automatically convert DataFrame to list[list]
            result = await session.call_tool('write_new_sheet', {
                'data': df,  # Pass DataFrame directly
                'headers': df.columns.tolist(),
                'sheet_name': f'MCP++ Test {timestamp}'
            })

            print(f"\n✅ MCP++ call completed")
            print(f"   Raw result: {result}")

            # Parse and verify result
            if result.content and result.content[0].text:
                result_content = json.loads(result.content[0].text)

                if result_content.get('success'):
                    print(f"\n   ✅ SUCCESS: DataFrame converted and sheet created!")
                    print(f"      Spreadsheet URL: {result_content.get('spreadsheet_url')}")
                    print(f"      Rows created: {result_content.get('rows_created')}")
                    print(f"      Columns created: {result_content.get('columns_created')}")
                    print(f"      Data shape: {result_content.get('data_shape')}")

                    # Verify dimensions match DataFrame
                    expected_rows = len(df)
                    expected_cols = len(df.columns)
                    actual_rows = result_content.get('rows_created', 0)
                    actual_cols = result_content.get('columns_created', 0)

                    if actual_rows == expected_rows and actual_cols == expected_cols:
                        print(f"\n   ✅ PASS: Dimensions match DataFrame shape")
                        print(f"      Expected: {expected_rows}x{expected_cols}")
                        print(f"      Got: {actual_rows}x{actual_cols}")
                    else:
                        print(f"\n   ⚠️  WARNING: Dimension mismatch")
                        print(f"      Expected: {expected_rows}x{expected_cols}")
                        print(f"      Got: {actual_rows}x{actual_cols}")
                else:
                    print(f"\n   ❌ FAIL: {result_content.get('message')}")
                    print(f"      Error: {result_content.get('error')}")
            else:
                print(f"\n   ❌ FAIL: No content in response")

            # # Test 2: Verify the conversion happened correctly
            # print(f"\n📝 Test 2: Verify DataFrame to list[list] conversion")

            # # Manual conversion check
            # expected_list = df.values.tolist()
            # print(f"   Expected conversion result (first 2 rows):")
            # for i, row in enumerate(expected_list[:2]):
            #     print(f"      Row {i}: {row}")

            # # Test the _convert_value method directly
            # converted = session._convert_value(df)
            # print(f"\n   Actual converted result (first 2 rows):")
            # for i, row in enumerate(converted[:2]):
            #     print(f"      Row {i}: {row}")

            # if converted == expected_list:
            #     print(f"\n   ✅ PASS: DataFrame converted correctly to list[list]")
            # else:
            #     print(f"\n   ❌ FAIL: Conversion mismatch")

            # # Test 3: Test with Series input
            # print(f"\n📝 Test 3: Series to list conversion")

            # series = pd.Series([100, 200, 300, 400], name='test_values')
            # converted_series = session._convert_value(series)
            # expected_series = series.tolist()

            # print(f"   Input Series: {series.tolist()}")
            # print(f"   Converted: {converted_series}")
            # print(f"   Expected: {expected_series}")

            # if converted_series == expected_series:
            #     print(f"   ✅ PASS: Series converted correctly to list")
            # else:
            #     print(f"   ❌ FAIL: Series conversion mismatch")

            print(f"\n✅ MCP++ DataFrame conversion test completed!")


async def test_mcpplus_append_and_update(url, headers):
    """Test MCP++ with append_rows and update_range using DataFrames"""
    print(f"\n🚀 Testing MCP++ append_rows and update_range")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with MCPPlus(read, write) as session:
            await session.initialize()

            # Test 1: append_rows with DataFrame
            print(f"\n📝 Test 1: append_rows with DataFrame via MCP++")

            # Create sample data to append
            df_append = pl.DataFrame({
                'product': ['Widget X', 'Gadget Y', 'Tool Z'],
                'price': [49.99, 89.99, 24.99],
                'category': ['Hardware', 'Electronics', 'Tools'],
                'stock': [150, 75, 200],
                'last_updated': [timestamp, timestamp, timestamp]
            })

            print(f"   📊 DataFrame to append:")
            print(f"      Shape: {df_append.shape}")
            print(df_append.to_string(index=False))

            # Append rows using DataFrame
            result_append = await session.call_tool('append_rows', {
                'uri': READ_WRITE_URI,
                'data': df_append  # DataFrame will be auto-converted
            })

            print(f"\n✅ append_rows result: {result_append}")

            if result_append.content and result_append.content[0].text:
                if result_append.isError:
                    print(f"   ❌ ERROR: {result_append.content[0].text}")
                else:
                    content = json.loads(result_append.content[0].text)
                    if content.get('success'):
                        print(f"   ✅ SUCCESS: DataFrame appended!")
                        print(f"      Rows appended: {content.get('rows_appended', 0)}")
                        print(f"      Updated range: {content.get('range', 'N/A')}")
                    else:
                        print(f"   ❌ FAIL: {content.get('message')}")

            # Test 2: update_range with DataFrame
            print(f"\n📝 Test 2: update_range with DataFrame via MCP++")

            # Create sample data for range update
            df_update = pl.DataFrame({
                'status': ['Active', 'Pending', 'Completed'],
                'rating': [4.5, 4.0, 5.0],
                'notes': ['Excellent', 'Good', 'Perfect']
            })

            print(f"   📊 DataFrame to update:")
            print(f"      Shape: {df_update.shape}")
            print(df_update.to_string(index=False))

            # Update specific range with DataFrame
            result_update = await session.call_tool('update_range', {
                'uri': READ_WRITE_URI,
                'data': df_update,  # DataFrame will be auto-converted
                'range_address': 'G5:I7'  # Specific range
            })

            print(f"\n✅ update_range result: {result_update}")

            if result_update.content and result_update.content[0].text:
                if result_update.isError:
                    print(f"   ❌ ERROR: {result_update.content[0].text}")
                else:
                    content = json.loads(result_update.content[0].text)
                    if content.get('success'):
                        print(f"   ✅ SUCCESS: DataFrame updated in range!")
                        print(f"      Updated cells: {content.get('updated_cells', 0)}")
                        print(f"      Range: {content.get('range', 'N/A')}")
                        print(f"      Data shape: {content.get('data_shape', 'N/A')}")
                    else:
                        print(f"   ❌ FAIL: {content.get('message')}")

            # # Test 3: append_rows with Series (single column)
            # print(f"\n📝 Test 3: append_rows with Series via MCP++")

            # # Create a Series for single column append
            # series_data = pd.Series([
            #     'Comment 1: Great product',
            #     'Comment 2: Fast delivery',
            #     'Comment 3: Will buy again'
            # ], name='comments')

            # print(f"   📊 Series to append (will be converted to column):")
            # print(f"      Length: {len(series_data)}")
            # print(series_data.to_string(index=False))

            # # Need to convert Series to DataFrame with single column for append_rows
            # df_series = pd.DataFrame(series_data)

            # result_series = await session.call_tool('append_rows', {
            #     'uri': READ_WRITE_URI,
            #     'data': df_series
            # })

            # print(f"\n✅ append_rows with Series result: {result_series}")

            # if result_series.content and result_series.content[0].text:
            #     content = json.loads(result_series.content[0].text)
            #     if content.get('success'):
            #         print(f"   ✅ SUCCESS: Series appended as column!")
            #         print(f"      Rows appended: {content.get('rows_appended', 0)}")
            #     else:
            #         print(f"   ❌ FAIL: {content.get('message')}")

            # # Test 4: update_range with mixed data types in DataFrame
            # print(f"\n📝 Test 4: update_range with mixed data types")

            # # Create DataFrame with various types
            # df_mixed = pd.DataFrame({
            #     'id': [101, 102, 103],
            #     'name': ['Alice', 'Bob', 'Charlie'],
            #     'active': [True, False, True],
            #     'balance': [1250.50, 890.25, 2100.00],
            #     'timestamp': [timestamp, timestamp, timestamp]
            # })

            # print(f"   📊 DataFrame with mixed types:")
            # print(f"      Shape: {df_mixed.shape}")
            # print(f"      Types: {df_mixed.dtypes.to_dict()}")
            # print(df_mixed.to_string(index=False))

            # result_mixed = await session.call_tool('update_range', {
            #     'uri': READ_WRITE_URI,
            #     'data': df_mixed,
            #     'range_address': 'A10:E12'
            # })

            # print(f"\n✅ update_range with mixed types result: {result_mixed}")

            # if result_mixed.content and result_mixed.content[0].text:
            #     content = json.loads(result_mixed.content[0].text)
            #     if content.get('success'):
            #         print(f"   ✅ SUCCESS: Mixed type DataFrame updated!")
            #         print(f"      Updated cells: {content.get('updated_cells', 0)}")
            #         print(f"      Expected: {df_mixed.shape[0] * df_mixed.shape[1]} cells")
            #     else:
            #         print(f"   ❌ FAIL: {content.get('message')}")

            print(f"\n✅ MCP++ append and update tests completed!")


async def test_mcpplus_update_spreadsheet_rows(url, headers):
    """Test MCP++ updating spreadsheet with real-world data row by row"""
    print(f"\n🚀 Testing MCP++ update_range for spreadsheet rows")
    print("=" * 60)

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with MCPPlus(read, write) as session:
            await session.initialize()

            # Real-world spreadsheet data
            data = {
                'URL': [
                    'https://x.com/topickai/status/1942203390001586186',
                    'https://x.com/AndrewYNg/status/1960731961494004077',
                    'https://x.com/Rejweb3/status/1960213278564741596',
                    'https://x.com/vista8/status/1960206485553893608',
                    'https://x.com/dotey/status/1960170374219309333',
                    'https://x.com/axichuhai/status/1960192885623758950'
                ],
                'Status': ['Completed'] * 6,
                'Infographics URL': [
                    'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/94b48778-73ec-451a-b9df-041841114439.jpeg',
                    'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/0a6e4c8a-3895-48d8-9cd6-b076ec1707eb.jpeg',
                    'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/2785b46c-546e-46a2-88e9-9a408dc7a307.jpeg',
                    'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/c5b633e9-3188-4eb4-b8d8-6b0319422555.jpeg',
                    'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/cf2e70dc-5b71-4428-aff8-4c21337de572.jpeg',
                    'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/ea6cd84d-5470-496a-b25b-a068019bf95b.jpeg'
                ],
                'Tweet': [
                    'AI agents and workflows both solve real problems, but the business and psychological dynamics are different. Workflows optimize for reliability. Agents generate engagement by surfacing new behavior and motivating exploration. A shift from proven efficiency to emergent opportunity.',
                    'AI agents that extract and link data into knowledge graphs make RAG systems much more accurate. A lot of recent progress on building these automatically from unstructured sources. Impressive improvement over vector-only retrieval.',
                    'Watching the Fed shift towards rate cuts this cycle. Effects on capital markets and liquidity will likely shape both short-term market moves and long-run tech investment strategies. Macro changes often show up in unexpected ways for new technologies.',
                    'Building in AI is never boring—met a former architect now creating generative video and a lawyer turned prompt engineer, both thriving. The feedback loops in this field make every transition rewarding. The energy still surprises me.',
                    'Having a command-line agent and one built into an IDE each changes how coders interact with AI. Context handling, token usage, and model selection all shape outcomes. The different choices reflect broader shifts in how we build and scale AI productivity.',
                    'watching inference and training costs drop while capability keeps skyrocketing is wild\nthis phase of ai feels like the early internet: new things possible every month, hard to predict what seems obvious in retrospect'
                ]
            }

            df = pl.DataFrame(data)

            print(f"   📊 Spreadsheet data to update:")
            print(f"      Shape: {df.shape}")
            print(f"      Columns: {df.columns.tolist()}")
            print(f"      Rows: {len(df)}")

            # Test 1: Update entire DataFrame at once
            print(f"\n📝 Test 1: Update entire DataFrame to spreadsheet")

            result_full = await session.call_tool('update_range', {
                'uri': TWITTER_CASE_URI,
                'data': df,
                'range_address': 'A2:D7'  # A1:D7 includes header row + 6 data rows
            })

            print(f"\n✅ Full DataFrame update result: {result_full}")

            if result_full.content and result_full.content[0].text:
                try:
                    content = json.loads(result_full.content[0].text)
                    if content.get('success'):
                        print(f"   ✅ SUCCESS: Full DataFrame updated!")
                        print(f"      Updated cells: {content.get('updated_cells', 0)}")
                        print(f"      Range: {content.get('range', 'N/A')}")
                        print(f"      Data shape: {content.get('data_shape', 'N/A')}")
                    else:
                        print(f"   ❌ FAIL: {content.get('message')}")
                except json.JSONDecodeError:
                    # Handle error responses that aren't JSON
                    print(f"   ❌ ERROR: {result_full.content[0].text}")
            else:
                print(f"   ❌ FAIL: No content in response")

            # # Test 2: Update individual rows using DataFrame slices
            # print(f"\n📝 Test 2: Update rows individually using DataFrame slices")

            # for idx, row in df.iterrows():
            #     row_num = idx + 2  # Start from row 2 (row 1 is headers)

            #     # Create single-row DataFrame
            #     row_df = pd.DataFrame([row.tolist()], columns=df.columns)

            #     print(f"\n   Updating row {row_num} (index {idx}):")
            #     print(f"      URL: {row['URL'][:50]}...")
            #     print(f"      Tweet preview: {row['Tweet'][:60]}...")

            #     result_row = await session.call_tool('update_range', {
            #         'uri': TWITTER_CASE_URI,
            #         'data': row_df,
            #         'range_address': f'A{row_num}:D{row_num}'
            #     })

            #     if result_row.content and result_row.content[0].text:
            #         try:
            #             content = json.loads(result_row.content[0].text)
            #             if content.get('success'):
            #                 print(f"      ✅ Row {row_num} updated successfully")
            #                 print(f"         Cells: {content.get('updated_cells', 0)}")
            #             else:
            #                 print(f"      ❌ Row {row_num} failed: {content.get('message')}")
            #         except json.JSONDecodeError:
            #             print(f"      ❌ ERROR: {result_row.content[0].text}")

            # # Test 3: Update specific column using Series
            # print(f"\n📝 Test 3: Update single column (Status) using Series")

            # status_series = df['Status']
            # status_df = pd.DataFrame(status_series)

            # result_column = await session.call_tool('update_range', {
            #     'uri': TWITTER_CASE_URI,
            #     'data': status_df,
            #     'range_address': 'B2:B7'  # Column B, rows 2-7
            # })

            # print(f"\n✅ Column update result: {result_column}")

            # if result_column.content and result_column.content[0].text:
            #     try:
            #         content = json.loads(result_column.content[0].text)
            #         if content.get('success'):
            #             print(f"   ✅ SUCCESS: Status column updated!")
            #             print(f"      Updated cells: {content.get('updated_cells', 0)}")
            #         else:
            #             print(f"   ❌ FAIL: {content.get('message')}")
            #     except json.JSONDecodeError:
            #         print(f"   ❌ ERROR: {result_column.content[0].text}")

            # # Test 4: Update with partial DataFrame (subset of columns)
            # print(f"\n📝 Test 4: Update subset of columns (URL and Status only)")

            # df_subset = df[['URL', 'Status']]

            # result_subset = await session.call_tool('update_range', {
            #     'uri': TWITTER_CASE_URI,
            #     'data': df_subset,
            #     'range_address': 'A10:B16'  # Different location
            # })

            # print(f"\n✅ Subset update result: {result_subset}")

            # if result_subset.content and result_subset.content[0].text:
            #     try:
            #         content = json.loads(result_subset.content[0].text)
            #         if content.get('success'):
            #             print(f"   ✅ SUCCESS: Subset columns updated!")
            #             print(f"      Updated cells: {content.get('updated_cells', 0)}")
            #             print(f"      Expected: {df_subset.shape[0] * df_subset.shape[1]} cells")
            #         else:
            #             print(f"   ❌ FAIL: {content.get('message')}")
            #     except json.JSONDecodeError:
            #         print(f"   ❌ ERROR: {result_subset.content[0].text}")

            print(f"\n✅ Spreadsheet row update tests completed!")


async def test_oauth_query_integration():
    """Test the new OAuth query integration functionality"""
    print(f"\n🚀 Testing OAuth Query Integration (Stage 2)")
    print("=" * 60)
    
    # Test user ID from the requirement document
    provider = "google_sheets"
    
    print(f"   👤 User ID: {TEST_USER_ID}")
    print(f"   🔧 Provider: {provider}")
    
    try:
        # Query OAuth info from omnimcp_be API
        print(f"\n📡 Querying OAuth info from omnimcp_be API...")
        
        oauth_result = await query_user_oauth_info(user_id = TEST_USER_ID, provider_name = provider)
        
        print(f"✅ OAuth query successful!")
        print(f"   Success: {oauth_result.get('success')}")
        print(f"   User ID: {oauth_result.get('user_id')}")
        print(f"   Provider: {oauth_result.get('provider')}")
        print(f"   Message: {oauth_result.get('message')}")
        
        # Extract auth_info for use as headers
        auth_info = oauth_result.get('auth_info', {})
        if auth_info:
            print(f"\n🔑 Auth info retrieved:")
            print(f"   GOOGLE_OAUTH_CLIENT_ID: {auth_info.get('GOOGLE_OAUTH_CLIENT_ID', 'N/A')}")
            print(f"   GOOGLE_OAUTH_CLIENT_SECRET: {'***' if auth_info.get('GOOGLE_OAUTH_CLIENT_SECRET') else 'N/A'}")
            print(f"   GOOGLE_OAUTH_REFRESH_TOKEN: {'***' if auth_info.get('GOOGLE_OAUTH_REFRESH_TOKEN') else 'N/A'}")
            
            # Test using the retrieved OAuth info with MCP++
            print(f"\n📝 Testing MCP++ with retrieved OAuth credentials...")
            
            # Use local endpoint for testing
            endpoint = "https://datatable-mcp.maybe.ai/mcp"
            
            async with streamablehttp_client(url=f"{endpoint}/mcp", headers=auth_info) as (read, write, _):
                async with MCPPlus(read, write) as session:
                    await session.initialize()
                    
                    # Test with a simple DataFrame
                    df_test = pl.DataFrame({
                        'test_column': ['OAuth Test 1', 'OAuth Test 2'],
                        'timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 2
                    })
                    
                    print(f"   📊 Test DataFrame:")
                    print(f"      Shape: {df_test.shape}")
                    print(df_test.to_string(index=False))
                    
                    # Create a new sheet with OAuth credentials
                    result = await session.call_tool('write_new_sheet', {
                        'data': df_test,
                        'headers': df_test.columns.tolist(),
                        'sheet_name': f'OAuth Integration Test {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                    })
                    
                    print(f"\n✅ MCP++ call with OAuth credentials completed")
                    
                    if result.content and result.content[0].text:
                        try:
                            result_content = json.loads(result.content[0].text)
                            if result_content.get('success'):
                                print(f"   ✅ SUCCESS: Sheet created with OAuth credentials!")
                                print(f"      Spreadsheet URL: {result_content.get('spreadsheet_url')}")
                                print(f"      Rows created: {result_content.get('rows_created')}")
                                print(f"      Columns created: {result_content.get('columns_created')}")
                            else:
                                print(f"   ❌ FAIL: {result_content.get('message')}")
                        except json.JSONDecodeError:
                            print(f"   ❌ ERROR: {result.content[0].text}")
                    else:
                        print(f"   ❌ FAIL: No content in response")
        else:
            print(f"   ⚠️  WARNING: No auth_info in response")
            
    except Exception as e:
        print(f"❌ OAuth query failed: {str(e)}")
        print(f"   This might be expected if the API is not accessible or user doesn't exist")
        print(f"   Error type: {type(e).__name__}")
    
    print(f"\n✅ OAuth integration test completed!")


async def test_simplified_call_tool():
    """Test the new simplified call_tool function (Stage 4)"""
    print(f"\n🚀 Testing Simplified call_tool Function (Stage 4)")
    print("=" * 60)

    provider = "google_sheets"

    print(f"   👤 User ID: {TEST_USER_ID}")
    print(f"   🔧 Provider: {provider}")

    try:
        # Create test DataFrame
        df_test = pl.DataFrame({
            'URL': [
                'https://x.com/topickai/status/1942203390001586186',
                'https://x.com/AndrewYNg/status/1960731961494004077'
            ],
            'Status': ['Completed'] * 2,
            'Infographics URL': [
                'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/94b48778-73ec-451a-b9df-041841114439.jpeg',
                'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/0a6e4c8a-3895-48d8-9cd6-b076ec1707eb.jpeg'
            ],
            'Tweet': [
                'AI agents and workflows both solve real problems...',
                'AI agents that extract and link data into knowledge graphs...'
            ]
        })

        print(f"\n   📊 Test DataFrame:")
        print(f"      Shape: {df_test.shape}")
        print(f"      Columns: {df_test.columns.tolist()}")

        # Use the simplified call_tool function - one line does it all!
        print(f"\n📡 Calling tool with simplified API...")

        result = await call_tool(
            user_id=TEST_USER_ID,
            provider=provider,
            tool_name="update_range",
            args={
                'uri': TWITTER_CASE_URI,
                'data': df_test,
                'range_address': 'A2:D3'
            },
            mcp_endpoint= TEST_MCP_ENDPOINT
        )

        print(f"\n✅ Simplified call_tool completed!")
        print(f"   Result: {result}")

        if result.content and result.content[0].text:
            try:
                result_content = json.loads(result.content[0].text)
                if result_content.get('success'):
                    print(f"\n   ✅ SUCCESS: Tool called successfully with simplified API!")
                    print(f"      Updated cells: {result_content.get('updated_cells', 0)}")
                    print(f"      Range: {result_content.get('range', 'N/A')}")
                    print(f"      Data shape: {result_content.get('data_shape', 'N/A')}")
                else:
                    print(f"\n   ❌ FAIL: {result_content.get('message')}")
            except json.JSONDecodeError:
                print(f"\n   ❌ ERROR: {result.content[0].text}")
        else:
            print(f"\n   ❌ FAIL: No content in response")

    except Exception as e:
        print(f"❌ Simplified call_tool failed: {str(e)}")
        print(f"   Error type: {type(e).__name__}")

    print(f"\n✅ Simplified call_tool test completed!")


async def test_query_oauth_by_sse():
    """Test the new query_user_oauth_info_by_sse function"""
    print(f"\n🚀 Testing query_user_oauth_info_by_sse Function")
    print("=" * 60)

    provider = "google_sheets"

    print(f"   🔗 SSE URL: {TEST_SSE_URL}")
    print(f"   🔧 Provider: {provider}")

    try:
        # Query OAuth info using SSE URL
        print(f"\n📡 Querying OAuth info by SSE URL...")

        oauth_result = await query_user_oauth_info_by_sse(
            sse_url=TEST_SSE_URL,
            provider=provider
        )

        print(f"\n✅ OAuth query by SSE successful!")
        print(f"   Success: {oauth_result.get('success')}")
        print(f"   User ID: {oauth_result.get('user_id', 'N/A')}")
        print(f"   Provider: {oauth_result.get('provider')}")
        print(f"   Message: {oauth_result.get('message')}")

        # Extract auth_info for use as headers
        auth_info = oauth_result.get('auth_info', {})
        if auth_info:
            print(f"\n🔑 Auth info retrieved:")
            print(f"   GOOGLE_OAUTH_CLIENT_ID: {auth_info.get('GOOGLE_OAUTH_CLIENT_ID', 'N/A')}")
            print(f"   GOOGLE_OAUTH_CLIENT_SECRET: {'***' if auth_info.get('GOOGLE_OAUTH_CLIENT_SECRET') else 'N/A'}")
            print(f"   GOOGLE_OAUTH_REFRESH_TOKEN: {'***' if auth_info.get('GOOGLE_OAUTH_REFRESH_TOKEN') else 'N/A'}")

            print(f"\n✅ SUCCESS: OAuth query by SSE URL works correctly!")
        else:
            print(f"   ⚠️  WARNING: No auth_info in response")

    except Exception as e:
        print(f"❌ OAuth query by SSE failed: {str(e)}")
        print(f"   Error type: {type(e).__name__}")

    print(f"\n✅ OAuth query by SSE test completed!")


async def test_call_tool_by_sse():
    """Test the new call_tool_by_sse function"""
    print(f"\n🚀 Testing call_tool_by_sse Function")
    print("=" * 60)

    provider = "google_sheets"

    print(f"   🔗 SSE URL: {TEST_SSE_URL}")
    print(f"   🔧 Provider: {provider}")

    try:
        # Create test DataFrame
        df_test = pl.DataFrame({
            'URL': [
                'https://x.com/topickai/status/1942203390001586186',
                'https://x.com/AndrewYNg/status/1960731961494004077'
            ],
            'Status': ['Completed'] * 2,
            'Infographics URL': [
                'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/94b48778-73ec-451a-b9df-041841114439.jpeg',
                'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/0a6e4c8a-3895-48d8-9cd6-b076ec1707eb.jpeg'
            ],
            'Tweet': [
                'AI agents and workflows both solve real problems...',
                'AI agents that extract and link data into knowledge graphs...'
            ]
        })

        print(f"\n   📊 Test DataFrame:")
        print(f"      Shape: {df_test.shape}")
        print(f"      Columns: {df_test.columns.tolist()}")

        # Use the call_tool_by_sse function - simplest API using SSE URL directly!
        print(f"\n📡 Calling tool with SSE URL...")

        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__update_range",
            args={
                'uri': TWITTER_CASE_URI,
                'data': df_test,
                'range_address': 'A2:D3'
            }
        )

        print(f"\n✅ call_tool_by_sse completed!")
        print(f"   Result: {result}")

        if result.structuredContent:
            result_content = result.structuredContent
            if result_content.get('success'):
                print(f"\n   ✅ SUCCESS: Tool called successfully using SSE URL!")
                print(f"      Updated cells: {result_content.get('updated_cells', 0)}")
                print(f"      Range: {result_content.get('range', 'N/A')}")
                print(f"      Data shape: {result_content.get('data_shape', 'N/A')}")
            else:
                print(f"\n   ❌ FAIL: {result_content.get('message')}")
        elif result.content and result.content[0].text:
            try:
                result_content = json.loads(result.content[0].text)
                if result_content.get('success'):
                    print(f"\n   ✅ SUCCESS: Tool called successfully using SSE URL!")
                    print(f"      Updated cells: {result_content.get('updated_cells', 0)}")
                    print(f"      Range: {result_content.get('range', 'N/A')}")
                    print(f"      Data shape: {result_content.get('data_shape', 'N/A')}")
                else:
                    print(f"\n   ❌ FAIL: {result_content.get('message')}")
            except json.JSONDecodeError:
                print(f"\n   ❌ ERROR: {result.content[0].text}")
        else:
            print(f"\n   ❌ FAIL: No content in response")

       
    except Exception as e:
        print(f"❌ call_tool_by_sse failed: {str(e)}")
        print(f"   Error type: {type(e).__name__}")

    print(f"\n✅ call_tool_by_sse test completed!")


async def test_call_tool_write_new_sheet():
    """Test the call_tool_by_sse function with google_sheets__write_new_sheet"""
    print(f"\n🚀 Testing call_tool_by_sse with write_new_sheet")
    print("=" * 60)

    provider = "google_sheets"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"   🔗 SSE URL: {TEST_SSE_URL}")
    print(f"   🔧 Provider: {provider}")

    try:
        # Create test list_data (case2 format)
        list_data = [
            ["name", "description", "category"],  # Short headers
            ["Widget Pro", "This is a comprehensive widget with advanced features and capabilities", "Electronics"],
            ["Gadget Max", "A revolutionary gadget that transforms the way you work and increases productivity", "Tools"],
            ["Device Ultra", "State-of-the-art device with cutting-edge technology and premium materials", "Hardware"]
        ]

        print(f"\n   📊 Test List Data:")
        print(f"      Rows: {len(list_data)}")
        print(f"      First row (headers): {list_data[0]}")
        print(f"      Sample data row: {list_data[1]}")

        # Use the call_tool_by_sse function for write_new_sheet
        print(f"\n📡 Calling write_new_sheet tool with SSE URL...")

        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__write_new_sheet",
            args={
                'data': list_data,
                'headers': None,  # Let it auto-detect
                'sheet_name': f'MCP++ SSE Test {timestamp}'
            }
        )

        print(f"\n✅ call_tool_by_sse write_new_sheet completed!")
        print(f"   Result: {result}")

        if result.structuredContent:
            result_content = result.structuredContent
            if result_content.get('success'):
                print(f"\n   ✅ SUCCESS: write_new_sheet called successfully using SSE URL!")
                print(f"      Sheet name: {result_content.get('sheet_name', 'N/A')}")
                print(f"      Sheet URL: {result_content.get('sheet_url', 'N/A')}")
                print(f"      Rows written: {result_content.get('rows_written', 0)}")
                print(f"      Data shape: {result_content.get('data_shape', 'N/A')}")
            else:
                print(f"\n   ❌ FAIL: {result_content.get('message')}")
        elif result.content and result.content[0].text:
            try:
                result_content = json.loads(result.content[0].text)
                if result_content.get('success'):
                    print(f"\n   ✅ SUCCESS: write_new_sheet called successfully using SSE URL!")
                    print(f"      Sheet name: {result_content.get('sheet_name', 'N/A')}")
                    print(f"      Sheet URL: {result_content.get('sheet_url', 'N/A')}")
                    print(f"      Rows written: {result_content.get('rows_written', 0)}")
                    print(f"      Data shape: {result_content.get('data_shape', 'N/A')}")
                else:
                    print(f"\n   ❌ FAIL: {result_content.get('message')}")
            except json.JSONDecodeError:
                print(f"\n   ❌ ERROR: {result.content[0].text}")
        else:
            print(f"\n   ❌ FAIL: No content in response")

       
    except Exception as e:
        print(f"❌ call_tool_by_sse write_new_sheet failed: {str(e)}")
        print(f"   Error type: {type(e).__name__}")

    print(f"\n✅ call_tool_write_new_sheet test completed!")


async def test_call_tool_append_rows():
    """Test the call_tool_by_sse function with google_sheets__append_rows"""
    print(f"\n🚀 Testing call_tool_by_sse with append_rows")
    print("=" * 60)

    provider = "google_sheets"

    print(f"   🔗 SSE URL: {TEST_SSE_URL}")
    print(f"   🔧 Provider: {provider}")
    print(f"   📄 Target Sheet: {TWITTER_CASE_URI}")

    try:
        # Create test DataFrame for appending
        df_append = pl.DataFrame({
            'URL': [
                'https://x.com/newuser1/status/1942203390001586999',
                'https://x.com/newuser2/status/1960731961494004888'
            ],
            'Status': ['Pending', 'In Progress'],
            'Infographics URL': [
                'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/new-image-1.jpeg',
                'https://xcelsior-1317942802.cos.ap-singapore.myqcloud.com/infographic/new-image-2.jpeg'
            ],
            'Tweet': [
                'New tweet content about AI automation and workflow optimization...',
                'Another tweet discussing machine learning applications in business...'
            ]
        })

        print(f"\n   📊 Test DataFrame for Appending:")
        print(f"      Shape: {df_append.shape}")
        print(f"      Columns: {df_append.columns}")

        # Use the call_tool_by_sse function for append_rows
        print(f"\n📡 Calling append_rows tool with SSE URL...")

        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__append_rows",
            args={
                'uri': TWITTER_CASE_URI,
                'data': df_append
            }
        )

        print(f"\n✅ call_tool_by_sse append_rows completed!")
        print(f"   Result: {result}")

        if result.structuredContent:
            result_content = result.structuredContent
            if result_content.get('success'):
                print(f"\n   ✅ SUCCESS: append_rows called successfully using SSE URL!")
                print(f"      Updated cells: {result_content.get('updated_cells', 0)}")
                print(f"      Range: {result_content.get('range', 'N/A')}")
                print(f"      Worksheet: {result_content.get('worksheet', 'N/A')}")
                print(f"      Data shape: {result_content.get('data_shape', 'N/A')}")
                print(f"      Message: {result_content.get('message', 'N/A')}")
            else:
                print(f"\n   ❌ FAIL: {result_content.get('message')}")
        elif result.content and result.content[0].text:
            try:
                result_content = json.loads(result.content[0].text)
                if result_content.get('success'):
                    print(f"\n   ✅ SUCCESS: append_rows called successfully using SSE URL!")
                    print(f"      Updated cells: {result_content.get('updated_cells', 0)}")
                    print(f"      Range: {result_content.get('range', 'N/A')}")
                    print(f"      Data shape: {result_content.get('data_shape', 'N/A')}")
                else:
                    print(f"\n   ❌ FAIL: {result_content.get('message')}")
            except json.JSONDecodeError:
                print(f"\n   ❌ ERROR: {result.content[0].text}")
        else:
            print(f"\n   ❌ FAIL: No content in response")

       
    except Exception as e:
        print(f"❌ call_tool_by_sse append_rows failed: {str(e)}")
        print(f"   Error type: {type(e).__name__}")

    print(f"\n✅ call_tool_append_rows test completed!")


async def test_write_new_sheet_comprehensive():
    """Comprehensive test for write_new_sheet tool with various data formats"""
    print(f"\n🚀 Testing write_new_sheet Tool Comprehensively")
    print("=" * 60)
    
    from fastestai.tools.mcpplus import query_user_oauth_info, MCPPlus
    from mcp.client.streamable_http import streamablehttp_client
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Helper function to call tools with OAuth handling
    async def call_tool_with_oauth(tool_name: str, args: dict):
        try:
            # Get OAuth credentials
            oauth_result = await query_user_oauth_info(
                user_id=TEST_USER_ID,
                provider_name="google_sheets"
            )
            headers = oauth_result.get('auth_info', {})
            
            # Call tool using MCP++
            endpoint = "http://127.0.0.1:8321/mcp"
            async with streamablehttp_client(url=endpoint, headers=headers) as (read, write, _):
                async with MCPPlus(read, write) as session:
                    await session.initialize()
                    return await session.call_tool(tool_name, args)
                    
        except Exception as e:
            print(f"   ⚠️  Connection/OAuth error: {type(e).__name__}: {str(e)}")
            return None
    
    # Test 1: DataFrame with mixed data types
    print(f"\n📝 Test 1: DataFrame with mixed data types")

    df_mixed = pl.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'salary': [50000.50, 75000.75, 100000.00],
        'active': [True, False, True],
        'department': ['Engineering', 'Sales', 'Marketing']
    })
    
    print(f"   📊 Input DataFrame:")
    print(f"      Shape: {df_mixed.shape}")
    print(f"      Data types: {df_mixed.dtypes.to_dict()}")
    print(df_mixed.to_string(index=False))
    
    try:
        result1 = await call_tool_with_oauth(
            tool_name="write_new_sheet",
            args={
                'data': df_mixed,
                'headers': df_mixed.columns.tolist(),
                'sheet_name': f'MCP++ Mixed Types Test {timestamp}'
            }
        )
        
        if result1 and result1.content and result1.content[0].text:
            content = json.loads(result1.content[0].text)
            if content.get('success'):
                print(f"   ✅ SUCCESS: Mixed types DataFrame created!")
                print(f"      URL: {content.get('spreadsheet_url')}")
                print(f"      Rows: {content.get('rows_created')}, Cols: {content.get('columns_created')}")
            else:
                print(f"   ❌ FAIL: {content.get('message')}")
        elif result1 is None:
            print(f"   ⚠️  Test skipped due to connection issues")
        else:
            print(f"   ❌ FAIL: No content in response")
        
    except Exception as e:
        print(f"   ❌ ERROR: {str(e)}")
    
    # Test 2: List[List] format with auto-detected headers
    print(f"\n📝 Test 2: List[List] with auto-detected headers")
    
    # Data where first row has short strings, subsequent rows have longer content
    list_data = [
        ["name", "description", "category"],  # Short headers
        ["Widget Pro", "This is a comprehensive widget with advanced features and capabilities", "Electronics"],
        ["Gadget Max", "A revolutionary gadget that transforms the way you work and increases productivity", "Tools"],
        ["Device Ultra", "State-of-the-art device with cutting-edge technology and premium materials", "Hardware"]
    ]
    
    print(f"   📊 Input List[List] data:")
    print(f"      Rows: {len(list_data)}")
    print(f"      First row (headers): {list_data[0]}")
    print(f"      Sample data row: {list_data[1]}")
    
    try:
        result2 = await call_tool_with_oauth(
            tool_name="write_new_sheet",
            args={
                'data': list_data,
                'headers': None,  # Let it auto-detect
                'sheet_name': f'MCP++ Auto Headers Test {timestamp}'
            }
        )
        
        if result2.content and result2.content[0].text:
            content = json.loads(result2.content[0].text)
            if content.get('success'):
                print(f"   ✅ SUCCESS: Auto-detected headers sheet created!")
                print(f"      URL: {content.get('spreadsheet_url')}")
                print(f"      Rows: {content.get('rows_created')}, Cols: {content.get('columns_created')}")
            else:
                print(f"   ❌ FAIL: {content.get('message')}")
        
    except Exception as e:
        print(f"   ❌ ERROR: {str(e)}")
    
    # Test 3: Simple 2D array with explicit headers
    print(f"\n📝 Test 3: Simple 2D array with explicit headers")
    
    simple_data = [
        ["John", 25, "Engineer"],
        ["Jane", 30, "Designer"],
        ["Mike", 28, "Manager"],
        ["Sarah", 32, "Analyst"]
    ]
    headers = ["Name", "Age", "Role"]
    
    print(f"   📊 Input 2D array:")
    print(f"      Data: {simple_data}")
    print(f"      Headers: {headers}")
    
    try:
        result3 = await call_tool_with_oauth(
            tool_name="write_new_sheet",
            args={
                'data': simple_data,
                'headers': headers,
                'sheet_name': f'MCP++ Simple Array Test {timestamp}'
            }
        )
        
        if result3.content and result3.content[0].text:
            content = json.loads(result3.content[0].text)
            if content.get('success'):
                print(f"   ✅ SUCCESS: Simple array sheet created!")
                print(f"      URL: {content.get('spreadsheet_url')}")
                print(f"      Rows: {content.get('rows_created')}, Cols: {content.get('columns_created')}")
            else:
                print(f"   ❌ FAIL: {content.get('message')}")
        
    except Exception as e:
        print(f"   ❌ ERROR: {str(e)}")
    
    # Test 4: DataFrame with None values and special characters
    print(f"\n📝 Test 4: DataFrame with None values and special characters")

    df_special = pl.DataFrame({
        'product': ['Widget™', 'Gadget®', None, 'Device©'],
        'price': [99.99, None, 149.50, 199.99],
        'available': [True, False, None, True],
        'notes': ['Special offer!', None, 'Limited edition', 'Best seller 🏆']
    })
    
    print(f"   📊 Input DataFrame with special cases:")
    print(f"      Shape: {df_special.shape}")
    print(df_special.to_string(index=False))
    
    try:
        result4 = await call_tool_with_oauth(
            tool_name="write_new_sheet",
            args={
                'data': df_special,
                'headers': ['Product', 'Price ($)', 'Available', 'Notes'],
                'sheet_name': f'MCP++ Special Cases Test {timestamp}'
            }
        )
        
        if result4.content and result4.content[0].text:
            content = json.loads(result4.content[0].text)
            if content.get('success'):
                print(f"   ✅ SUCCESS: Special cases sheet created!")
                print(f"      URL: {content.get('spreadsheet_url')}")
                print(f"      Rows: {content.get('rows_created')}, Cols: {content.get('columns_created')}")
            else:
                print(f"   ❌ FAIL: {content.get('message')}")
        
    except Exception as e:
        print(f"   ❌ ERROR: {str(e)}")
    
    # Test 5: Large DataFrame to test performance
    print(f"\n📝 Test 5: Large DataFrame (performance test)")
    
    # Create a larger dataset
    import random
    
    large_data = {
        'id': range(1, 101),
        'name': [f'User_{i}' for i in range(1, 101)],
        'score': [random.randint(0, 100) for _ in range(100)],
        'category': [random.choice(['A', 'B', 'C', 'D']) for _ in range(100)],
        'timestamp': [f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}' for _ in range(100)]
    }

    df_large = pl.DataFrame(large_data)

    print(f"   📊 Large DataFrame:")
    print(f"      Shape: {df_large.shape}")
    print(f"      Sample rows:")
    print(df_large.head(3).to_string(index=False))
    
    try:
        result5 = await call_tool(
            user_id=TEST_USER_ID,
            provider="google_sheets",
            tool_name="write_new_sheet",
            args={
                'data': df_large,
                'headers': df_large.columns.tolist(),
                'sheet_name': f'MCP++ Large Dataset Test {timestamp}'
            }
        )
        
        if result5.content and result5.content[0].text:
            content = json.loads(result5.content[0].text)
            if content.get('success'):
                print(f"   ✅ SUCCESS: Large dataset sheet created!")
                print(f"      URL: {content.get('spreadsheet_url')}")
                print(f"      Rows: {content.get('rows_created')}, Cols: {content.get('columns_created')}")
                print(f"      Data shape: {content.get('data_shape')}")
            else:
                print(f"   ❌ FAIL: {content.get('message')}")
        
    except Exception as e:
        print(f"   ❌ ERROR: {str(e)}")
    
    print(f"\n✅ write_new_sheet comprehensive test completed!")


if __name__ == "__main__":
    # Load environment variables from .env
    from dotenv import load_dotenv
    load_dotenv()

    # Use local endpoint for testing
    endpoint = TEST_MCP_ENDPOINT
    print(f"🔗 Using endpoint: {endpoint}")

    print(f"🎯 Running MCP++ Test Suite")
    print("=" * 80)

    async def run_all_tests():
        provider = "google_sheets"
        oauth_result = await query_user_oauth_info(user_id = TEST_USER_ID, provider_name = provider)
        
        print(f"✅ OAuth query successful!")
        print(f"   Success: {oauth_result.get('success')}")
        print(f"   User ID: {oauth_result.get('user_id')}")
        print(f"   Provider: {oauth_result.get('provider')}")
        print(f"   Message: {oauth_result.get('message')}")
        
        # Extract auth_info for use as headers
        auth_info = oauth_result.get('auth_info', {})
        print(f"\nauth_info : {auth_info}")

        # # Test 1: Basic DataFrame conversion with write_new_sheet
        # print(f"\n{'='*20} TEST 1: DATAFRAME CONVERSION {'='*20}")
        # await test_mcpplus_dataframe_conversion(
        #     url=f"{endpoint}/mcp",
        #     headers=auth_info
        # )

        # Test 2: append_rows and update_range with DataFrames
        # print(f"\n{'='*20} TEST 2: APPEND & UPDATE {'='*20}")
        # await test_mcpplus_append_and_update(
        #     url=f"{endpoint}/mcp",
        #     headers=auth_info
        # )

        # # Test 3: Real-world spreadsheet row updates
        # print(f"\n{'='*20} TEST 3: SPREADSHEET ROW UPDATES {'='*20}")
        # await test_mcpplus_update_spreadsheet_rows(
        #     url=f"{endpoint}/mcp",
        #     headers=auth_info
        # )

        # # Test 4: OAuth Query Integration (Stage 2)
        # print(f"\n{'='*20} TEST 4: OAUTH QUERY INTEGRATION {'='*20}")
        # await test_oauth_query_integration()

        # # Test 5: Query OAuth by SSE
        # print(f"\n{'='*20} TEST 5: QUERY OAUTH BY SSE {'='*20}")
        # await test_query_oauth_by_sse()

        # Test 6: Call tool by SSE
        print(f"\n{'='*20} TEST 6: CALL TOOL BY SSE {'='*20}")
        await test_call_tool_by_sse()

        # Test 7: Call write_new_sheet tool by SSE
        print(f"\n{'='*20} TEST 7: CALL WRITE_NEW_SHEET BY SSE {'='*20}")
        await test_call_tool_write_new_sheet()

        # Test 8: Call append_rows tool by SSE
        print(f"\n{'='*20} TEST 8: CALL APPEND_ROWS BY SSE {'='*20}")
        await test_call_tool_append_rows()

        # # Test 9: Comprehensive write_new_sheet testing
        # print(f"\n{'='*20} TEST 9: WRITE_NEW_SHEET COMPREHENSIVE {'='*20}")
        # await test_write_new_sheet_comprehensive()

   
        print(f"\n{'='*80}")
        print("🎉 ALL MCP++ TESTS COMPLETED!")
        print(f"{'='*80}")

    # Run all tests
    asyncio.run(run_all_tests())
