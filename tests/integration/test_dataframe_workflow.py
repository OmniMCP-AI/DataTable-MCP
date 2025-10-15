#!/usr/bin/env python3
"""
End-to-End DataFrame Workflow Tests

Tests complete workflow from tool discovery through DataFrame operations.
Verifies Stage 5.2.4 E2E integration.

Usage:
    python tests/integration/test_dataframe_workflow.py

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

import asyncio
import os
import sys
import json
from datetime import datetime
from typing import List, Dict, Any

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print("Warning: Polars not available, some tests will be skipped")

from datatable_tools.datatable_tools.tools.mcpplus import call_tool_by_sse, query_user_oauth_info_by_sse
from datatable_tools.auth.service_factory import create_google_service_from_env
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable


# Test configuration
TEST_SSE_URL = os.getenv(
    "TEST_SSE_URL",
    "https://be-dev.omnimcp.ai/api/v1/mcp/a6ebdc49-50e7-4c54-8d2a-639f10098a63/68d688ee3bced208d241bef6/sse"
)
READ_WRITE_URI = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=1210260183#gid=1210260183"


async def test_tool_discovery():
    """Test 1: Verify list_tools() shows DataFrame support in descriptions"""
    print("\n" + "="*60)
    print("Test 1: Tool Discovery - DataFrame Capabilities")
    print("="*60)

    try:
        from mcp.client.sse import sse_client
        from datatable_tools.datatable_tools.tools.mcpplus import MCPPlus

        # Get OAuth info for headers
        oauth_result = await query_user_oauth_info_by_sse(TEST_SSE_URL, "google_sheets")
        auth_info = oauth_result.get('auth_info', {})

        print("   🔍 Connecting to MCP server...")
        async with sse_client(url=TEST_SSE_URL, headers=auth_info) as (read, write):
            async with MCPPlus(read, write) as session:
                await session.initialize()
                print("   ✅ Connected, calling list_tools()...")

                tools = await session.list_tools()
                print(f"   📋 Found {len(tools.tools)} tools")

                # Find tools with DataFrame support
                dataframe_tools = []
                for tool in tools.tools:
                    # Check description
                    description = tool.description or ""

                    # Check inputSchema if available
                    schema_mentions_df = False
                    if hasattr(tool, 'inputSchema') and tool.inputSchema:
                        schema_str = json.dumps(tool.inputSchema)
                        if 'DataFrame' in schema_str or 'polars' in schema_str:
                            schema_mentions_df = True

                    if 'DataFrame' in description or 'polars' in description or schema_mentions_df:
                        dataframe_tools.append(tool.name)
                        print(f"   ✅ {tool.name} - Supports DataFrame")

                if len(dataframe_tools) >= 4:
                    print(f"   ✅ Success: {len(dataframe_tools)} tools advertise DataFrame support")
                    return True
                else:
                    print(f"   ⚠️  Expected at least 4 DataFrame-capable tools, found {len(dataframe_tools)}")
                    return False

    except Exception as e:
        print(f"   ❌ Tool discovery failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_direct_call_with_dataframe():
    """Test 2: Direct call mode with DataFrame input"""
    if not POLARS_AVAILABLE:
        print("\n⚠️  Test 2 skipped - Polars not available")
        return True

    print("\n" + "="*60)
    print("Test 2: Direct Call Mode with DataFrame")
    print("="*60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create test DataFrame
    df = pl.DataFrame({
        'product': ['Widget', 'Gadget'],
        'price': [99.99, 149.99],
        'stock': [10, 5],
        'updated': [timestamp, timestamp]
    })

    print(f"   📊 Test DataFrame:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(df)

    try:
        print("   🚀 Calling google_sheets__update_range with direct_call=True...")
        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__update_range",
            direct_call=True,  # Bypass MCP protocol
            args={
                'uri': READ_WRITE_URI,
                'data': df,  # DataFrame works in direct mode!
                'range_address': 'E1'
            }
        )

        # Parse result
        if result.content and len(result.content) > 0:
            result_text = result.content[0].text
            result_data = json.loads(result_text)

            if result_data.get('success'):
                print(f"   ✅ Direct call succeeded!")
                print(f"      Range: {result_data.get('range')}")
                print(f"      Shape: {result_data.get('shape')}")
                print(f"      spreadsheet_url: {result_data.get('spreadsheet_url')}")
                print(f"      Updated cells: {result_data.get('updated_cells')}")
                return True
            else:
                print(f"   ❌ Direct call failed: {result_data.get('error')}")
                return False
        else:
            print("   ❌ No content in result")
            return False

    except Exception as e:
        print(f"   ❌ Direct call error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_mcp_protocol_with_serializable_data():
    """Test 3: MCP protocol mode with List[List] data"""
    print("\n" + "="*60)
    print("Test 3: MCP Protocol Mode with Serializable Data")
    print("="*60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Use List[List] format (JSON-serializable)
    data = [
        ['Product A', 199.99, 20, timestamp],
        ['Product B', 299.99, 15, timestamp]
    ]

    print(f"   📊 Test Data (List[List]):")
    print(f"      Rows: {len(data)}")
    print(f"      Columns: {len(data[0])}")

    try:
        print("   🚀 Calling google_sheets__update_range with direct_call=False...")
        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__update_range",
            direct_call=False,  # Use MCP protocol
            args={
                'uri': READ_WRITE_URI,
                'data': data,
                'range_address': 'I10'
            }
        )

        # Parse result
        if result.content and len(result.content) > 0:
            result_text = result.content[0].text
            result_data = json.loads(result_text)

            if result_data.get('success'):
                print(f"   ✅ MCP protocol call succeeded!")
                print(f"      Range: {result_data.get('range')}")
                print(f"      Shape: {result_data.get('shape')}")
                print(f"      Updated cells: {result_data.get('updated_cells')}")
                print(f"      spreadsheet_url: {result_data.get('spreadsheet_url')}")
                return True
            else:
                print(f"   ❌ MCP call failed: {result_data.get('error')}")
                return False
        else:
            print("   ❌ No content in result")
            return False

    except Exception as e:
        print(f"   ❌ MCP protocol error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_write_new_sheet_workflow():
    """Test 4: Complete workflow - create new sheet with DataFrame"""
    if not POLARS_AVAILABLE:
        print("\n⚠️  Test 4 skipped - Polars not available")
        return True

    print("\n" + "="*60)
    print("Test 4: E2E Workflow - Create New Sheet with DataFrame")
    print("="*60)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create comprehensive DataFrame
    df = pl.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'employee': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'department': ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'],
        'salary': [120000, 85000, 75000, 90000, 95000],
        'active': [True, True, False, True, True],
        'hire_date': ['2020-01-15', '2021-03-22', '2019-07-01', '2022-05-10', '2023-02-14']
    })

    print(f"   📊 Test DataFrame:")
    print(f"      Shape: {df.shape}")
    print(f"      Columns: {df.columns}")
    print(df)

    try:
        print("   🚀 Creating new sheet with direct_call=True...")
        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__write_new_sheet",
            direct_call=True,  # Direct mode for DataFrame
            args={
                'data': df,
                'sheet_name': f"E2E Workflow Test {timestamp}"
            }
        )

        # Parse result
        if result.content and len(result.content) > 0:
            result_text = result.content[0].text
            result_data = json.loads(result_text)

            if result_data.get('success'):
                print(f"   ✅ New sheet created successfully!")
                print(f"      Spreadsheet URL: {result_data.get('spreadsheet_url')}")
                print(f"      Rows: {result_data.get('rows_created')}")
                print(f"      Columns: {result_data.get('columns_created')}")
                print(f"      Shape: {result_data.get('shape')}")

                # Verify dimensions
                expected_rows = df.height
                expected_cols = df.width
                actual_rows = result_data.get('rows_created')
                actual_cols = result_data.get('columns_created')

                if actual_rows == expected_rows and actual_cols == expected_cols:
                    print(f"   ✅ Dimensions match: {actual_rows}x{actual_cols}")
                    return True
                else:
                    print(f"   ⚠️  Dimension mismatch: expected {expected_rows}x{expected_cols}, got {actual_rows}x{actual_cols}")
                    return False
            else:
                print(f"   ❌ Sheet creation failed: {result_data.get('error')}")
                return False
        else:
            print("   ❌ No content in result")
            return False

    except Exception as e:
        print(f"   ❌ Workflow error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_error_handling():
    """Test 5: Error handling for invalid scenarios"""
    print("\n" + "="*60)
    print("Test 5: Error Handling")
    print("="*60)

    # Test 5a: Invalid tool name
    print("\n   Test 5a: Invalid tool name")
    try:
        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="invalid_tool__nonexistent",
            direct_call=True,
            args={}
        )
        print("   ⚠️  Expected error for invalid tool name")
        return False
    except Exception as e:
        print(f"   ✅ Correctly raised error: {type(e).__name__}")

    # Test 5b: Missing required arguments
    print("\n   Test 5b: Missing required arguments")
    try:
        result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__update_range",
            direct_call=True,
            args={}  # Missing uri, data, range_address
        )

        # Check if error is indicated
        if result.isError or (result.content and 'error' in result.content[0].text.lower()):
            print(f"   ✅ Correctly indicated error for missing arguments")
        else:
            print("   ⚠️  Expected error indication for missing arguments")
            return False
    except Exception as e:
        print(f"   ✅ Correctly raised error: {type(e).__name__}")

    print("\n   ✅ Error handling tests passed")
    return True


async def run_all_tests():
    """Run all E2E workflow tests"""
    print("\n" + "="*60)
    print("🚀 Stage 5.2.4: E2E DataFrame Workflow Tests")
    print("="*60)
    print("\nTesting complete integration from discovery to execution\n")

    # Check environment
    required_vars = [
        "TEST_GOOGLE_OAUTH_REFRESH_TOKEN",
        "TEST_GOOGLE_OAUTH_CLIENT_ID",
        "TEST_GOOGLE_OAUTH_CLIENT_SECRET"
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("   Please set these variables before running tests")
        return False

    results = []

    try:
        # Test 1: Tool discovery
        result1 = await test_tool_discovery()
        results.append(("Tool Discovery", result1))

        # Test 2: Direct call with DataFrame
        result2 = await test_direct_call_with_dataframe()
        results.append(("Direct Call with DataFrame", result2))

        # Test 3: MCP protocol with serializable data
        result3 = await test_mcp_protocol_with_serializable_data()
        results.append(("MCP Protocol Mode", result3))

        # Test 4: Complete workflow
        result4 = await test_write_new_sheet_workflow()
        results.append(("E2E Workflow - New Sheet", result4))

        # Test 5: Error handling
        result5 = await test_error_handling()
        results.append(("Error Handling", result5))

    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Summary
    print("\n" + "="*60)
    print("📊 E2E Test Summary")
    print("="*60)

    all_passed = True
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n🎉 All E2E workflow tests passed!")
        print("\n✅ Stage 5.2.4 E2E integration verified")
        print("✅ Tool discovery working")
        print("✅ Direct call mode working")
        print("✅ MCP protocol mode working")
        print("✅ Error handling working")
    else:
        print("\n⚠️  Some tests failed")

    return all_passed


if __name__ == "__main__":
    # Run the E2E workflow tests
    success = asyncio.run(run_all_tests())

    # Exit with appropriate code
    exit(0 if success else 1)
