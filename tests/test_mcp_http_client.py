#!/usr/bin/env python3
"""
Test MCP server through HTTP client
"""

import asyncio
import json
import sys
import logging

sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_mcp_http_client():
    """Test MCP server using HTTP requests"""
    print("🧪 Testing MCP Server via HTTP...")

    try:
        import httpx

        base_url = "http://localhost:8001/mcp"

        # Test 1: Get server capabilities
        print("\n1. Testing server capabilities...")
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{base_url}/initialize", json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {}
                }
            })

            if response.status_code == 200:
                result = response.json()
                print(f"✅ Server initialized successfully")
                print(f"   Protocol version: {result.get('result', {}).get('protocolVersion', 'unknown')}")
            else:
                print(f"❌ Failed to initialize: {response.status_code}")
                return False

        # Test 2: List available tools
        print("\n2. Testing tools list...")
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{base_url}/tools/list", json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            })

            if response.status_code == 200:
                result = response.json()
                tools = result.get('result', {}).get('tools', [])
                print(f"✅ Found {len(tools)} available tools:")
                for tool in tools[:5]:  # Show first 5 tools
                    print(f"   - {tool.get('name', 'unnamed')}: {tool.get('description', 'no description')[:60]}...")
            else:
                print(f"❌ Failed to list tools: {response.status_code}")
                return False

        # Test 3: Create a table using the create_table tool
        print("\n3. Testing create_table tool...")
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{base_url}/tools/call", json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "create_table",
                    "arguments": {
                        "data": [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"]],
                        "headers": ["Name", "Age", "Role"],
                        "name": "Test Employees"
                    }
                }
            })

            if response.status_code == 200:
                result = response.json()
                tool_result = result.get('result', {})
                if tool_result.get('content', [{}])[0].get('text'):
                    content = json.loads(tool_result['content'][0]['text'])
                    if content.get('success'):
                        table_id = content.get('table_id')
                        print(f"✅ Table created successfully: {table_id}")
                        print(f"   Shape: {content.get('shape')}")

                        # Test 4: List tables
                        print("\n4. Testing list_tables tool...")
                        list_response = await client.post(f"{base_url}/tools/call", json={
                            "jsonrpc": "2.0",
                            "id": 4,
                            "method": "tools/call",
                            "params": {
                                "name": "list_tables",
                                "arguments": {}
                            }
                        })

                        if list_response.status_code == 200:
                            list_result = list_response.json()
                            list_content = json.loads(list_result['result']['content'][0]['text'])
                            if list_content.get('success'):
                                print(f"✅ Tables listed successfully: {list_content.get('count')} tables")
                            else:
                                print(f"❌ Failed to list tables: {list_content.get('error')}")

                        return True
                    else:
                        print(f"❌ Tool execution failed: {content.get('error')}")
                        return False
                else:
                    print(f"❌ Unexpected tool response format")
                    return False
            else:
                print(f"❌ Failed to call create_table: {response.status_code}")
                return False

    except Exception as e:
        print(f"❌ HTTP client test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run HTTP client tests"""
    print("🚀 Starting MCP HTTP Client Tests")
    print("=" * 50)

    success = await test_mcp_http_client()

    if success:
        print("\n🎉 All MCP HTTP tests passed!")
    else:
        print("\n❌ Some MCP HTTP tests failed!")

    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)