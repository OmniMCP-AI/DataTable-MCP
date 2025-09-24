 #!/usr/bin/env python3
"""
Test MCP server using the official MCP client
"""

import asyncio
import json
import sys
import logging

sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_mcp_client():
    """Test MCP server using the MCP client library"""
    print("🧪 Testing MCP Server with MCP Client...")

    try:
        from mcp.client.stdio import stdio_client
        from mcp.client.session import ClientSession
        from mcp import StdioServerParameters

        # Set up the server parameters
        import os
        server_params = StdioServerParameters(
            command="python",
            args=["main.py", "--transport", "stdio"],
            cwd=os.path.dirname(os.path.dirname(__file__))
        )

        # Connect to the server
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as client:
                try:
                    # Initialize the connection
                    print("Initializing client connection...")
                    await client.initialize()
                    print("✅ Client initialized successfully")

                    # Test 1: List tools
                    print("\n1. Testing tools list...")
                    tools = await client.list_tools()
                    print(f"✅ Found {len(tools.tools)} available tools:")
                    for tool in tools.tools[:5]:  # Show first 5 tools
                        print(f"   - {tool.name}: {tool.description[:60]}...")

                    # Test 2: Create a table
                    print("\n2. Testing create_table tool...")
                    result = await client.call_tool(
                        "create_table",
                        {
                            "data": [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"]],
                            "headers": ["Name", "Age", "Role"],
                            "name": "Test Employees"
                        }
                    )

                    if result.content:
                        content = json.loads(result.content[0].text)
                        if content.get('success'):
                            table_id = content.get('table_id')
                            print(f"✅ Table created successfully: {table_id}")
                            print(f"   Shape: {content.get('shape')}")

                            # Test 3: List tables
                            print("\n3. Testing list_tables tool...")
                            list_result = await client.call_tool("list_tables", {})
                            if list_result.content:
                                list_content = json.loads(list_result.content[0].text)
                                if list_content.get('success'):
                                    print(f"✅ Tables listed successfully: {list_content.get('count')} tables")
                                else:
                                    print(f"❌ Failed to list tables: {list_content.get('error')}")
                                    return False
                        else:
                            print(f"❌ Tool execution failed: {content.get('error')}")
                            return False
                    else:
                        print("❌ No content in tool result")
                        return False

                except Exception as e:
                    print(f"❌ Error during client operations: {e}")
                    import traceback
                    traceback.print_exc()
                    return False

        return True

    except Exception as e:
        print(f"❌ MCP client test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run MCP client tests"""
    print("🚀 Starting MCP Client Tests")
    print("=" * 50)

    success = await test_mcp_client()

    if success:
        print("\n🎉 All MCP client tests passed!")
    else:
        print("\n❌ Some MCP client tests failed!")

    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)