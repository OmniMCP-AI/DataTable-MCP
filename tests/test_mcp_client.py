 #!/usr/bin/env python3
"""
Test MCP server using both STDIO and basic HTTP connectivity
"""

import asyncio
import json
import sys
import logging
import os
import subprocess
import httpx

sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_mcp_stdio_client():
    """Test MCP server using STDIO client"""
    print("🧪 Testing MCP Server with STDIO Client...")

    try:
        from mcp.client.stdio import stdio_client
        from mcp.client.session import ClientSession
        from mcp import StdioServerParameters

        # Set up the server parameters
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
                    print("Initializing STDIO client connection...")
                    await client.initialize()
                    print("✅ STDIO Client initialized successfully")

                    # Test tools and operations
                    success = await run_client_tests(client, "STDIO")
                    return success

                except Exception as e:
                    print(f"❌ Error during STDIO client operations: {e}")
                    import traceback
                    traceback.print_exc()
                    return False

    except Exception as e:
        print(f"❌ MCP STDIO client test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_streamable_http_server():
    """Test that the streamable-http server starts and responds properly"""
    print("\n🧪 Testing Streamable-HTTP Server Startup...")

    server_process = None
    try:
        # Start the server
        print("Starting HTTP server...")
        server_process = subprocess.Popen([
            "python", "main.py",
            "--transport", "streamable-http",
            "--port", "8003"
        ], cwd=os.path.dirname(os.path.dirname(__file__)))

        # Wait for server to start
        await asyncio.sleep(3)

        # Test if server is responding to basic HTTP requests
        async with httpx.AsyncClient() as http_client:
            try:
                # Check if the MCP endpoint exists and returns expected status codes
                response = await http_client.get("http://127.0.0.1:8003/mcp", timeout=5.0)
                # FastMCP streamable-http returns 405/406 for GET requests, which is expected
                if response.status_code in [405, 406]:
                    print("✅ HTTP Server is running and MCP endpoint is responding correctly")
                    print(f"   Status: {response.status_code} (expected for GET on MCP endpoint)")
                    return True
                else:
                    print(f"❌ Unexpected HTTP status: {response.status_code}")
                    return False

            except Exception as e:
                print(f"❌ Failed to connect to HTTP server: {e}")
                return False

    except Exception as e:
        print(f"❌ HTTP server test failed: {e}")
        return False
    finally:
        # Clean up server process
        if server_process:
            try:
                server_process.terminate()
                server_process.wait(timeout=5)
                print("✅ HTTP Server stopped")
            except:
                server_process.kill()

async def run_client_tests(client, protocol_name):
    """Run the common test suite for STDIO protocol"""
    try:
        # Test 1: List tools
        print(f"\n1. Testing tools list ({protocol_name})...")
        tools = await client.list_tools()
        print(f"✅ Found {len(tools.tools)} available tools:")
        for tool in tools.tools[:5]:  # Show first 5 tools
            print(f"   - {tool.name}: {tool.description[:60]}...")

        # Test 2: Create a table
        print(f"\n2. Testing create_table tool ({protocol_name})...")
        result = await client.call_tool(
            "create_table",
            {
                "data": [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"]],
                "headers": ["Name", "Age", "Role"],
                "name": f"Test Employees ({protocol_name})"
            }
        )

        if result.content:
            content = json.loads(result.content[0].text)
            if content.get('success'):
                table_id = content.get('table_id')
                print(f"✅ Table created successfully: {table_id}")
                print(f"   Shape: {content.get('shape')}")

                # Test 3: List tables
                print(f"\n3. Testing list_tables tool ({protocol_name})...")
                list_result = await client.call_tool("list_tables", {})
                if list_result.content:
                    list_content = json.loads(list_result.content[0].text)
                    if list_content.get('success'):
                        print(f"✅ Tables listed successfully: {list_content.get('count')} tables")

                        # Test 4: Add a row
                        print(f"\n4. Testing add_row tool ({protocol_name})...")
                        add_result = await client.call_tool(
                            "add_row",
                            {
                                "table_id": table_id,
                                "row_data": ["Charlie", 32, "Designer"]
                            }
                        )
                        if add_result.content:
                            add_content = json.loads(add_result.content[0].text)
                            if add_content.get('success'):
                                print(f"✅ Row added successfully: {add_content.get('message')}")
                                return True
                            else:
                                print(f"❌ Failed to add row: {add_content.get('error')}")
                                return False
                    else:
                        print(f"❌ Failed to list tables: {list_content.get('error')}")
                        return False
            else:
                print(f"❌ Tool execution failed: {content.get('error')}")
                return False
        else:
            print("❌ No content in tool result")
            return False

        return True

    except Exception as e:
        print(f"❌ Error during {protocol_name} tests: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run MCP client tests"""
    print("🚀 Starting MCP Client Tests")
    print("=" * 60)

    # Test STDIO protocol
    stdio_success = await test_mcp_stdio_client()

    # Test HTTP server startup (basic connectivity test)
    http_success = await test_streamable_http_server()

    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    print(f"   STDIO Protocol:     {'✅ PASSED' if stdio_success else '❌ FAILED'}")
    print(f"   HTTP Server Startup: {'✅ PASSED' if http_success else '❌ FAILED'}")

    overall_success = stdio_success and http_success

    if overall_success:
        print("\n🎉 All MCP tests passed!")
        print("\n📝 Note: Full HTTP MCP client testing requires compatible")
        print("   SSE client library that works with FastMCP's streamable-http.")
    else:
        print("\n❌ Some MCP tests failed!")

    return overall_success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)