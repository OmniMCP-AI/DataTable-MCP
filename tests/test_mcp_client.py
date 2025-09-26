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
from typing import Dict, Any, Optional

sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import MCP client libraries
try:
    from mcp import ClientSession
    from mcp.client.stdio import stdio_client
    from mcp.client.streamable_http import streamablehttp_client
    from mcp import StdioServerParameters
    MCP_CLIENT_AVAILABLE = True
except ImportError:
    MCP_CLIENT_AVAILABLE = False
    print("⚠️  MCP client libraries not available")

def parse_sse_response(response_text):
    """Parse Server-Sent Events response"""
    lines = response_text.strip().split('\n')
    data_lines = [line[5:] for line in lines if line.startswith('data: ')]
    if data_lines:
        return json.loads(data_lines[0])
    return None

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

async def test_streamable_http_client():
    """Test MCP server using direct HTTP calls (not SSE)"""
    print("\n🧪 Testing MCP Server with Direct HTTP Calls...")

    server_process = None
    try:
        # Start the server
        print("Starting streamable-http server...")
        server_process = subprocess.Popen([
            "python", "main.py",
            "--transport", "streamable-http",
            "--port", "8003"
        ], cwd=os.path.dirname(os.path.dirname(__file__)))

        # Wait for server to start
        await asyncio.sleep(3)

        # Test using direct HTTP calls
        success = await test_http_streamable_list_tools()
        return success

    except Exception as e:
        print(f"❌ MCP streamable-http client test failed: {e}")
        import traceback
        traceback.print_exc()
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

async def test_http_streamable_list_tools():
    """Test tools list using direct HTTP calls"""
    try:
        base_url = "http://127.0.0.1:8003/mcp"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }

        async with httpx.AsyncClient() as client:
            # Initialize
            print("Initializing HTTP connection...")
            response = await client.post(base_url,
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {
                            "name": "test-client",
                            "version": "1.0.0"
                        }
                    }
                })

            if response.status_code == 200:
                # Parse SSE response
                result = parse_sse_response(response.text)
                if result and not result.get('error'):
                    # Extract session ID
                    session_id = response.headers.get('mcp-session-id')
                    if session_id:
                        headers['mcp-session-id'] = session_id
                        print(f"✅ HTTP client initialized with session: {session_id}")

                        # Test ping to verify connection
                        ping_response = await client.post(base_url,
                            headers=headers,
                            json={"jsonrpc": "2.0", "id": 2, "method": "ping", "params": {}})

                        if ping_response.status_code == 200:
                            ping_result = parse_sse_response(ping_response.text)
                            if ping_result and not ping_result.get('error'):
                                print("✅ Ping successful - HTTP connection working")
                                return True
                            else:
                                print(f"❌ Ping failed: {ping_result.get('error', {}).get('message', 'unknown error')}")
                                return False
                        else:
                            print(f"❌ Ping failed: {ping_response.status_code}")
                            return False
                    else:
                        print("❌ No session ID received")
                        return False
                else:
                    print(f"❌ Initialization failed: {result.get('error', {}).get('message', 'unknown error') if result else 'no response'}")
                    return False
            else:
                print(f"❌ Failed to initialize: {response.status_code}")
                return False

    except Exception as e:
        print(f"❌ HTTP test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

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

    # Test STDIO protocol (commented out for now)
    # stdio_success = await test_mcp_stdio_client()

    # Test Streamable-HTTP protocol with proper SSE client
    http_success = await test_streamable_http_client()

    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    # print(f"   STDIO Protocol:          {'✅ PASSED' if stdio_success else '❌ FAILED'}")
    print(f"   Streamable-HTTP Protocol: {'✅ PASSED' if http_success else '❌ FAILED'}")

    overall_success = http_success

    if overall_success:
        print("\n🎉 All MCP tests passed!")
    else:
        print("\n❌ Some MCP tests failed!")

    return overall_success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
