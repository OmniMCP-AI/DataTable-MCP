 #!/usr/bin/env python3
"""
Test MCP server using both STDIO and Streamable-HTTP protocols
"""

import asyncio
import json
import sys
import logging
import os
import subprocess
import httpx
import argparse

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

async def test_mcp_http_client(external_server=False, port=8002):
    """Test MCP server using Streamable-HTTP client"""
    print("\n🧪 Testing MCP Server with Streamable-HTTP Client...")

    if external_server:
        print(f"📡 Using external server at http://127.0.0.1:{port}")

    # Start server in background (only if not using external server)
    server_process = None
    try:
        from mcp.client.streamable_http import streamablehttp_client
        from mcp.client.session import ClientSession

        if not external_server:
            # Start the server
            print("Starting HTTP server...")
            server_process = subprocess.Popen([
                "python", "main.py",
                "--transport", "streamable-http",
                "--port", str(port)
            ], cwd=os.path.dirname(os.path.dirname(__file__)))

            # Wait for server to start
            print("⏳ Waiting for server to start...")
            await asyncio.sleep(3)

        # Test server connectivity with a simple health check if available
        try:
            async with httpx.AsyncClient() as http_client:
                response = await http_client.get(f"http://127.0.0.1:{port}/health", timeout=5.0)
                if response.status_code == 200:
                    print("✅ HTTP Server health check passed")
                else:
                    print(f"⚠️  Health check returned {response.status_code}, proceeding with MCP test...")
        except Exception as e:
            print(f"⚠️  Health check failed: {e}, proceeding with MCP test...")

        # Connect to the server via streamable-http
        async with streamablehttp_client(url=f"http://127.0.0.1:{port}/mcp", headers={}) as (read, write, _):
            async with ClientSession(read, write) as client:
                try:
                    # Initialize the connection
                    print("Initializing HTTP client connection...")
                    await client.initialize()
                    print("✅ HTTP Client initialized successfully")

                    # Test tools and operations
                    success = await run_client_tests(client, "HTTP")
                    return success

                except Exception as e:
                    print(f"❌ Error during HTTP client operations: {e}")
                    import traceback
                    traceback.print_exc()
                    return False

    except Exception as e:
        print(f"❌ MCP HTTP client test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up server process (only if we started it)
        if server_process and not external_server:
            try:
                server_process.terminate()
                server_process.wait(timeout=5)
                print("✅ HTTP Server stopped")
            except:
                server_process.kill()

async def run_client_tests(client, protocol_name):
    """Run the common test suite for both protocols"""
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
    """Run MCP client tests for both protocols"""
    parser = argparse.ArgumentParser(description='Test MCP server with client protocols')
    parser.add_argument('--external-server', action='store_true',
                        help='Use external running server instead of starting a new one')
    parser.add_argument('--port', type=int, default=8321,
                        help='Port for HTTP server (default: 8321)')
    parser.add_argument('--skip-stdio', action='store_true',
                        help='Skip STDIO protocol tests')
    parser.add_argument('--skip-http', action='store_true',
                        help='Skip HTTP protocol tests')

    args = parser.parse_args()

    print("🚀 Starting MCP Client Tests")
    print("=" * 60)
    print(f"📊 Configuration:")
    print(f"   External Server: {'Yes' if args.external_server else 'No'}")
    print(f"   HTTP Port: {args.port}")
    print(f"   Test STDIO: {'No' if args.skip_stdio else 'Yes'}")
    print(f"   Test HTTP: {'No' if args.skip_http else 'Yes'}")
    print()

    stdio_success = True
    http_success = True

    # Test STDIO protocol (unless skipped)
    if not args.skip_stdio:
        stdio_success = await test_mcp_stdio_client()
    else:
        print("⏭️  Skipping STDIO protocol tests")

    # Test HTTP protocol (unless skipped)
    if not args.skip_http:
        http_success = await test_mcp_http_client(
            external_server=args.external_server,
            port=args.port
        )
    else:
        print("⏭️  Skipping HTTP protocol tests")

    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    if not args.skip_stdio:
        print(f"   STDIO Protocol: {'✅ PASSED' if stdio_success else '❌ FAILED'}")
    if not args.skip_http:
        print(f"   HTTP Protocol:  {'✅ PASSED' if http_success else '❌ FAILED'}")

    overall_success = stdio_success and http_success

    if overall_success:
        print("\n🎉 All MCP client tests passed!")
    else:
        print("\n❌ Some MCP client tests failed!")

    return overall_success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)