#!/usr/bin/env python3
"""
SSE MCP Client test for Docker container
Tests actual MCP tools through SSE transport with the Docker container
"""

import asyncio
import sys
import logging
import json
import httpx
from typing import AsyncGenerator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOCKER_BASE_URL = "http://localhost:8001"

class SSEMCPClient:
    """Simple SSE-based MCP client for testing"""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = None
        self.message_id = 1

    async def __aenter__(self):
        self.client = httpx.AsyncClient()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    def _next_id(self) -> int:
        current_id = self.message_id
        self.message_id += 1
        return current_id

    async def send_mcp_request(self, method: str, params: dict = None) -> dict:
        """Send an MCP request via SSE"""
        message = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": method,
            "params": params or {}
        }

        # For SSE transport, we need to establish a connection and send the message
        # This is a simplified implementation for testing
        headers = {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }

        try:
            # Use POST to send the MCP message to SSE endpoint
            response = await self.client.post(
                f"{self.base_url}/sse",
                json=message,
                headers=headers,
                timeout=30.0
            )

            if response.status_code == 200:
                # Parse SSE response
                content = response.text
                # Look for JSON-RPC response in SSE data
                if content:
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError:
                        # SSE format might be different, try to extract JSON
                        lines = content.split('\n')
                        for line in lines:
                            if line.startswith('data: '):
                                try:
                                    return json.loads(line[6:])
                                except json.JSONDecodeError:
                                    continue
                        raise ValueError(f"No valid JSON found in response: {content}")
                else:
                    raise ValueError("Empty response from SSE endpoint")
            else:
                raise ValueError(f"HTTP {response.status_code}: {response.text}")

        except Exception as e:
            logger.error(f"Error sending MCP request: {e}")
            raise

async def test_docker_mcp_tools():
    """Test MCP tools through Docker SSE endpoint"""
    print("🧪 Testing MCP Tools via Docker SSE...")

    try:
        async with SSEMCPClient(DOCKER_BASE_URL) as client:
            # Test 1: Initialize
            print("🔗 Initializing MCP connection...")
            try:
                init_response = await client.send_mcp_request("initialize", {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test-client", "version": "1.0.0"}
                })
                print(f"   ✅ Initialized: {init_response}")
            except Exception as e:
                print(f"   ⚠️  Initialize may not be required: {e}")

            # Test 2: List tools
            print("\n🔧 Testing tools/list...")
            try:
                tools_response = await client.send_mcp_request("tools/list")
                if "result" in tools_response and "tools" in tools_response["result"]:
                    tools = tools_response["result"]["tools"]
                    print(f"   ✅ Found {len(tools)} tools:")
                    for tool in tools[:5]:  # Show first 5
                        print(f"      - {tool['name']}: {tool['description'][:50]}...")
                else:
                    print(f"   ❌ Unexpected tools response: {tools_response}")
                    return False
            except Exception as e:
                print(f"   ❌ Tools list failed: {e}")
                return False

            # Test 3: Call create_table tool
            print("\n📊 Testing tools/call - create_table...")
            try:
                create_response = await client.send_mcp_request("tools/call", {
                    "name": "create_table",
                    "arguments": {
                        "data": [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"]],
                        "headers": ["Name", "Age", "Role"],
                        "name": "Docker Test Table"
                    }
                })

                if "result" in create_response:
                    result = create_response["result"]
                    if "content" in result and result["content"]:
                        content_text = result["content"][0]["text"]
                        table_result = json.loads(content_text)

                        if table_result.get("success"):
                            table_id = table_result.get("table_id")
                            print(f"   ✅ Table created: {table_id}")

                            # Test 4: Call list_tables tool
                            print("\n📋 Testing tools/call - list_tables...")
                            list_response = await client.send_mcp_request("tools/call", {
                                "name": "list_tables",
                                "arguments": {}
                            })

                            if "result" in list_response:
                                list_result = list_response["result"]
                                if "content" in list_result and list_result["content"]:
                                    list_content_text = list_result["content"][0]["text"]
                                    list_table_result = json.loads(list_content_text)

                                    if list_table_result.get("success"):
                                        count = list_table_result.get("count", 0)
                                        print(f"   ✅ Listed tables: {count} table(s)")
                                        return True
                                    else:
                                        print(f"   ❌ List tables failed: {list_table_result.get('error')}")
                                        return False
                            else:
                                print(f"   ❌ Unexpected list response: {list_response}")
                                return False
                        else:
                            print(f"   ❌ Create table failed: {table_result.get('error')}")
                            return False
                else:
                    print(f"   ❌ Unexpected create response: {create_response}")
                    return False

            except Exception as e:
                print(f"   ❌ Tool call failed: {e}")
                return False

    except Exception as e:
        print(f"   ❌ Docker MCP test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def run_docker_mcp_tests():
    """Run Docker MCP SSE tests"""
    print("🚀 Starting Docker MCP SSE Tests")
    print("=" * 50)
    print("💡 This tests actual MCP tools through Docker SSE transport")
    print()

    try:
        if await test_docker_mcp_tools():
            print("\n" + "=" * 50)
            print("🎉 All Docker MCP tests passed!")
            print("✅ Tools are working correctly through Docker container")
            return True
        else:
            print("\n❌ Docker MCP tests failed")
            return False

    except Exception as e:
        print(f"\n❌ Docker MCP test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Run Docker MCP tests
    success = asyncio.run(run_docker_mcp_tests())
    sys.exit(0 if success else 1)