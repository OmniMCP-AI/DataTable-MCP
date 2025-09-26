#!/usr/bin/env python3
"""
Docker integration test for DataTable MCP Server
Tests the server running in a Docker container
"""

import asyncio
import sys
import logging
import httpx

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOCKER_BASE_URL = "http://localhost:8321"

async def test_docker_health():
    """Test if Docker container is healthy"""
    print("🧪 Testing Docker Health...")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DOCKER_BASE_URL}/health", timeout=10)
            assert response.status_code == 200, f"Health check failed: {response.status_code}"
            data = response.json()
            print(f"   ✅ Docker container is healthy: {data.get('status')}")
            return True
        except Exception as e:
            print(f"   ❌ Health check failed: {e}")
            return False

async def test_container_running():
    """Test that container is properly running"""
    print("🧪 Testing Container Status...")

    try:
        # Check if container is running and responsive
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{DOCKER_BASE_URL}/health", timeout=5)
            data = response.json()

            if response.status_code == 200 and data.get('service') == 'datatable-mcp':
                print("   ✅ Container is running and responsive")
                return True
            else:
                print(f"   ❌ Container health check failed: {data}")
                return False
    except Exception as e:
        print(f"   ❌ Container status test failed: {e}")
        return False

async def test_basic_functionality():
    """Test that the server is properly configured with tools"""
    print("🧪 Testing Basic Server Configuration...")

    # For now, just verify the server is running with the health check
    # In a real MCP client integration, you would use the MCP protocol
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{DOCKER_BASE_URL}/health", timeout=10)
            data = response.json()

            if data.get('service') == 'datatable-mcp':
                print("   ✅ DataTable MCP service is running")
                return True
            else:
                print(f"   ❌ Unexpected service response: {data}")
                return False
    except Exception as e:
        print(f"   ❌ Basic functionality test failed: {e}")
        return False

async def run_docker_tests():
    """Run all Docker integration tests"""
    print("🚀 Starting DataTable MCP Docker Tests")
    print("=" * 50)

    try:
        # Test Docker health
        if not await test_docker_health():
            print("❌ Docker health check failed. Is the container running?")
            return False

        # Test SSE endpoint
        if not await test_container_running():
            print("❌ Container status test failed")
            return False

        # Test basic functionality
        if not await test_basic_functionality():
            print("❌ Basic functionality test failed")
            return False

        print("\n" + "=" * 50)
        print("🎉 All Docker tests passed! Container is working correctly.")
        print("💡 Note: This tests the HTTP interface. For full MCP functionality,")
        print("   use an MCP client that supports SSE transport.")
        return True

    except Exception as e:
        print(f"\n❌ Docker test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Run Docker tests
    success = asyncio.run(run_docker_tests())
    sys.exit(0 if success else 1)