import argparse
import logging
import os
import sys
import asyncio
from importlib import metadata

# Local imports
from core.server import server
from datatable_tools.table_manager import cleanup_expired_tables
from mcp.server.sse import SseServerTransport
from mcp.server.stdio import stdio_server
from mcp.server.models import InitializationOptions
import mcp.types as types

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main entry point for the DataTable MCP server.
    Uses FastMCP's native streamable-http transport.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='DataTable MCP Server')
    parser.add_argument('--transport', choices=['stdio', 'streamable-http'], default='stdio',
                        help='Transport mode: stdio (default) or streamable-http')
    parser.add_argument('--port', type=int, default=8001,
                        help='Port for streamable-http transport (default: 8001)')
    args = parser.parse_args()

    print("🗃️  DataTable MCP Server")
    print("=" * 25)
    print("📋 Server Information:")
    try:
        version = metadata.version("datatable-mcp")
    except metadata.PackageNotFoundError:
        version = "dev"
    print(f"   📦 Version: {version}")
    print(f"   🌐 Transport: {args.transport}")
    if args.transport == 'streamable-http':
        print(f"   🔗 URL: http://localhost:{args.port}")
    print(f"   🐍 Python: {sys.version.split()[0]}")
    print("")

    # Import datatable tools to register them with the MCP server
    import datatable_tools.lifecycle_tools
    import datatable_tools.manipulation_tools
    import datatable_tools.query_tools
    import datatable_tools.export_tools
    import datatable_tools.advanced_tools
    import datatable_tools.session_tools

    print("🛠️  DataTable Tools Loaded:")
    print("   📊 Table Lifecycle Management (4 tools)")
    print("   ✏️  Data Manipulation (6 tools)")
    print("   🔍 Data Query & Access (3 tools)")
    print("   💾 Export & Persistence (2 tools)")
    print("   🔧 Advanced Operations (3 tools)")
    print("   🧹 Session Management (3 tools)")
    print("")

    print("📊 Configuration Summary:")
    print("   🔧 Tools Enabled: 21/21")
    print("   💾 Storage: In-memory with session management")
    print("   📝 Log Level: INFO")
    print("")

    try:
        if args.transport == 'streamable-http':
            print(f"🚀 Starting server on http://localhost:{args.port}")

            async def run_sse():
                async with SseServerTransport("/messages") as (read_stream, write_stream):
                    await server.run(read_stream, write_stream, InitializationOptions())

            # Use uvicorn to serve the SSE transport on HTTP
            import uvicorn
            from fastapi import FastAPI
            from mcp.server.sse import SseServerTransport

            app = FastAPI()
            transport = SseServerTransport("/messages")

            @app.get("/sse")
            async def sse_endpoint():
                return transport.handle_sse_request()

            @app.get("/health")
            async def health_check():
                return {"status": "healthy", "service": "datatable-mcp"}

            uvicorn.run(app, host="0.0.0.0", port=args.port)

        else:
            print("🚀 Starting server in stdio mode")
            async def run_stdio():
                async with stdio_server() as (read_stream, write_stream):
                    await server.run(
                        read_stream,
                        write_stream,
                        InitializationOptions(
                            server_name="datatable",
                            server_version="1.0.0",
                            capabilities=types.ServerCapabilities(
                                tools={}
                            )
                        )
                    )

            asyncio.run(run_stdio())

    except KeyboardInterrupt:
        print("\n👋 Server shutdown requested")
        cleanup_expired_tables()
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        logger.error(f"Unexpected error running server: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()