import argparse
import logging
import os
import sys
from importlib import metadata

# Local imports
from core.server import mcp
# cleanup_expired_tables moved to temp/old_code/session_tools.py
# from datatable_tools.table_manager import cleanup_expired_tables

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
    parser.add_argument('--transport', choices=['stdio', 'streamable-http'], default='streamable-http',
                        help='Transport mode: stdio (default) or streamable-http')
    parser.add_argument('--port', type=int, default=8321,
                        help='Port for streamable-http transport (default: 8321)')
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
        print(f"   🔗 URL: http://0.0.0.0:{args.port}")
    print(f"   🐍 Python: {sys.version.split()[0]}")
    print("")

    # Import datatable tools to register them with the MCP server
    # Stage 1 Refactoring: Keep only 5 essential MCP tools
    import datatable_tools.lifecycle_tools   # load_data_table
    import datatable_tools.detailed_tools    # write_new_sheet, append_rows, append_columns, update_range

   

    try:
        # Set the transport mode for health check
        import core.server
        core.server._current_transport_mode = args.transport

        if args.transport == 'streamable-http':
            print(f"🚀 Starting server on http://0.0.0.0:{args.port}")
            mcp.run(transport="streamable-http", port=args.port, host="0.0.0.0")
        else:
            print("🚀 Starting server in stdio mode")
            mcp.run()

    except KeyboardInterrupt:
        print("\n👋 Server shutdown requested")
        # cleanup_expired_tables() - removed in Stage 1 refactoring
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        logger.error(f"Unexpected error running server: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()