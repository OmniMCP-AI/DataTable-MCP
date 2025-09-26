#!/usr/bin/env python3
"""
Generate OpenAPI documentation for the DataTable MCP Server
"""

import json
import sys
import logging
from pathlib import Path

# Import the server and all tools to register them
from core.server import server
import datatable_tools.lifecycle_tools
import datatable_tools.manipulation_tools
import datatable_tools.query_tools
import datatable_tools.export_tools
import datatable_tools.advanced_tools
import datatable_tools.session_tools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_mcp_openapi():
    """Generate OpenAPI documentation for MCP tools"""

    # Get the FastAPI app from FastMCP
    app = server.app

    # Generate OpenAPI schema
    openapi_schema = app.openapi()

    # Enhance the schema with MCP-specific information
    openapi_schema["info"] = {
        "title": "DataTable MCP Server API",
        "description": "Model Context Protocol (MCP) server for in-memory tabular data manipulation with external source integration",
        "version": "1.0.0",
        "contact": {
            "name": "DataTable MCP Server",
            "url": "https://github.com/your-repo/datatable-mcp"
        },
        "license": {
            "name": "MIT",
            "url": "https://opensource.org/licenses/MIT"
        }
    }

    # Add server information
    openapi_schema["servers"] = [
        {
            "url": "http://localhost:8321",
            "description": "Local development server (HTTP mode)"
        },
        {
            "url": "stdio://",
            "description": "Standard I/O transport for MCP clients"
        }
    ]

    # Add MCP-specific tags
    openapi_schema["tags"] = [
        {
            "name": "MCP Protocol",
            "description": "Model Context Protocol endpoints"
        },
        {
            "name": "Table Lifecycle",
            "description": "Create, list, clone, and manage table lifecycle"
        },
        {
            "name": "Data Manipulation",
            "description": "Modify table data - append rows, add columns, set values"
        },
        {
            "name": "Data Query",
            "description": "Query and filter table data"
        },
        {
            "name": "Export & Import",
            "description": "Export tables to files and load from external sources"
        },
        {
            "name": "Advanced Operations",
            "description": "Advanced data operations like aggregation and mapping"
        },
        {
            "name": "Session Management",
            "description": "Manage server session and cleanup"
        }
    ]

    return openapi_schema

def save_openapi_json(schema, output_path="openapi.json"):
    """Save OpenAPI schema to JSON file"""
    with open(output_path, 'w') as f:
        json.dump(schema, f, indent=2)

    print(f"✅ OpenAPI documentation saved to: {output_path}")
    return output_path

def generate_mcp_tools_docs():
    """Generate MCP tools documentation in a more readable format"""

    tools_docs = {
        "mcp_server_info": {
            "name": "DataTable MCP Server",
            "version": "1.0.0",
            "description": "In-memory tabular data manipulation with external source integration",
            "total_tools": 21
        },
        "tool_categories": {
            "lifecycle": {
                "name": "Table Lifecycle Management",
                "description": "Create, list, clone, and manage table lifecycle",
                "tool_count": 4,
                "tools": [
                    "create_table",
                    "load_table",
                    "clone_table",
                    "list_tables"
                ]
            },
            "manipulation": {
                "name": "Data Manipulation",
                "description": "Modify table data - append rows, add columns, set values",
                "tool_count": 6,
                "tools": [
                    "append_row",
                    "add_column",
                    "set_range_values",
                    "delete_rows",
                    "delete_columns",
                    "rename_column"
                ]
            },
            "query": {
                "name": "Data Query & Access",
                "description": "Query and filter table data",
                "tool_count": 3,
                "tools": [
                    "get_table_data",
                    "filter_rows",
                    "sort_table"
                ]
            },
            "export": {
                "name": "Export & Persistence",
                "description": "Export tables to files and load from external sources",
                "tool_count": 2,
                "tools": [
                    "save_table",
                    "export_table"
                ]
            },
            "advanced": {
                "name": "Advanced Operations",
                "description": "Advanced data operations like aggregation and mapping",
                "tool_count": 3,
                "tools": [
                    "aggregate_data",
                    "join_tables",
                    "map_values"
                ]
            },
            "session": {
                "name": "Session Management",
                "description": "Manage server session and cleanup",
                "tool_count": 3,
                "tools": [
                    "get_session_stats",
                    "cleanup_tables",
                    "get_table_info"
                ]
            }
        },
        "supported_formats": {
            "input": ["CSV", "Excel", "Google Sheets (planned)", "Database (planned)"],
            "output": ["CSV", "JSON", "Excel", "Parquet", "Google Sheets (planned)"]
        },
        "transport_modes": [
            {
                "name": "stdio",
                "description": "Standard I/O transport for MCP clients (default)",
                "usage": "python main.py --transport stdio"
            },
            {
                "name": "streamable-http",
                "description": "HTTP transport for testing and web integration",
                "usage": "python main.py --transport streamable-http --port 8321"
            }
        ]
    }

    return tools_docs

def main():
    """Generate all documentation"""
    print("🔧 Generating OpenAPI documentation for DataTable MCP Server...")

    try:
        # Generate OpenAPI schema
        openapi_schema = generate_mcp_openapi()
        openapi_file = save_openapi_json(openapi_schema)

        # Generate MCP tools documentation
        tools_docs = generate_mcp_tools_docs()
        tools_file = "mcp_tools_docs.json"
        with open(tools_file, 'w') as f:
            json.dump(tools_docs, f, indent=2)

        print(f"✅ MCP tools documentation saved to: {tools_file}")

        # Generate markdown documentation
        generate_markdown_docs(tools_docs, openapi_schema)

        print("\n📚 Generated Documentation Files:")
        print(f"   🔧 OpenAPI Schema: {openapi_file}")
        print(f"   🛠️  MCP Tools Info: {tools_file}")
        print(f"   📝 Markdown Docs: API_DOCS.md")
        print("\n🌐 View OpenAPI docs by:")
        print("   1. Start server: python main.py --transport streamable-http --port 8321")
        print("   2. Visit: http://localhost:8321/docs (if FastMCP supports it)")
        print("   3. Or use online viewer: https://editor.swagger.io/ (paste openapi.json)")

    except Exception as e:
        print(f"❌ Error generating documentation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def generate_markdown_docs(tools_docs, openapi_schema):
    """Generate markdown documentation"""

    md_content = f"""# DataTable MCP Server API Documentation

## Overview
{tools_docs['mcp_server_info']['description']}

**Version:** {tools_docs['mcp_server_info']['version']}
**Total Tools:** {tools_docs['mcp_server_info']['total_tools']}

## Transport Modes

### Standard I/O (Default)
```bash
python main.py --transport stdio
```
For use with MCP clients like Claude Desktop.

### HTTP Mode (Testing)
```bash
python main.py --transport streamable-http --port 8321
```
For testing and web integration. Server will be available at http://localhost:8321

## Tool Categories

"""

    for category_id, category in tools_docs['tool_categories'].items():
        md_content += f"""### {category['name']}
**Description:** {category['description']}
**Tools:** {category['tool_count']}

**Available Tools:**
"""
        for tool in category['tools']:
            md_content += f"- `{tool}`\n"
        md_content += "\n"

    md_content += f"""## Supported Formats

### Input Formats
{', '.join(tools_docs['supported_formats']['input'])}

### Output Formats
{', '.join(tools_docs['supported_formats']['output'])}

## OpenAPI Documentation

The complete OpenAPI 3.0 schema is available in `openapi.json`. You can:

1. View it online at [Swagger Editor](https://editor.swagger.io/)
2. Use it with API client tools like Postman
3. Generate client SDKs in various languages

## Example Usage

### Create a Table
```json
{{
  "tool": "create_table",
  "arguments": {{
    "data": [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"]],
    "headers": ["Name", "Age", "Role"],
    "name": "Employees"
  }}
}}
```

### Filter Data
```json
{{
  "tool": "filter_rows",
  "arguments": {{
    "table_id": "dt_12345678",
    "conditions": [
      {{"column": "Age", "operator": "gt", "value": 27}}
    ]
  }}
}}
```

### Export Table
```json
{{
  "tool": "export_table",
  "arguments": {{
    "table_id": "dt_12345678",
    "export_format": "csv",
    "return_content": true
  }}
}}
```

---
*Generated automatically from DataTable MCP Server*
"""

    with open("API_DOCS.md", 'w') as f:
        f.write(md_content)

if __name__ == "__main__":
    main()