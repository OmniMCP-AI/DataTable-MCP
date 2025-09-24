#!/usr/bin/env python3
"""
Generate OpenAPI documentation for DataTable MCP Server by parsing source files
"""

import json
import ast
from typing import Dict, Any, List

def parse_function_signature(source: str) -> List[Dict[str, Any]]:
    """Parse Python source to extract function definitions and docstrings"""
    try:
        tree = ast.parse(source)
        functions = []

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # Skip private functions
                if node.name.startswith('_'):
                    continue

                # Extract function info
                func_info = {
                    'name': node.name,
                    'description': ast.get_docstring(node) or f"Execute {node.name}",
                    'parameters': {},
                    'required': []
                }

                # Parse parameters
                for arg in node.args.args:
                    param_name = arg.arg
                    if param_name == 'self':
                        continue

                    # Basic parameter info
                    param_info = {
                        'description': f'Parameter {param_name}',
                        'type': 'string'  # Default type
                    }

                    # Try to extract type from annotation
                    if arg.annotation:
                        if isinstance(arg.annotation, ast.Name):
                            type_name = arg.annotation.id
                            if type_name == 'str':
                                param_info['type'] = 'string'
                            elif type_name == 'int':
                                param_info['type'] = 'integer'
                            elif type_name == 'float':
                                param_info['type'] = 'number'
                            elif type_name == 'bool':
                                param_info['type'] = 'boolean'
                        elif isinstance(arg.annotation, ast.Subscript):
                            # Handle List, Dict, Optional etc
                            if isinstance(arg.annotation.value, ast.Name):
                                base_type = arg.annotation.value.id
                                if base_type == 'List':
                                    param_info['type'] = 'array'
                                    param_info['items'] = {'type': 'string'}
                                elif base_type == 'Dict':
                                    param_info['type'] = 'object'
                                elif base_type == 'Optional':
                                    # Extract the inner type for Optional
                                    param_info['type'] = 'string'

                    func_info['parameters'][param_name] = param_info

                # Handle defaults - assume parameters without defaults are required
                num_defaults = len(node.args.defaults)
                num_args = len(node.args.args)
                required_count = num_args - num_defaults

                for i, arg in enumerate(node.args.args):
                    if arg.arg != 'self' and i < required_count:
                        func_info['required'].append(arg.arg)

                functions.append(func_info)

        return functions
    except Exception as e:
        print(f"Error parsing source: {e}")
        return []

def categorize_tool(tool_name: str) -> str:
    """Categorize tool by name"""
    lifecycle_tools = ['create_table', 'load_table', 'clone_table', 'list_tables']
    manipulation_tools = ['append_row', 'add_column', 'set_range_values', 'delete_from_table', 'rename_columns', 'clear_range']
    query_tools = ['get_table_data', 'filter_rows', 'sort_table']
    export_tools = ['save_table', 'export_table']
    advanced_tools = ['merge_tables', 'aggregate_data', 'map_values']
    session_tools = ['cleanup_tables', 'get_table_info', 'get_session_stats']

    if tool_name in lifecycle_tools:
        return "Table Lifecycle"
    elif tool_name in manipulation_tools:
        return "Data Manipulation"
    elif tool_name in query_tools:
        return "Data Query"
    elif tool_name in export_tools:
        return "Export & Import"
    elif tool_name in advanced_tools:
        return "Advanced Operations"
    elif tool_name in session_tools:
        return "Session Management"
    else:
        return "MCP Tools"

def generate_openapi_schema() -> Dict[str, Any]:
    """Generate OpenAPI schema by parsing tool files"""

    # Define tool files
    tool_files = [
        'datatable_tools/lifecycle_tools.py',
        'datatable_tools/manipulation_tools.py',
        'datatable_tools/query_tools.py',
        'datatable_tools/export_tools.py',
        'datatable_tools/advanced_tools.py',
        'datatable_tools/session_tools.py'
    ]

    all_tools = []

    # Parse each tool file
    for file_path in tool_files:
        try:
            with open(file_path, 'r') as f:
                source = f.read()

            functions = parse_function_signature(source)
            all_tools.extend(functions)
            print(f"📄 Parsed {file_path}: found {len(functions)} tools")

        except FileNotFoundError:
            print(f"⚠️  File not found: {file_path}")
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")

    # Create OpenAPI schema
    schema = {
        "openapi": "3.0.0",
        "info": {
            "title": "DataTable MCP Server API",
            "description": "Model Context Protocol (MCP) server for in-memory tabular data manipulation. Provides 21 tools across 6 categories for comprehensive data operations including table lifecycle management, data manipulation, querying, export/import, advanced operations, and session management.",
            "version": "1.0.0",
            "contact": {
                "name": "DataTable MCP Server"
            },
            "license": {
                "name": "MIT"
            }
        },
        "servers": [
            {
                "url": "http://localhost:8001",
                "description": "HTTP transport mode for testing and web integration"
            },
            {
                "url": "stdio://mcp",
                "description": "Standard I/O transport (default MCP mode for Claude Desktop)"
            }
        ],
        "tags": [
            {
                "name": "Table Lifecycle",
                "description": "Create, list, clone, and manage table lifecycle (4 tools)"
            },
            {
                "name": "Data Manipulation",
                "description": "Modify table data - append rows, add columns, set values (6 tools)"
            },
            {
                "name": "Data Query",
                "description": "Query and filter table data (3 tools)"
            },
            {
                "name": "Export & Import",
                "description": "Export tables to files and load from external sources (2 tools)"
            },
            {
                "name": "Advanced Operations",
                "description": "Advanced data operations like aggregation and mapping (3 tools)"
            },
            {
                "name": "Session Management",
                "description": "Manage server session and cleanup (3 tools)"
            }
        ],
        "paths": {},
        "components": {
            "schemas": {
                "MCPToolRequest": {
                    "type": "object",
                    "description": "MCP tool execution request",
                    "required": ["name", "arguments"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Tool name to execute"
                        },
                        "arguments": {
                            "type": "object",
                            "description": "Tool-specific arguments"
                        }
                    }
                },
                "MCPToolResponse": {
                    "type": "object",
                    "description": "MCP tool execution response",
                    "properties": {
                        "success": {
                            "type": "boolean",
                            "description": "Operation success status"
                        },
                        "message": {
                            "type": "string",
                            "description": "Human-readable message"
                        },
                        "error": {
                            "type": "string",
                            "description": "Error details if success=false"
                        },
                        "data": {
                            "type": "object",
                            "description": "Tool-specific response data"
                        }
                    }
                },
                "TableInfo": {
                    "type": "object",
                    "description": "DataTable information structure",
                    "properties": {
                        "table_id": {
                            "type": "string",
                            "pattern": "^dt_[a-z0-9]{8}$",
                            "description": "Unique table identifier",
                            "example": "dt_abc12345"
                        },
                        "headers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Column headers"
                        },
                        "data": {
                            "type": "array",
                            "items": {
                                "type": "array",
                                "items": {}
                            },
                            "description": "Table data as 2D array [rows][columns]"
                        },
                        "shape": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "minItems": 2,
                            "maxItems": 2,
                            "description": "Table dimensions [rows, columns]"
                        },
                        "dtypes": {
                            "type": "object",
                            "description": "Column data types mapping"
                        },
                        "metadata": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "created_at": {"type": "string", "format": "date-time"},
                                "source_info": {"type": "object"}
                            }
                        }
                    }
                }
            }
        }
    }

    # Add paths for each tool
    for tool in all_tools:
        tag = categorize_tool(tool['name'])
        path = f"/mcp/tools/{tool['name']}"

        # Extract first line of description for summary
        description = tool['description']
        summary = description.split('\n')[0] if description else f"Execute {tool['name']}"

        schema["paths"][path] = {
            "post": {
                "tags": [tag],
                "summary": summary,
                "description": description,
                "operationId": tool['name'],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": tool.get('required', []),
                                "properties": tool.get('parameters', {}),
                                "example": generate_example_for_tool(tool['name'])
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Successful tool execution",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/MCPToolResponse"}
                            }
                        }
                    },
                    "400": {
                        "description": "Invalid request parameters"
                    },
                    "500": {
                        "description": "Internal server error"
                    }
                }
            }
        }

    return schema

def generate_example_for_tool(tool_name: str) -> Dict[str, Any]:
    """Generate example request for a tool"""
    examples = {
        'create_table': {
            'data': [['Alice', 25, 'Engineer'], ['Bob', 30, 'Manager']],
            'headers': ['Name', 'Age', 'Role'],
            'name': 'Employees'
        },
        'filter_rows': {
            'table_id': 'dt_abc12345',
            'conditions': [{'column': 'Age', 'operator': 'gt', 'value': 27}]
        },
        'export_table': {
            'table_id': 'dt_abc12345',
            'export_format': 'csv',
            'return_content': True
        },
        'get_table_data': {
            'table_id': 'dt_abc12345',
            'format': 'json'
        }
    }

    return examples.get(tool_name, {'table_id': 'dt_abc12345'})

def main():
    """Generate OpenAPI documentation"""
    print("🔧 Generating OpenAPI documentation for DataTable MCP Server...")
    print("📂 Parsing source files to extract tool definitions...")

    try:
        # Generate schema
        schema = generate_openapi_schema()

        # Save to file
        with open('openapi.json', 'w') as f:
            json.dump(schema, f, indent=2)

        total_tools = len(schema['paths'])
        print(f"✅ Generated openapi.json with {total_tools} tool endpoints")

        # Generate summary by category
        tools_by_category = {}
        for path, methods in schema['paths'].items():
            category = methods['post']['tags'][0]
            if category not in tools_by_category:
                tools_by_category[category] = []
            tool_name = path.split('/')[-1]
            tools_by_category[category].append(tool_name)

        print("\n📊 Tools by Category:")
        for category, tools in tools_by_category.items():
            print(f"   📋 {category}: {len(tools)} tools")
            for tool in tools[:3]:  # Show first 3 tools
                print(f"      • {tool}")
            if len(tools) > 3:
                print(f"      • ... and {len(tools)-3} more")

        print(f"\n🌐 View Documentation:")
        print("   📖 Online Swagger Editor: https://editor.swagger.io/")
        print("      👆 Copy & paste the openapi.json content")
        print("   🚀 Local Server: python main.py --transport streamable-http --port 8001")
        print("      📍 Then visit: http://localhost:8001/docs")

        # Generate quick reference
        print(f"\n📋 Quick Reference:")
        print(f"   🗃️  Total Tools: {total_tools}")
        print(f"   🏷️  Categories: {len(tools_by_category)}")
        print(f"   📄 OpenAPI Version: 3.0.0")
        print(f"   🔗 Transports: stdio (MCP), HTTP (testing)")

        return schema

    except Exception as e:
        print(f"❌ Error generating documentation: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    main()