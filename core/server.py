import logging
from mcp.server import Server
from mcp.server.models import InitializationOptions
import mcp.types as types
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DataTable MCP server instance
server = Server("datatable")

# Import all tool functions (will be imported later to avoid circular imports)
_tool_functions = {}

def register_tool(name: str, func):
    """Register a tool function"""
    _tool_functions[name] = func

@server.call_tool()
async def handle_call_tool(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle all tool calls by dispatching to the appropriate function
    """
    logger.info(f"Tool call: {tool_name} with arguments: {arguments}")
    
    if tool_name not in _tool_functions:
        return {
            "success": False,
            "error": f"Unknown tool: {tool_name}",
            "message": f"Tool '{tool_name}' is not registered"
        }
    
    try:
        # Call the actual tool function with unpacked arguments
        result = await _tool_functions[tool_name](**arguments)
        return result
    except Exception as e:
        logger.error(f"Error calling tool {tool_name}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to execute tool '{tool_name}'"
        }

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """
    List available tools.
    Each tool in the list will be registered by the @server.call_tool() decorators
    in the datatable_tools modules.
    """
    return [
        types.Tool(
            name="create_table",
            description="Create a new DataTable from data array with auto-detected headers",
            inputSchema={
                "type": "object",
                "properties": {
                    "data": {
                        "type": "array",
                        "items": {"type": "array"},
                        "description": "2D array of table data (rows x columns)"
                    },
                    "headers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional column headers"
                    },
                    "name": {
                        "type": "string",
                        "description": "Optional table name for identification",
                        "default": "Untitled Table"
                    }
                },
                "required": ["data"]
            }
        ),
        types.Tool(
            name="list_tables",
            description="List all available tables in the current session",
            inputSchema={
                "type": "object",
                "properties": {},
                "additionalProperties": False
            }
        ),
        types.Tool(
            name="get_table_info",
            description="Get detailed information about a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    }
                },
                "required": ["table_id"]
            }
        ),
        types.Tool(
            name="delete_table",
            description="Delete a table from the session",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table to delete"
                    }
                },
                "required": ["table_id"]
            }
        ),
        types.Tool(
            name="add_row",
            description="Add a new row to an existing table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "row_data": {
                        "type": "array",
                        "description": "Data for the new row"
                    }
                },
                "required": ["table_id", "row_data"]
            }
        ),
        types.Tool(
            name="add_column",
            description="Add a new column to an existing table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "column_name": {
                        "type": "string",
                        "description": "Name of the new column"
                    },
                    "default_value": {
                        "description": "Default value for existing rows"
                    },
                    "position": {
                        "type": "integer",
                        "description": "Position to insert the column (0-based index)"
                    }
                },
                "required": ["table_id", "column_name"]
            }
        ),
        types.Tool(
            name="update_cell",
            description="Update a specific cell in the table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "row_index": {
                        "type": "integer",
                        "description": "Row index (0-based)"
                    },
                    "column": {
                        "description": "Column identifier (name or index)"
                    },
                    "value": {
                        "description": "New value for the cell"
                    }
                },
                "required": ["table_id", "row_index", "column", "value"]
            }
        ),
        types.Tool(
            name="delete_row",
            description="Delete rows from the table by index or condition",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "row_indices": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "List of row indices to delete (0-based)"
                    }
                },
                "required": ["table_id", "row_indices"]
            }
        ),
        types.Tool(
            name="delete_column",
            description="Delete columns from the table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "columns": {
                        "type": "array",
                        "description": "List of column names or indices to delete"
                    }
                },
                "required": ["table_id", "columns"]
            }
        ),
        types.Tool(
            name="sort_table",
            description="Sort table by one or more columns",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "sort_columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"description": "Column name or index"},
                                "ascending": {"type": "boolean", "default": True}
                            },
                            "required": ["column"]
                        },
                        "description": "List of columns to sort by with direction"
                    }
                },
                "required": ["table_id", "sort_columns"]
            }
        ),
        types.Tool(
            name="get_table_data",
            description="Get table data with optional pagination and filtering",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "start_row": {
                        "type": "integer",
                        "description": "Starting row index (0-based)",
                        "default": 0
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return"
                    },
                    "columns": {
                        "type": "array",
                        "description": "Specific columns to return (names or indices)"
                    }
                },
                "required": ["table_id"]
            }
        ),
        types.Tool(
            name="search_table",
            description="Search for rows matching specific criteria",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query string"
                    },
                    "columns": {
                        "type": "array",
                        "description": "Specific columns to search in"
                    },
                    "case_sensitive": {
                        "type": "boolean",
                        "description": "Whether search should be case sensitive",
                        "default": False
                    }
                },
                "required": ["table_id", "query"]
            }
        ),
        types.Tool(
            name="filter_table",
            description="Filter table rows using pandas query syntax (simplified)",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "query": {
                        "type": "string",
                        "description": "Pandas query string (e.g., 'Age > 25', 'Name == \"John\"', 'Age > 20 and Role == \"Engineer\"')"
                    },
                    "create_new_table": {
                        "type": "boolean",
                        "description": "If True, creates a new table with filtered results",
                        "default": False
                    },
                    "new_table_name": {
                        "type": "string",
                        "description": "Name for the new table (if create_new_table is True)"
                    }
                },
                "required": ["table_id", "query"]
            }
        ),
        types.Tool(
            name="export_table",
            description="Export table data to various formats including Google Sheets",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "export_format": {
                        "type": "string",
                        "enum": ["csv", "json", "excel", "parquet", "google_sheets"],
                        "description": "Export format"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Optional file path to save to"
                    },
                    "return_content": {
                        "type": "boolean",
                        "description": "If True, returns content in response instead of saving to file",
                        "default": False
                    },
                    "encoding": {
                        "type": "string",
                        "description": "File encoding for CSV files"
                    },
                    "delimiter": {
                        "type": "string",
                        "description": "Delimiter for CSV files"
                    },
                    "spreadsheet_id": {
                        "type": "string",
                        "description": "Google Sheets spreadsheet ID (required for google_sheets format)"
                    },
                    "worksheet_id": {
                        "type": "string",
                        "description": "Google Sheets worksheet ID (optional, creates new if not provided)"
                    }
                },
                "required": ["table_id", "export_format"]
            }
        ),
        types.Tool(
            name="calculate_statistics",
            description="Calculate basic statistics for numeric columns",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "columns": {
                        "type": "array",
                        "description": "Specific columns to analyze (names or indices)"
                    },
                    "statistics": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["count", "sum", "mean", "median", "std", "min", "max"]
                        },
                        "description": "Statistics to calculate",
                        "default": ["count", "mean", "std", "min", "max"]
                    }
                },
                "required": ["table_id"]
            }
        ),
        types.Tool(
            name="group_by",
            description="Group table data by one or more columns with aggregation",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "group_columns": {
                        "type": "array",
                        "description": "Columns to group by (names or indices)"
                    },
                    "aggregations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"description": "Column to aggregate"},
                                "function": {
                                    "type": "string",
                                    "enum": ["count", "sum", "mean", "min", "max", "std"]
                                },
                                "alias": {"type": "string", "description": "Optional alias for the result column"}
                            },
                            "required": ["column", "function"]
                        },
                        "description": "Aggregation functions to apply"
                    }
                },
                "required": ["table_id", "group_columns", "aggregations"]
            }
        ),
        types.Tool(
            name="pivot_table",
            description="Create a pivot table from the data",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "Unique identifier of the table"
                    },
                    "index_columns": {
                        "type": "array",
                        "description": "Columns to use as row index"
                    },
                    "column_to_pivot": {
                        "description": "Column whose values will become new columns"
                    },
                    "value_column": {
                        "description": "Column containing values to aggregate"
                    },
                    "aggregation_function": {
                        "type": "string",
                        "enum": ["sum", "mean", "count", "min", "max"],
                        "description": "Function to aggregate values",
                        "default": "sum"
                    }
                },
                "required": ["table_id", "index_columns", "column_to_pivot", "value_column"]
            }
        ),
        types.Tool(
            name="get_session_info",
            description="Get information about the current session",
            inputSchema={
                "type": "object",
                "properties": {},
                "additionalProperties": False
            }
        ),
        types.Tool(
            name="clear_session",
            description="Clear all tables and data from the current session",
            inputSchema={
                "type": "object",
                "properties": {
                    "confirm": {
                        "type": "boolean",
                        "description": "Confirmation flag to prevent accidental clearing",
                        "default": False
                    }
                },
                "required": ["confirm"]
            }
        ),
        types.Tool(
            name="cleanup_expired_tables",
            description="Clean up expired tables based on TTL settings",
            inputSchema={
                "type": "object",
                "properties": {
                    "force": {
                        "type": "boolean",
                        "description": "Force cleanup even if tables haven't expired",
                        "default": False
                    }
                }
            }
        )
    ]