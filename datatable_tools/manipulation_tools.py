from typing import Dict, List, Optional, Any, Union
import logging
import numpy as np
from fastmcp import Context
from core.server import mcp
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

@mcp.tool
async def add_row(
    ctx: Context,
    table_id: str,
    row_data: List[Any],
    fill_strategy: str = "none"
) -> Dict[str, Any]:
    """
    Add a new row to the table with robust handling of dimension mismatches.

    Args:
        table_id: ID of the target table
        row_data: List of values for the new row
        fill_strategy: How to handle dimension mismatches ("none", "fill_na", "fill_empty", "fill_zero")

    Returns:
        Dict containing success status and updated table information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        original_shape = table.shape.copy()
        table.append_row(row_data, fill_strategy)

        return {
            "success": True,
            "table_id": table_id,
            "original_shape": original_shape,
            "new_shape": table.shape,
            "fill_strategy": fill_strategy,
            "message": f"Added row to table {table_id}. Shape changed from {original_shape} to {table.shape}"
        }

    except Exception as e:
        logger.error(f"Error appending row to table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to append row to table {table_id}"
        }

@mcp.tool
async def add_column(
    ctx: Context,
    table_id: str,
    column_name: str,
    default_value: Optional[Any] = None,
    position: Optional[int] = None
) -> Dict[str, Any]:
    """
    Add a new column to the table with optional default values.

    Args:
        table_id: ID of the target table
        column_name: Name of the new column
        default_value: Default value for all rows in the new column
        position: Optional position to insert column (if not specified, adds at end)

    Returns:
        Dict containing success status and updated table information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        if column_name in table.headers:
            return {
                "success": False,
                "error": f"Column '{column_name}' already exists",
                "message": f"Column '{column_name}' already exists in table"
            }

        original_columns = table.headers.copy()
        table.add_column(column_name, default_value)

        return {
            "success": True,
            "table_id": table_id,
            "column_name": column_name,
            "default_value": default_value,
            "original_columns": len(original_columns),
            "new_columns": len(table.headers),
            "message": f"Added column '{column_name}' to table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error adding column to table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to add column to table {table_id}"
        }

# @mcp.tool
async def update_cell(
    ctx: Context,
    table_id: str,
    row_index: int,
    column: Union[str, int],
    value: Any
) -> Dict[str, Any]:
    """
    Update a specific cell in the table.

    Args:
        table_id: ID of the target table
        row_index: Row index (0-based)
        column: Column identifier (name or index)
        value: New value for the cell

    Returns:
        Dict containing success status and update information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        # Validate row index
        if row_index >= len(table.df) or row_index < 0:
            return {
                "success": False,
                "error": f"Row index {row_index} out of range (table has {len(table.df)} rows)",
                "message": "Invalid row index specified"
            }

        # Handle column identifier
        if isinstance(column, int):
            if column >= len(table.headers) or column < 0:
                return {
                    "success": False,
                    "error": f"Column index {column} out of range (table has {len(table.headers)} columns)",
                    "message": "Invalid column index specified"
                }
            column_name = table.headers[column]
        else:
            column_name = str(column)
            if column_name not in table.headers:
                return {
                    "success": False,
                    "error": f"Column '{column_name}' not found",
                    "message": "Specified column does not exist"
                }

        # Get old value
        old_value = table.df.at[row_index, column_name]

        # Update the cell
        with table._lock:
            table.df.at[row_index, column_name] = value
            table._update_modified_time()

        return {
            "success": True,
            "table_id": table_id,
            "row_index": row_index,
            "column": column_name,
            "old_value": old_value,
            "new_value": value,
            "message": f"Updated cell [{row_index}, {column_name}] in table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error updating cell in table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to update cell in table {table_id}"
        }

# @mcp.tool
async def delete_row(
    ctx: Context,
    table_id: str,
    row_indices: List[int]
) -> Dict[str, Any]:
    """
    Delete rows from the table by index.

    Args:
        table_id: ID of the target table
        row_indices: List of row indices to delete (0-based)

    Returns:
        Dict containing success status and deletion information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        original_shape = table.shape.copy()

        # Validate row indices
        invalid_indices = [idx for idx in row_indices if idx >= len(table.df) or idx < 0]
        if invalid_indices:
            return {
                "success": False,
                "error": f"Invalid row indices: {invalid_indices}",
                "message": "Some row indices are out of range"
            }

        table.delete_rows(row_indices)

        return {
            "success": True,
            "table_id": table_id,
            "deleted_count": len(row_indices),
            "original_shape": original_shape,
            "new_shape": table.shape,
            "message": f"Deleted {len(row_indices)} rows from table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error deleting rows from table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to delete rows from table {table_id}"
        }

# @mcp.tool
async def delete_column(
    ctx: Context,
    table_id: str,
    columns: List[str]
) -> Dict[str, Any]:
    """
    Delete columns from the table.

    Args:
        table_id: ID of the target table
        columns: List of column names to delete

    Returns:
        Dict containing success status and deletion information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        original_shape = table.shape.copy()

        # Validate column names
        invalid_columns = [col for col in columns if col not in table.headers]
        if invalid_columns:
            return {
                "success": False,
                "error": f"Invalid columns: {invalid_columns}",
                "message": "Some specified columns do not exist"
            }

        table.delete_columns(columns)

        return {
            "success": True,
            "table_id": table_id,
            "deleted_count": len(columns),
            "original_shape": original_shape,
            "new_shape": table.shape,
            "message": f"Deleted {len(columns)} columns from table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error deleting columns from table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to delete columns from table {table_id}"
        }