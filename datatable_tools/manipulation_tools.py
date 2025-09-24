from typing import Dict, List, Optional, Any, Union
import logging
import numpy as np
from core.server import register_tool
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

async def append_row(
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

async def add_column(
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

async def set_range_values(
    table_id: str,
    row_indices: List[int],
    column_names: List[str],
    values: List[List[Any]],
    fill_strategy: str = "none"
) -> Dict[str, Any]:
    """
    Update values in specified ranges using pandas .loc style updates with fill strategies.

    Args:
        table_id: ID of the target table
        row_indices: List of row indices to update
        column_names: List of column names to update
        values: 2D array of new values [rows x columns]
        fill_strategy: How to handle dimension mismatches ("none", "fill_na", "fill_empty", "fill_zero")

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

        # Validate row indices
        max_row_idx = max(row_indices) if row_indices else 0
        if max_row_idx >= len(table.df):
            return {
                "success": False,
                "error": f"Row index {max_row_idx} out of range (table has {len(table.df)} rows)",
                "message": "Invalid row indices specified"
            }

        # Validate column names
        invalid_columns = [col for col in column_names if col not in table.headers]
        if invalid_columns:
            return {
                "success": False,
                "error": f"Invalid columns: {invalid_columns}",
                "message": "Some specified columns do not exist"
            }

        # Handle dimension mismatches
        if fill_strategy != "none" and values:
            target_rows = len(row_indices)
            target_cols = len(column_names)

            # Pad values array if needed
            for i in range(len(values), target_rows):
                if fill_strategy == "fill_na":
                    values.append([np.nan] * target_cols)
                elif fill_strategy == "fill_empty":
                    values.append([""] * target_cols)
                elif fill_strategy == "fill_zero":
                    values.append([0] * target_cols)

            for i in range(len(values)):
                for j in range(len(values[i]), target_cols):
                    if fill_strategy == "fill_na":
                        values[i].append(np.nan)
                    elif fill_strategy == "fill_empty":
                        values[i].append("")
                    elif fill_strategy == "fill_zero":
                        values[i].append(0)

        table.set_values(row_indices, column_names, values)

        return {
            "success": True,
            "table_id": table_id,
            "rows_updated": len(row_indices),
            "columns_updated": len(column_names),
            "fill_strategy": fill_strategy,
            "message": f"Updated {len(row_indices)} rows and {len(column_names)} columns in table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error setting range values in table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to update range in table {table_id}"
        }

async def delete_from_table(
    table_id: str,
    target_type: str,
    target_names: List[Union[str, int]]
) -> Dict[str, Any]:
    """
    Unified deletion of rows or columns from the table.

    Args:
        table_id: ID of the target table
        target_type: Type of deletion ("rows" or "columns")
        target_names: List of row indices (for rows) or column names (for columns)

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

        if target_type.lower() == "rows":
            row_indices = [int(idx) for idx in target_names]
            # Validate row indices
            invalid_indices = [idx for idx in row_indices if idx >= len(table.df) or idx < 0]
            if invalid_indices:
                return {
                    "success": False,
                    "error": f"Invalid row indices: {invalid_indices}",
                    "message": "Some row indices are out of range"
                }
            table.delete_rows(row_indices)

        elif target_type.lower() == "columns":
            column_names = [str(name) for name in target_names]
            # Validate column names
            invalid_columns = [col for col in column_names if col not in table.headers]
            if invalid_columns:
                return {
                    "success": False,
                    "error": f"Invalid columns: {invalid_columns}",
                    "message": "Some specified columns do not exist"
                }
            table.delete_columns(column_names)

        else:
            return {
                "success": False,
                "error": f"Invalid target_type: {target_type}",
                "message": "target_type must be 'rows' or 'columns'"
            }

        return {
            "success": True,
            "table_id": table_id,
            "target_type": target_type,
            "deleted_count": len(target_names),
            "original_shape": original_shape,
            "new_shape": table.shape,
            "message": f"Deleted {len(target_names)} {target_type} from table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error deleting {target_type} from table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to delete {target_type} from table {table_id}"
        }

async def rename_columns(
    table_id: str,
    column_mapping: Dict[str, str]
) -> Dict[str, Any]:
    """
    Rename multiple columns in bulk.

    Args:
        table_id: ID of the target table
        column_mapping: Dictionary mapping old column names to new column names

    Returns:
        Dict containing success status and rename information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        # Validate that all old column names exist
        invalid_columns = [old_name for old_name in column_mapping.keys() if old_name not in table.headers]
        if invalid_columns:
            return {
                "success": False,
                "error": f"Invalid columns: {invalid_columns}",
                "message": "Some specified columns do not exist"
            }

        # Check for duplicate new names
        new_names = list(column_mapping.values())
        if len(new_names) != len(set(new_names)):
            return {
                "success": False,
                "error": "Duplicate new column names detected",
                "message": "New column names must be unique"
            }

        # Perform rename
        with table._lock:
            table.df = table.df.rename(columns=column_mapping)
            table._update_modified_time()

        return {
            "success": True,
            "table_id": table_id,
            "renamed_columns": len(column_mapping),
            "column_mapping": column_mapping,
            "new_headers": table.headers,
            "message": f"Renamed {len(column_mapping)} columns in table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error renaming columns in table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to rename columns in table {table_id}"
        }

async def clear_range(
    table_id: str,
    row_indices: Optional[List[int]] = None,
    column_names: Optional[List[str]] = None,
    clear_value: Any = None
) -> Dict[str, Any]:
    """
    Clear values in specified range while preserving table structure.

    Args:
        table_id: ID of the target table
        row_indices: Optional list of row indices to clear (if None, clears all rows)
        column_names: Optional list of column names to clear (if None, clears all columns)
        clear_value: Value to set cleared cells to (default: None/NaN)

    Returns:
        Dict containing success status and clear operation information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        # Default to all rows/columns if not specified
        if row_indices is None:
            row_indices = list(range(len(table.df)))
        if column_names is None:
            column_names = table.headers.copy()

        # Validate inputs
        invalid_rows = [idx for idx in row_indices if idx >= len(table.df) or idx < 0]
        if invalid_rows:
            return {
                "success": False,
                "error": f"Invalid row indices: {invalid_rows}",
                "message": "Some row indices are out of range"
            }

        invalid_columns = [col for col in column_names if col not in table.headers]
        if invalid_columns:
            return {
                "success": False,
                "error": f"Invalid columns: {invalid_columns}",
                "message": "Some specified columns do not exist"
            }

        # Clear the specified range
        with table._lock:
            for row_idx in row_indices:
                for col_name in column_names:
                    table.df.at[row_idx, col_name] = clear_value
            table._update_modified_time()

        return {
            "success": True,
            "table_id": table_id,
            "cleared_rows": len(row_indices),
            "cleared_columns": len(column_names),
            "clear_value": clear_value,
            "total_cells_cleared": len(row_indices) * len(column_names),
            "message": f"Cleared {len(row_indices) * len(column_names)} cells in table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error clearing range in table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to clear range in table {table_id}"
        }

# Register all tool functions
register_tool("add_row", append_row)
register_tool("add_column", add_column) 
register_tool("update_cell", set_range_values)
register_tool("delete_row", delete_from_table)
register_tool("delete_column", rename_columns)
register_tool("sort_table", clear_range)