from typing import Dict, List, Optional, Any, Union
import logging
import json
from fastmcp import Context
from core.server import mcp
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

@mcp.tool()
async def get_table_data(
    ctx: Context,
    table_id: str,
    output_format: str = "dict",
    start_row: Optional[int] = None,
    end_row: Optional[int] = None,
    columns: Optional[List[str]] = None,
    max_rows: Optional[int] = 1000
) -> Dict[str, Any]:
    """
    Get table data with flexible slicing and multiple output formats.

    Args:
        table_id: ID of the target table
        output_format: Output format ("dict", "records", "values", "json")
        start_row: Starting row index (inclusive)
        end_row: Ending row index (exclusive)
        columns: Optional list of specific columns to return
        max_rows: Maximum number of rows to return (safety limit)

    Returns:
        Dict containing table data in requested format
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        # Apply row slicing
        if start_row is not None or end_row is not None:
            start_row = start_row or 0
            end_row = end_row or len(table.df)
            row_slice = slice(start_row, min(end_row, start_row + (max_rows or 1000)))
        else:
            row_slice = slice(0, min(len(table.df), max_rows or 1000))

        # Get sliced data
        df_slice = table.get_slice(rows=row_slice, columns=columns)

        # Format output based on requested format
        if output_format == "dict":
            data = df_slice.to_dict('dict')
        elif output_format == "records":
            data = df_slice.to_dict('records')
        elif output_format == "values":
            data = df_slice.values.tolist()
        elif output_format == "json":
            data = df_slice.to_json(orient='records')
        else:
            return {
                "success": False,
                "error": f"Invalid output format: {output_format}",
                "message": "output_format must be one of: dict, records, values, json"
            }

        return {
            "success": True,
            "table_id": table_id,
            "data": data,
            "headers": df_slice.columns.tolist(),
            "shape": list(df_slice.shape),
            "output_format": output_format,
            "slice_info": {
                "start_row": row_slice.start,
                "end_row": row_slice.stop,
                "columns": columns or "all"
            },
            "message": f"Retrieved {df_slice.shape[0]} rows and {df_slice.shape[1]} columns from table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error getting data from table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to get data from table {table_id}"
        }

@mcp.tool()
async def filter_rows(
    ctx: Context,
    table_id: str,
    conditions: List[Dict[str, Any]],
    logic: str = "AND",
    in_place: bool = True
) -> Dict[str, Any]:
    """
    Filter table rows based on multiple conditions with AND/OR logic.

    Args:
        table_id: ID of the target table
        conditions: List of filter conditions. Each condition should have:
                   {"column": "col_name", "operator": "eq|ne|gt|gte|lt|lte|contains|startswith|endswith|isnull|notnull", "value": filter_value}
        logic: Logic operator for combining conditions ("AND" or "OR")
        in_place: If True, modifies the original table; if False, creates a new table

    Returns:
        Dict containing filtered results and operation information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        # Validate conditions
        valid_operators = ["eq", "ne", "gt", "gte", "lt", "lte", "contains", "startswith", "endswith", "isnull", "notnull"]
        for condition in conditions:
            if "column" not in condition or "operator" not in condition:
                return {
                    "success": False,
                    "error": "Each condition must have 'column' and 'operator' fields",
                    "message": "Invalid condition format"
                }
            if condition["operator"] not in valid_operators:
                return {
                    "success": False,
                    "error": f"Invalid operator: {condition['operator']}. Valid operators: {valid_operators}",
                    "message": "Invalid filter operator"
                }
            if condition["column"] not in table.headers:
                return {
                    "success": False,
                    "error": f"Column '{condition['column']}' not found in table",
                    "message": "Invalid column name in filter condition"
                }

        # Apply filter
        filtered_df = table.filter_rows(conditions, logic)

        result = {
            "success": True,
            "table_id": table_id,
            "original_rows": len(table.df),
            "filtered_rows": len(filtered_df),
            "conditions": conditions,
            "logic": logic,
            "message": f"Filtered table {table_id}: {len(filtered_df)} rows match the criteria"
        }

        if not in_place:
            # Create new table with filtered results
            new_name = f"{table.metadata.name} (Filtered)"
            new_table_id = table_manager.create_table(
                data=filtered_df.values.tolist(),
                headers=filtered_df.columns.tolist(),
                name=new_name,
                source_info={
                    "type": "filtered_table",
                    "source_table_id": table_id,
                    "filter_conditions": conditions,
                    "filter_logic": logic
                }
            )
            result["new_table_id"] = new_table_id
            result["new_table_name"] = new_name
        else:
            # Update original table in-place
            with table._lock:
                table.df = filtered_df
                table._update_modified_time()
            result["data"] = filtered_df.to_dict('records')
            result["headers"] = filtered_df.columns.tolist()
            result["message"] += " (in-place)"

        return result

    except Exception as e:
        logger.error(f"Error filtering table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to filter table {table_id}"
        }

@mcp.tool()
async def sort_table(
    ctx: Context,
    table_id: str,
    sort_columns: List[str],
    ascending: Optional[List[bool]] = None,
    in_place: bool = True
) -> Dict[str, Any]:
    """
    Sort table by multiple columns with option to create new table or modify in-place.

    Args:
        table_id: ID of the target table
        sort_columns: List of column names to sort by (in order of priority)
        ascending: List of boolean values indicating sort direction for each column (True = ascending)
        in_place: If True, modifies the original table; if False, creates a new table

    Returns:
        Dict containing sort operation information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        # Validate sort columns
        invalid_columns = [col for col in sort_columns if col not in table.headers]
        if invalid_columns:
            return {
                "success": False,
                "error": f"Invalid columns: {invalid_columns}",
                "message": "Some specified sort columns do not exist"
            }

        # Default ascending to True for all columns if not specified
        if ascending is None:
            ascending = [True] * len(sort_columns)
        elif len(ascending) != len(sort_columns):
            return {
                "success": False,
                "error": f"Length mismatch: {len(sort_columns)} sort columns but {len(ascending)} ascending values",
                "message": "Number of ascending values must match number of sort columns"
            }

        # Perform sort
        sorted_df = table.sort_table(sort_columns, ascending)

        result = {
            "success": True,
            "table_id": table_id,
            "sort_columns": sort_columns,
            "ascending": ascending,
            "in_place": in_place,
            "message": f"Sorted table {table_id} by columns: {sort_columns}"
        }

        if in_place:
            # Update original table
            with table._lock:
                table.df = sorted_df
                table._update_modified_time()
            result["message"] += " (in-place)"
        else:
            # Create new table with sorted results
            new_name = f"{table.metadata.name} (Sorted)"
            new_table_id = table_manager.create_table(
                data=sorted_df.values.tolist(),
                headers=sorted_df.columns.tolist(),
                name=new_name,
                source_info={
                    "type": "sorted_table",
                    "source_table_id": table_id,
                    "sort_columns": sort_columns,
                    "ascending": ascending
                }
            )
            result["new_table_id"] = new_table_id
            result["new_table_name"] = new_name
            result["message"] += f" (new table: {new_table_id})"

        return result

    except Exception as e:
        logger.error(f"Error sorting table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to sort table {table_id}"
        }

@mcp.tool()
async def filter_table(
    ctx: Context,
    table_id: str,
    query: str,
    in_place: bool = True
) -> Dict[str, Any]:
    """
    Filter table rows using simple pandas query syntax (like DataFrame filter).

    Args:
        table_id: ID of the target table
        query: Pandas query string (e.g., "Age > 25", "Name == 'John'", "Age > 20 and Role == 'Engineer'")
        in_place: If True, modifies the original table; if False, creates a new table

    Returns:
        Dict containing filtered results and operation information

    Examples:
        - query="Age > 25"
        - query="Name == 'John'"
        - query="Age > 20 and Role == 'Engineer'"
        - query="Age >= 25 or Department == 'IT'"
        - query="Name.str.contains('John')"
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Target table does not exist"
            }

        # Apply filter using pandas query
        try:
            filtered_df = table.filter_by_query(query)
        except Exception as e:
            return {
                "success": False,
                "error": f"Invalid query: {str(e)}",
                "message": f"Query syntax error. Example: 'Age > 25' or 'Name == \"John\"'"
            }

        result = {
            "success": True,
            "table_id": table_id,
            "original_rows": len(table.df),
            "filtered_rows": len(filtered_df),
            "query": query,
            "message": f"Filtered table {table_id}: {len(filtered_df)} rows match the query '{query}'"
        }

        if not in_place:
            # Create new table with filtered results
            new_name = f"{table.metadata.name} (Filtered)"
            new_table_id = table_manager.create_table(
                data=filtered_df.values.tolist(),
                headers=filtered_df.columns.tolist(),
                name=new_name,
                source_info={
                    "type": "filtered_table",
                    "source_table_id": table_id,
                    "filter_query": query
                }
            )
            result["new_table_id"] = new_table_id
            result["new_table_name"] = new_name
        else:
            # Update original table in-place
            with table._lock:
                table.df = filtered_df
                table._update_modified_time()
            result["data"] = filtered_df.to_dict('records')
            result["headers"] = filtered_df.columns.tolist()
            result["message"] += " (in-place)"

        return result

    except Exception as e:
        logger.error(f"Error filtering table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to filter table {table_id}"
        }

