from typing import Dict, List, Optional, Any, Union, Callable
import logging
import pandas as pd
from fastmcp import Context
from core.server import mcp
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

@mcp.tool()
async def merge_tables(
    ctx: Context,
    left_table_id: str,
    right_table_id: str,
    join_type: str = "inner",
    left_on: Optional[Union[str, List[str]]] = None,
    right_on: Optional[Union[str, List[str]]] = None,
    on: Optional[Union[str, List[str]]] = None,
    suffixes: List[str] = ["_left", "_right"],
    new_table_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Merge two tables using pandas-style joins (inner/left/right/outer).

    Args:
        left_table_id: ID of the left table
        right_table_id: ID of the right table
        join_type: Type of join ("inner", "left", "right", "outer")
        left_on: Column(s) to join on in left table
        right_on: Column(s) to join on in right table
        on: Column(s) to join on (if same in both tables)
        suffixes: Suffixes for overlapping column names
        new_table_name: Name for the merged table

    Returns:
        Dict containing merged table information
    """
    try:
        left_table = table_manager.get_table(left_table_id)
        right_table = table_manager.get_table(right_table_id)

        if not left_table:
            return {
                "success": False,
                "error": f"Left table {left_table_id} not found",
                "message": "Left table does not exist"
            }

        if not right_table:
            return {
                "success": False,
                "error": f"Right table {right_table_id} not found",
                "message": "Right table does not exist"
            }

        # Validate join type
        valid_joins = ["inner", "left", "right", "outer"]
        if join_type not in valid_joins:
            return {
                "success": False,
                "error": f"Invalid join type: {join_type}. Valid types: {valid_joins}",
                "message": "Invalid join type specified"
            }

        # Determine join keys
        if on is not None:
            # Use same column(s) for both tables
            join_keys = {"on": on}
            if isinstance(on, str):
                missing_left = on not in left_table.headers
                missing_right = on not in right_table.headers
            else:
                missing_left = any(col not in left_table.headers for col in on)
                missing_right = any(col not in right_table.headers for col in on)

            if missing_left or missing_right:
                return {
                    "success": False,
                    "error": f"Join column(s) {on} not found in one or both tables",
                    "message": "Specified join columns do not exist"
                }
        else:
            # Use different columns for each table
            if left_on is None or right_on is None:
                return {
                    "success": False,
                    "error": "Either 'on' or both 'left_on' and 'right_on' must be specified",
                    "message": "Missing join column specification"
                }

            join_keys = {"left_on": left_on, "right_on": right_on}

            # Validate left join columns
            if isinstance(left_on, str):
                if left_on not in left_table.headers:
                    return {
                        "success": False,
                        "error": f"Left join column '{left_on}' not found in left table",
                        "message": "Left join column does not exist"
                    }
            else:
                missing_left = [col for col in left_on if col not in left_table.headers]
                if missing_left:
                    return {
                        "success": False,
                        "error": f"Left join columns {missing_left} not found in left table",
                        "message": "Some left join columns do not exist"
                    }

            # Validate right join columns
            if isinstance(right_on, str):
                if right_on not in right_table.headers:
                    return {
                        "success": False,
                        "error": f"Right join column '{right_on}' not found in right table",
                        "message": "Right join column does not exist"
                    }
            else:
                missing_right = [col for col in right_on if col not in right_table.headers]
                if missing_right:
                    return {
                        "success": False,
                        "error": f"Right join columns {missing_right} not found in right table",
                        "message": "Some right join columns do not exist"
                    }

        # Perform merge
        merged_df = pd.merge(
            left_table.df,
            right_table.df,
            how=join_type,
            suffixes=suffixes,
            **join_keys
        )

        # Create new table with merged results
        new_name = new_table_name or f"Merged_{left_table.metadata.name}_{right_table.metadata.name}"
        new_table_id = table_manager.create_table(
            data=merged_df.values.tolist(),
            headers=merged_df.columns.tolist(),
            name=new_name,
            source_info={
                "type": "merged_table",
                "left_table_id": left_table_id,
                "right_table_id": right_table_id,
                "join_type": join_type,
                "join_keys": join_keys,
                "suffixes": suffixes
            }
        )

        return {
            "success": True,
            "new_table_id": new_table_id,
            "new_table_name": new_name,
            "join_type": join_type,
            "left_table_id": left_table_id,
            "right_table_id": right_table_id,
            "left_rows": len(left_table.df),
            "right_rows": len(right_table.df),
            "merged_rows": len(merged_df),
            "merged_columns": len(merged_df.columns),
            "join_keys": join_keys,
            "message": f"Merged tables {left_table_id} and {right_table_id} using {join_type} join. Result: {len(merged_df)} rows, {len(merged_df.columns)} columns"
        }

    except Exception as e:
        logger.error(f"Error merging tables {left_table_id} and {right_table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to merge tables {left_table_id} and {right_table_id}"
        }

@mcp.tool()
async def aggregate_data(
    ctx: Context,
    table_id: str,
    group_by: List[str],
    aggregations: Dict[str, Union[str, List[str]]],
    new_table_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Group by specified columns and apply multiple aggregation functions.

    Args:
        table_id: ID of the table to aggregate
        group_by: List of columns to group by
        aggregations: Dict mapping column names to aggregation functions
                     {"column": "function"} or {"column": ["func1", "func2"]}
                     Valid functions: count, sum, mean, median, min, max, std, var, first, last
        new_table_name: Name for the aggregated table

    Returns:
        Dict containing aggregated table information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Source table does not exist"
            }

        # Validate group by columns
        missing_group_cols = [col for col in group_by if col not in table.headers]
        if missing_group_cols:
            return {
                "success": False,
                "error": f"Group by columns {missing_group_cols} not found in table",
                "message": "Some group by columns do not exist"
            }

        # Validate aggregation columns
        missing_agg_cols = [col for col in aggregations.keys() if col not in table.headers]
        if missing_agg_cols:
            return {
                "success": False,
                "error": f"Aggregation columns {missing_agg_cols} not found in table",
                "message": "Some aggregation columns do not exist"
            }

        # Validate aggregation functions
        valid_functions = ["count", "sum", "mean", "median", "min", "max", "std", "var", "first", "last", "nunique"]
        for column, functions in aggregations.items():
            if isinstance(functions, str):
                functions = [functions]
            invalid_funcs = [f for f in functions if f not in valid_functions]
            if invalid_funcs:
                return {
                    "success": False,
                    "error": f"Invalid aggregation functions: {invalid_funcs}. Valid functions: {valid_functions}",
                    "message": "Invalid aggregation functions specified"
                }

        # Perform aggregation
        grouped = table.df.groupby(group_by)

        # Build aggregation dictionary for pandas
        agg_dict = {}
        for column, functions in aggregations.items():
            if isinstance(functions, str):
                agg_dict[column] = functions
            else:
                agg_dict[column] = functions

        aggregated_df = grouped.agg(agg_dict)

        # Flatten column names if multiple functions were used
        if any(isinstance(funcs, list) and len(funcs) > 1 for funcs in aggregations.values()):
            aggregated_df.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in aggregated_df.columns]

        # Reset index to make group by columns regular columns
        aggregated_df = aggregated_df.reset_index()

        # Create new table with aggregated results
        new_name = new_table_name or f"{table.metadata.name} (Aggregated)"
        new_table_id = table_manager.create_table(
            data=aggregated_df.values.tolist(),
            headers=aggregated_df.columns.tolist(),
            name=new_name,
            source_info={
                "type": "aggregated_table",
                "source_table_id": table_id,
                "group_by": group_by,
                "aggregations": aggregations
            }
        )

        return {
            "success": True,
            "new_table_id": new_table_id,
            "new_table_name": new_name,
            "source_table_id": table_id,
            "group_by": group_by,
            "aggregations": aggregations,
            "original_rows": len(table.df),
            "aggregated_rows": len(aggregated_df),
            "aggregated_columns": len(aggregated_df.columns),
            "message": f"Aggregated table {table_id} by {group_by}. Result: {len(aggregated_df)} rows, {len(aggregated_df.columns)} columns"
        }

    except Exception as e:
        logger.error(f"Error aggregating table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to aggregate table {table_id}"
        }

@mcp.tool()
async def map_values(
    ctx: Context,
    table_id: str,
    column_mappings: Dict[str, Dict[str, Any]],
    default_value: Optional[Any] = None,
    create_new_columns: bool = False,
    new_column_suffix: str = "_mapped"
) -> Dict[str, Any]:
    """
    Apply value transformations and mapping to specified columns.

    Args:
        table_id: ID of the table to transform
        column_mappings: Dict mapping column names to value mappings
                        {"column_name": {"old_value": "new_value", ...}}
        default_value: Default value for unmapped values (if None, keeps original)
        create_new_columns: If True, creates new columns instead of modifying existing ones
        new_column_suffix: Suffix for new columns when create_new_columns=True

    Returns:
        Dict containing value mapping operation information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Source table does not exist"
            }

        # Validate columns
        missing_columns = [col for col in column_mappings.keys() if col not in table.headers]
        if missing_columns:
            return {
                "success": False,
                "error": f"Columns {missing_columns} not found in table",
                "message": "Some specified columns do not exist"
            }

        # Apply mappings
        with table._lock:
            mapping_stats = {}

            for column, value_mapping in column_mappings.items():
                if create_new_columns:
                    new_column_name = f"{column}{new_column_suffix}"
                    # Create new column with mapped values
                    table.df[new_column_name] = table.df[column].map(value_mapping)
                    if default_value is not None:
                        table.df[new_column_name] = table.df[new_column_name].fillna(default_value)
                    else:
                        # Fill unmapped values with original values
                        table.df[new_column_name] = table.df[new_column_name].fillna(table.df[column])

                    # Count mappings
                    mapped_count = table.df[column].isin(value_mapping.keys()).sum()
                    mapping_stats[new_column_name] = {
                        "total_rows": len(table.df),
                        "mapped_rows": int(mapped_count),
                        "unmapped_rows": len(table.df) - int(mapped_count),
                        "mapping_rules": len(value_mapping)
                    }
                else:
                    # Modify existing column
                    original_values = table.df[column].copy()
                    table.df[column] = table.df[column].map(value_mapping)

                    if default_value is not None:
                        table.df[column] = table.df[column].fillna(default_value)
                    else:
                        # Fill unmapped values with original values
                        table.df[column] = table.df[column].fillna(original_values)

                    # Count mappings
                    mapped_count = original_values.isin(value_mapping.keys()).sum()
                    mapping_stats[column] = {
                        "total_rows": len(table.df),
                        "mapped_rows": int(mapped_count),
                        "unmapped_rows": len(table.df) - int(mapped_count),
                        "mapping_rules": len(value_mapping)
                    }

            table._update_modified_time()

        return {
            "success": True,
            "table_id": table_id,
            "column_mappings": column_mappings,
            "create_new_columns": create_new_columns,
            "new_column_suffix": new_column_suffix if create_new_columns else None,
            "default_value": default_value,
            "mapping_statistics": mapping_stats,
            "total_columns_processed": len(column_mappings),
            "message": f"Applied value mappings to {len(column_mappings)} columns in table {table_id}"
        }

    except Exception as e:
        logger.error(f"Error mapping values in table {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to map values in table {table_id}"
        }