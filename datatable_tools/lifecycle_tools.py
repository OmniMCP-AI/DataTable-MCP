from typing import Dict, List, Optional, Any
import logging
from core.server import register_tool
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

async def create_table(
    data: List[List[Any]],
    headers: Optional[List[str]] = None,
    name: str = "Untitled Table"
) -> Dict[str, Any]:
    """
    Create a new DataTable from data array with auto-detected headers.

    Args:
        data: 2D array of table data (rows x columns)
        headers: Optional column headers. If not provided, will auto-generate (Column_1, Column_2, etc.)
        name: Optional table name for identification

    Returns:
        Dict containing table_id and basic table information
    """
    logger.info(f"create_table called with data={data}, headers={headers}, name={name}")
    try:
        table_id = table_manager.create_table(
            data=data,
            headers=headers,
            name=name,
            source_info={"type": "manual_creation"}
        )

        table = table_manager.get_table(table_id)
        if not table:
            raise Exception("Failed to create table")

        return {
            "success": True,
            "table_id": table_id,
            "name": table.metadata.name,
            "shape": table.shape,
            "headers": table.headers,
            "message": f"Created table '{name}' with {table.shape[0]} rows and {table.shape[1]} columns"
        }

    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to create table"
        }

async def load_table(
    source_type: str,
    source_path: str,
    name: Optional[str] = None,
    sheet_name: Optional[str] = None,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load a table from external sources (spreadsheet/excel/database).

    Args:
        source_type: Type of source ("google_sheets", "excel", "csv", "database")
        source_path: Path/URL to the source (file path, spreadsheet ID, database connection string)
        name: Optional table name
        sheet_name: Sheet name for Excel/Google Sheets (optional)
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)

    Returns:
        Dict containing table_id and loaded table information
    """
    try:
        import pandas as pd

        # Prepare load parameters
        load_params = {}
        if encoding:
            load_params['encoding'] = encoding
        if delimiter:
            load_params['delimiter'] = delimiter

        # Load data based on source type
        if source_type == "csv":
            df = pd.read_csv(source_path, **load_params)
        elif source_type == "excel":
            df = pd.read_excel(source_path, sheet_name=sheet_name or 0)
        elif source_type == "google_sheets":
            # Placeholder for Google Sheets integration
            raise NotImplementedError("Google Sheets integration not yet implemented")
        elif source_type == "database":
            # Placeholder for database integration
            raise NotImplementedError("Database integration not yet implemented")
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        # Create table
        table_id = table_manager.create_table(
            data=df.values.tolist(),
            headers=df.columns.tolist(),
            name=name or f"Loaded from {source_type}",
            source_info={
                "type": source_type,
                "source_path": source_path,
                "sheet_name": sheet_name,
                "load_params": load_params
            }
        )

        table = table_manager.get_table(table_id)
        return {
            "success": True,
            "table_id": table_id,
            "name": table.metadata.name,
            "shape": table.shape,
            "headers": table.headers,
            "message": f"Loaded table from {source_type} with {table.shape[0]} rows and {table.shape[1]} columns"
        }

    except Exception as e:
        logger.error(f"Error loading table from {source_type}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to load table from {source_type}"
        }

async def clone_table(
    source_table_id: str,
    new_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a deep copy of an existing table.

    Args:
        source_table_id: ID of the table to clone
        new_name: Optional name for the cloned table

    Returns:
        Dict containing new table_id and cloned table information
    """
    try:
        source_table = table_manager.get_table(source_table_id)
        if not source_table:
            return {
                "success": False,
                "error": f"Table {source_table_id} not found",
                "message": "Source table does not exist"
            }

        new_table_id = table_manager.clone_table(source_table_id, new_name)
        if not new_table_id:
            raise Exception("Failed to clone table")

        new_table = table_manager.get_table(new_table_id)
        return {
            "success": True,
            "table_id": new_table_id,
            "source_table_id": source_table_id,
            "name": new_table.metadata.name,
            "shape": new_table.shape,
            "message": f"Cloned table {source_table_id} to {new_table_id}"
        }

    except Exception as e:
        logger.error(f"Error cloning table {source_table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to clone table {source_table_id}"
        }

async def list_tables() -> Dict[str, Any]:
    """
    Get inventory of all tables in the current session.

    Returns:
        Dict containing list of all active tables with their basic information
    """
    try:
        tables_info = table_manager.list_tables()

        return {
            "success": True,
            "count": len(tables_info),
            "tables": tables_info,
            "message": f"Found {len(tables_info)} active table{'s' if len(tables_info) != 1 else ''}"
        }

    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to list tables"
        }

# Register all tool functions
register_tool("create_table", create_table)
register_tool("load_table", load_table)
register_tool("clone_table", clone_table)
register_tool("list_tables", list_tables)