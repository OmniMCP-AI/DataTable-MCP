from typing import Dict, List, Optional, Any
import logging
from fastmcp import Context
from core.server import mcp
from datatable_tools.table_manager import table_manager
from datatable_tools.auth.service_decorator import require_google_service

logger = logging.getLogger(__name__)

@mcp.tool()
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

@mcp.tool()
@require_google_service("sheets", "sheets_read")
async def test_google_sheets_connection(
    service,
    ctx: Context,
    spreadsheet_id: str
) -> Dict[str, Any]:
    """
    Test Google Sheets connection by getting basic spreadsheet info.

    Args:
        spreadsheet_id: Spreadsheet ID to test

    Returns:
        Dict containing spreadsheet metadata
    """
    try:
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

        # Simple spreadsheet info call - should be fast
        info = await GoogleSheetsService.get_spreadsheet_info(service, ctx, spreadsheet_id)

        return {
            "success": True,
            "spreadsheet_info": info,
            "message": f"Successfully connected to spreadsheet: {info.get('title', 'Unknown')}"
        }

    except Exception as e:
        logger.error(f"Error testing Google Sheets connection: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to connect to Google Sheets"
        }

@mcp.tool()
@require_google_service("sheets", "sheets_read")
async def load_table_google_sheets(
    service,
    ctx: Context,
    source_path: str,
    name: Optional[str] = None,
    sheet_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load a table from Google Sheets.

    Args:
        source_path: Spreadsheet ID
        name: Optional table name
        sheet_name: Sheet name (optional)

    Returns:
        Dict containing table_id and loaded table information
    """
    try:
        from datatable_tools.data_sources import create_data_source, SpreadsheetDataSource
        from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

        # Create Google Sheets data source
        source_params = {
            "spreadsheet_id": source_path,
            "worksheet": sheet_name
        }

        data_source = create_data_source("google_sheets", **source_params)

        # Load data from source - call GoogleSheetsService directly with injected service and ctx
        response = await GoogleSheetsService.read_sheet_structured(
            service,  # Use the authenticated service from the decorator
            ctx,      # Use the context from the decorator
            source_path,  # spreadsheet_id
            sheet_name    # sheet_name
        )

        if not response.get("success"):
            raise Exception(f"Failed to read spreadsheet: {response.get('message', 'Unknown error')}")

        # Extract data and headers
        headers = response.get("headers", [])
        data = response.get("data", [])

        # Create source_info from the response
        source_info = {
            "type": "google_sheets",
            "spreadsheet_id": source_path,
            "worksheet": response["worksheet"]["title"],
            "used_range": response.get("used_range"),
            "worksheet_url": response.get("worksheet_url"),
            "row_count": response.get("row_count", len(data)),
            "column_count": response.get("column_count", len(headers))
        }

        # Create table
        table_id = table_manager.create_table(
            data=data,
            headers=headers,
            name=name or f"Loaded from Google Sheets",
            source_info=source_info
        )

        table = table_manager.get_table(table_id)
        return {
            "success": True,
            "table_id": table_id,
            "name": table.metadata.name,
            "shape": table.shape,
            "headers": table.headers,
            "source_info": source_info,
            "message": f"Loaded table from Google Sheets with {table.shape[0]} rows and {table.shape[1]} columns"
        }

    except Exception as e:
        logger.error(f"Error loading table from Google Sheets: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to load table from Google Sheets"
        }

@mcp.tool()
async def load_table(
    source_type: str,
    source_path: str,
    name: Optional[str] = None,
    sheet_name: Optional[str] = None,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
    user_id: Optional[str] = None,
    query: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load a table from external sources (excel/csv/database). For Google Sheets, use load_table_google_sheets.

    Args:
        source_type: Type of source ("excel", "csv", "database") - NOT "google_sheets"
        source_path: Path/URL to the source (file path, database connection string)
        name: Optional table name
        sheet_name: Sheet name for Excel (optional)
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)
        user_id: Deprecated - not used
        query: SQL query for database sources (required for database)

    Returns:
        Dict containing table_id and loaded table information
    """
    try:
        from datatable_tools.data_sources import create_data_source

        # Redirect Google Sheets to the dedicated function
        if source_type == "google_sheets":
            return {
                "success": False,
                "error": "Use load_table_google_sheets for Google Sheets sources",
                "message": "Please use the load_table_google_sheets function for loading Google Sheets data"
            }

        # Validate required parameters
        if source_type == "database" and not query:
            return {
                "success": False,
                "error": "query is required for database source",
                "message": "Please provide a SQL query for database sources"
            }

        # Create appropriate data source
        source_params = {
            "file_path": source_path if source_type in ["excel", "csv"] else None,
            "sheet_name": sheet_name,
            "encoding": encoding,
            "delimiter": delimiter,
            "connection_string": source_path if source_type == "database" else None,
            "query": query
        }

        data_source = create_data_source(source_type, **source_params)

        # Load data from source
        data, headers, source_info = await data_source.load_data()

        # Create table
        table_id = table_manager.create_table(
            data=data,
            headers=headers,
            name=name or f"Loaded from {source_type}",
            source_info=source_info
        )

        table = table_manager.get_table(table_id)
        return {
            "success": True,
            "table_id": table_id,
            "name": table.metadata.name,
            "shape": table.shape,
            "headers": table.headers,
            "source_info": source_info,
            "message": f"Loaded table from {source_type} with {table.shape[0]} rows and {table.shape[1]} columns"
        }

    except Exception as e:
        logger.error(f"Error loading table from {source_type}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to load table from {source_type}"
        }

@mcp.tool()
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

@mcp.tool()
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