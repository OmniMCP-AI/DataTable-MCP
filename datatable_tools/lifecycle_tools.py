from typing import Dict, List, Optional, Any
import logging
from fastmcp import Context
from core.server import mcp
from datatable_tools.table_manager import table_manager
from datatable_tools.auth.service_decorator import require_google_service

logger = logging.getLogger(__name__)

@mcp.tool()
async def create_table(
    ctx: Context,
    data: Any,
    headers: Optional[List[str]] = None,
    name: str = "Untitled Table"
) -> Dict[str, Any]:
    """
    Create a new DataTable from various data formats (similar to pd.DataFrame).

    Args:
        data: Data in various formats:
              - List[List[Any]]: 2D array of table data (rows x columns)
              - Dict[str, List]: Dictionary with column names as keys and column data as values
              - Dict[str, Any]: Dictionary with column names as keys and scalar/sequence values
              - List[Dict]: List of dictionaries (records format)
              - pandas.DataFrame: Existing DataFrame
              - pandas.Series: Single column data
              - List[Any]: Single column or row data
              - scalar: Single value
        headers: Optional column headers. If not provided, will auto-generate or use data keys
        name: Optional table name for identification

    Returns:
        Dict containing table_id and basic table information

    Examples:
        # 2D list (traditional format)
        data = [["Alice", 25], ["Bob", 30]]
        headers = ["Name", "Age"]

        # Dictionary format (like pd.DataFrame)
        data = {"Name": ["Alice", "Bob"], "Age": [25, 30]}

        # Records format
        data = [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]

        # Single column
        data = [1, 2, 3, 4, 5]
        headers = ["Numbers"]
    """
    logger.info(f"create_table called with data={data}, headers={headers}, name={name}")
    try:
        # Convert input data to standardized format
        processed_data, processed_headers = _process_data_input(data, headers)

        table_id = table_manager.create_table(
            data=processed_data,
            headers=processed_headers,
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
async def load_table(
    ctx: Context,
    uri: str,
    name: Optional[str] = None,
    query: Optional[str] = None,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load a table from various data sources using URI-based auto-detection.

    Args:
        uri: URI to the data source. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{id}/edit or spreadsheet ID
             - CSV files: /path/to/file.csv or https://example.com/data.csv
             - Excel files: /path/to/file.xlsx or https://example.com/data.xlsx
             - JSON files: /path/to/file.json
             - Database: postgresql://user:pass@host:port/db
        name: Optional table name
        query: SQL query for database sources (required for databases)
        encoding: File encoding for CSV files (optional)
        delimiter: Delimiter for CSV files (optional)

    Returns:
        Dict containing table_id and loaded table information

    Examples:
        # Google Sheets
        uri = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"

        # CSV file
        uri = "/path/to/data.csv"

        # Database
        uri = "postgresql://user:pass@localhost:5432/mydb"
        query = "SELECT * FROM users"
    """
    try:
        from datatable_tools.utils import parse_source_uri, detect_source_type
        from datatable_tools.data_sources import create_data_source

        # Parse the URI to determine source type and parameters
        source_info = parse_source_uri(uri)
        source_type = source_info["source_type"]

        logger.info(f"Loading table from {source_type} source: {uri}")

        # Handle Google Sheets with authentication
        if source_type == "google_sheets":
            return await _load_google_sheets(ctx, source_info, name)

        # Validate database query requirement
        if source_type == "database" and not query:
            return {
                "success": False,
                "error": "query is required for database sources",
                "message": "Please provide a SQL query for database sources"
            }

        # Create appropriate data source
        source_params = _build_source_params(source_info, encoding, delimiter, query)
        data_source = create_data_source(source_type, **source_params)

        # Load data from source
        data, headers, metadata = await data_source.load_data()

        # Enhance metadata with original URI
        metadata.update({
            "original_uri": uri,
            "detected_type": source_type
        })

        # Create table
        table_id = table_manager.create_table(
            data=data,
            headers=headers,
            name=name or f"Loaded from {source_type}",
            source_info=metadata
        )

        table = table_manager.get_table(table_id)
        return {
            "success": True,
            "table_id": table_id,
            "name": table.metadata.name,
            "shape": table.shape,
            "headers": table.headers,
            "source_type": source_type,
            "source_info": metadata,
            "message": f"Loaded table from {source_type} with {table.shape[0]} rows and {table.shape[1]} columns"
        }

    except Exception as e:
        logger.error(f"Error loading table from {uri}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to load table from {uri}"
        }


@require_google_service("sheets", "sheets_read")
async def _load_google_sheets(service, ctx: Context, source_info: dict, name: Optional[str] = None) -> Dict[str, Any]:
    """
    Internal function to load Google Sheets with authentication.

    Args:
        service: Authenticated Google Sheets service
        ctx: Context
        source_info: Parsed source information
        name: Optional table name
    """
    from datatable_tools.third_party.google_sheets.service import GoogleSheetsService

    spreadsheet_id = source_info["spreadsheet_id"]
    sheet_name = source_info["sheet_name"]
    original_uri = source_info["original_uri"]

    # Load data from Google Sheets
    response = await GoogleSheetsService.read_sheet_structured(
        service, ctx, spreadsheet_id, sheet_name
    )

    if not response.get("success"):
        raise Exception(f"Failed to read spreadsheet: {response.get('message', 'Unknown error')}")

    # Extract data and headers
    headers = response.get("headers", [])
    data = response.get("data", [])

    # Create enhanced source_info
    metadata = {
        "type": "google_sheets",
        "spreadsheet_id": spreadsheet_id,
        "original_uri": original_uri,
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
        source_info=metadata
    )

    table = table_manager.get_table(table_id)
    return {
        "success": True,
        "table_id": table_id,
        "name": table.metadata.name,
        "shape": table.shape,
        "headers": table.headers,
        "source_type": "google_sheets",
        "source_info": metadata,
        "message": f"Loaded table from Google Sheets with {table.shape[0]} rows and {table.shape[1]} columns"
    }


def _build_source_params(source_info: dict, encoding: Optional[str], delimiter: Optional[str], query: Optional[str]) -> dict:
    """
    Build parameters for data source creation based on source type.

    Args:
        source_info: Parsed source information
        encoding: File encoding
        delimiter: CSV delimiter
        query: SQL query

    Returns:
        Parameters dictionary for data source
    """
    source_type = source_info["source_type"]

    if source_type == "database":
        return {
            "connection_string": source_info["connection_string"],
            "query": query
        }
    elif source_type in ["csv", "excel", "json", "file"]:
        params = {
            "file_path": source_info["file_path"]
        }
        if encoding:
            params["encoding"] = encoding
        if delimiter:
            params["delimiter"] = delimiter
        return params
    else:
        return {}

# @mcp.tool()
async def clone_table(
    ctx: Context,
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
async def list_tables(ctx: Context) -> Dict[str, Any]:
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


def _process_data_input(data: Any, headers: Optional[List[str]] = None) -> tuple[List[List[Any]], List[str]]:
    """
    Process various data input formats into standardized format for table creation.

    Args:
        data: Input data in various formats
        headers: Optional headers

    Returns:
        Tuple of (processed_data, processed_headers)
    """
    import pandas as pd
    import numpy as np

    # Handle pandas DataFrame
    if isinstance(data, pd.DataFrame):
        processed_headers = headers or list(data.columns)
        processed_data = data.values.tolist()
        return processed_data, processed_headers

    # Handle pandas Series
    if isinstance(data, pd.Series):
        processed_headers = headers or [data.name or "Series"]
        processed_data = [[value] for value in data.tolist()]
        return processed_data, processed_headers

    # Handle numpy arrays
    if isinstance(data, np.ndarray):
        if data.ndim == 1:
            # 1D array - single column
            processed_headers = headers or ["Column_1"]
            processed_data = [[value] for value in data.tolist()]
        elif data.ndim == 2:
            # 2D array - multiple columns
            processed_headers = headers or [f"Column_{i+1}" for i in range(data.shape[1])]
            processed_data = data.tolist()
        else:
            raise ValueError(f"Unsupported numpy array dimension: {data.ndim}")
        return processed_data, processed_headers

    # Handle dictionary formats
    if isinstance(data, dict):
        if not data:
            # Empty dict
            processed_headers = headers or []
            processed_data = []
            return processed_data, processed_headers

        # Check if it's column-oriented data (dict of lists/arrays)
        first_key = next(iter(data.keys()))
        first_value = data[first_key]

        if isinstance(first_value, (list, tuple, np.ndarray, pd.Series)):
            # Column-oriented: {"col1": [1,2,3], "col2": [4,5,6]}
            processed_headers = headers or list(data.keys())

            # Get the length of data (assume all columns have same length)
            lengths = [len(v) if hasattr(v, '__len__') else 1 for v in data.values()]
            if len(set(lengths)) > 1:
                raise ValueError("All columns must have the same length")

            num_rows = lengths[0] if lengths else 0
            processed_data = []

            for i in range(num_rows):
                row = []
                for col_name in processed_headers:
                    if col_name in data:
                        col_data = data[col_name]
                        if hasattr(col_data, '__getitem__'):
                            row.append(col_data[i])
                        else:
                            row.append(col_data)  # scalar value
                    else:
                        row.append(None)  # missing column
                processed_data.append(row)

        else:
            # Single row: {"col1": 1, "col2": 2}
            processed_headers = headers or list(data.keys())
            processed_data = [[data.get(col, None) for col in processed_headers]]

        return processed_data, processed_headers

    # Handle list of dictionaries (records format)
    if isinstance(data, list) and data and isinstance(data[0], dict):
        # Records format: [{"col1": 1, "col2": 2}, {"col1": 3, "col2": 4}]
        all_keys = set()
        for record in data:
            if isinstance(record, dict):
                all_keys.update(record.keys())

        processed_headers = headers or sorted(list(all_keys))
        processed_data = []

        for record in data:
            if isinstance(record, dict):
                row = [record.get(col, None) for col in processed_headers]
                processed_data.append(row)

        return processed_data, processed_headers

    # Handle 2D list (traditional format)
    if isinstance(data, list) and data and isinstance(data[0], (list, tuple)):
        # 2D list: [[1,2], [3,4]]
        processed_data = [list(row) for row in data]
        num_cols = len(processed_data[0]) if processed_data else 0
        processed_headers = headers or [f"Column_{i+1}" for i in range(num_cols)]
        return processed_data, processed_headers

    # Handle 1D list/tuple (single column or single row)
    if isinstance(data, (list, tuple)):
        if not data:
            # Empty list
            processed_headers = headers or []
            processed_data = []
            return processed_data, processed_headers

        # Treat as single column by default
        processed_headers = headers or ["Column_1"]
        processed_data = [[item] for item in data]
        return processed_data, processed_headers

    # Handle scalar values
    if isinstance(data, (int, float, str, bool, type(None))):
        processed_headers = headers or ["Value"]
        processed_data = [[data]]
        return processed_data, processed_headers

    # Fallback: try to convert to DataFrame and process
    try:
        df = pd.DataFrame(data)
        processed_headers = headers or list(df.columns)
        processed_data = df.values.tolist()
        return processed_data, processed_headers
    except Exception as e:
        raise ValueError(f"Unsupported data format: {type(data)}. Error: {str(e)}")