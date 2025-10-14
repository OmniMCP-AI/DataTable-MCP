"""
Utility functions for DataTable MCP tools
"""
import re
import os
import logging
from typing import Optional, Tuple, List, Any
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)


def detect_export_type(uri: str) -> str:
    """
    Automatically detect the export type from a URI.

    Args:
        uri: URI/path for the export destination

    Returns:
        Export type: "google_sheets", "csv", "excel", "json", "parquet", or "file"
    """
    if not uri:
        raise ValueError("URI cannot be empty")

    # Google Sheets detection
    if is_google_sheets_url(uri):
        return "google_sheets"

    # File extension based detection
    if uri.startswith(('http://', 'https://')):
        # Remote URL - check extension
        path_part = urlparse(uri).path.lower()
        if path_part.endswith('.csv'):
            return "csv"
        elif path_part.endswith(('.xlsx', '.xls')):
            return "excel"
        elif path_part.endswith('.json'):
            return "json"
        elif path_part.endswith('.parquet'):
            return "parquet"
        else:
            return "file"

    # Local file paths
    ext = os.path.splitext(uri.lower())[1]
    if ext == '.csv':
        return "csv"
    elif ext in ['.xlsx', '.xls']:
        return "excel"
    elif ext == '.json':
        return "json"
    elif ext == '.parquet':
        return "parquet"
    else:
        return "file"


def parse_export_uri(uri: str) -> dict:
    """
    Parse an export URI and extract relevant parameters.

    Args:
        uri: URI to parse for export

    Returns:
        Dictionary with parsed parameters
    """
    export_type = detect_export_type(uri)

    if export_type == "google_sheets":
        spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
        return {
            "export_type": "google_sheets",
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
            "original_uri": uri
        }
    else:
        # File-based exports
        return {
            "export_type": export_type,
            "file_path": uri,
            "original_uri": uri
        }


def detect_source_type(uri: str) -> str:
    """
    Automatically detect the source type from a URI.

    Args:
        uri: URI/path to the data source

    Returns:
        Source type: "google_sheets", "csv", "excel", "json", "database", or "file"
    """
    if not uri:
        raise ValueError("URI cannot be empty")

    # Google Sheets detection
    if is_google_sheets_url(uri):
        return "google_sheets"

    # Database connection strings
    if uri.startswith(('postgresql://', 'mysql://', 'sqlite://', 'oracle://', 'mssql://')):
        return "database"

    # HTTP/HTTPS URLs
    if uri.startswith(('http://', 'https://')):
        # Check file extension in URL
        path_part = urlparse(uri).path.lower()
        if path_part.endswith(('.csv')):
            return "csv"
        elif path_part.endswith(('.xlsx', '.xls')):
            return "excel"
        elif path_part.endswith(('.json')):
            return "json"
        else:
            return "file"  # Generic file

    # Local file paths
    if os.path.isfile(uri) or '/' in uri or '\\' in uri:
        ext = os.path.splitext(uri.lower())[1]
        if ext == '.csv':
            return "csv"
        elif ext in ['.xlsx', '.xls']:
            return "excel"
        elif ext == '.json':
            return "json"
        else:
            return "file"

    # Fallback
    return "file"


def parse_source_uri(uri: str) -> dict:
    """
    Parse a URI and extract relevant parameters for data loading.

    Args:
        uri: URI to parse

    Returns:
        Dictionary with parsed parameters
    """
    source_type = detect_source_type(uri)

    if source_type == "google_sheets":
        spreadsheet_id, sheet_name = parse_google_sheets_url(uri)
        return {
            "source_type": "google_sheets",
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
            "original_uri": uri
        }

    elif source_type == "database":
        return {
            "source_type": "database",
            "connection_string": uri,
            "original_uri": uri
        }

    else:
        # File-based sources (csv, excel, json, file)
        return {
            "source_type": source_type,
            "file_path": uri,
            "original_uri": uri
        }


def parse_google_sheets_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a Google Sheets URL to extract spreadsheet ID and sheet identifier.

    Supports various Google Sheets URL formats:
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid={sheet_id}
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}
    - Just the spreadsheet ID itself

    Args:
        url: Google Sheets URL or spreadsheet ID

    Returns:
        Tuple of (spreadsheet_id, sheet_identifier)
        sheet_identifier will be:
        - None if not specified in URL (will use first sheet)
        - "gid:{gid}" if gid is specified in URL (needs to be resolved to sheet name)
        - sheet name if directly specified
    """
    # If it's just a spreadsheet ID (no URL structure)
    if not url.startswith('http') and len(url) > 20 and '/' not in url:
        return url, None

    # Parse the URL
    try:
        parsed = urlparse(url)

        # Extract spreadsheet ID from path
        # Path format: /spreadsheets/d/{spreadsheet_id}/...
        path_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', parsed.path)
        if not path_match:
            return None, None

        spreadsheet_id = path_match.group(1)

        # Try to extract sheet identifier from various URL formats
        sheet_identifier = None

        # Method 1: From fragment (#gid=123456)
        if parsed.fragment:
            if parsed.fragment.startswith('gid='):
                gid = parsed.fragment[4:]
                sheet_identifier = f"gid:{gid}"
            elif 'gid=' in parsed.fragment:
                gid_match = re.search(r'gid=([0-9]+)', parsed.fragment)
                if gid_match:
                    sheet_identifier = f"gid:{gid_match.group(1)}"

        # Method 2: From query parameters (?gid=123456)
        if not sheet_identifier and parsed.query:
            query_params = parse_qs(parsed.query)
            if 'gid' in query_params:
                gid = query_params['gid'][0]
                sheet_identifier = f"gid:{gid}"

        # Method 3: Some URLs might have sheet name directly (less common)
        # This would be custom handling for specific URL formats

        return spreadsheet_id, sheet_identifier

    except Exception:
        # If URL parsing fails, try to extract just the ID
        id_match = re.search(r'([a-zA-Z0-9-_]{44})', url)
        if id_match:
            return id_match.group(1), None
        return None, None


def is_google_sheets_url(url: str) -> bool:
    """
    Check if a URL is a Google Sheets URL.

    Args:
        url: URL to check

    Returns:
        True if it's a Google Sheets URL, False otherwise
    """
    if not url:
        return False

    # Check for Google Sheets domain
    if 'docs.google.com/spreadsheets' in url:
        return True

    # Check if it looks like a spreadsheet ID
    if len(url) > 40 and len(url) < 50 and not '/' in url and not ' ' in url:
        return True

    return False


def format_google_sheets_url(spreadsheet_id: str, sheet_name: Optional[str] = None) -> str:
    """
    Create a properly formatted Google Sheets URL from components.

    Args:
        spreadsheet_id: The spreadsheet ID
        sheet_name: Optional sheet name

    Returns:
        Formatted Google Sheets URL
    """
    base_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    if sheet_name:
        # Note: This creates a URL with sheet name, but Google Sheets uses gid internally
        # This is more for display purposes
        base_url += f"#sheet={sheet_name}"

    return base_url# Helper Functions (kept for use by GoogleSheetDataTable)
# ============================================================================

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
            processed_headers = headers if headers else []
            processed_data = [[value] for value in data.tolist()]
        elif data.ndim == 2:
            # 2D array - multiple columns
            processed_headers = headers if headers else []
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

        # Auto-detect headers if not provided
        if headers is None and len(processed_data) >= 2:
            first_row = processed_data[0]

            # Check if first row contains potential headers
            # Headers are typically short strings while data rows contain longer content
            if all(isinstance(cell, str) for cell in first_row):
                # Check if subsequent rows have longer content
                has_long_content_after = any(
                    any(isinstance(cell, str) and len(cell) > 50 for cell in row)
                    for row in processed_data[1:]
                )

                if has_long_content_after or all(len(str(cell)) < 30 for cell in first_row):
                    # First row looks like headers
                    processed_headers = [str(cell) for cell in first_row]
                    processed_data = processed_data[1:]  # Remove header row from data
                    logger.info(f"Auto-detected headers from first row: {processed_headers}")
                    return processed_data, processed_headers

        # No headers detected or provided
        processed_headers = headers if headers else [f"Column_{i+1}" for i in range(num_cols)]
        return processed_data, processed_headers

    # Handle 1D list (single row or column)
    if isinstance(data, list):
        if headers:
            # If headers provided, treat as single row
            if len(data) == len(headers):
                processed_data = [data]
                processed_headers = headers
            else:
                # Mismatch, use as column
                processed_data = [[value] for value in data]
                processed_headers = headers[:1] if headers else ["Column_1"]
        else:
            # No headers, treat as single column
            processed_data = [[value] for value in data]
            processed_headers = ["Column_1"]
        return processed_data, processed_headers

    # Handle scalar value
    if not isinstance(data, (list, dict, pd.DataFrame, pd.Series, np.ndarray)):
        processed_headers = headers if headers else ["Value"]
        processed_data = [[data]]
        return processed_data, processed_headers

    # Fallback: try to convert to list
    try:
        processed_data = [[data]]
        processed_headers = headers if headers else ["Column_1"]
        return processed_data, processed_headers
    except Exception as e:
        raise ValueError(f"Unsupported data format: {type(data)}. Error: {e}")
