"""
Utility functions for DataTable MCP tools
"""
import re
import os
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs


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

    return base_url