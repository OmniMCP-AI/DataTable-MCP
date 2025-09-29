"""
Utility functions for DataTable MCP tools
"""
import re
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs


def parse_google_sheets_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a Google Sheets URL to extract spreadsheet ID and sheet name.

    Supports various Google Sheets URL formats:
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid={sheet_id}
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit
    - https://docs.google.com/spreadsheets/d/{spreadsheet_id}
    - Just the spreadsheet ID itself

    Args:
        url: Google Sheets URL or spreadsheet ID

    Returns:
        Tuple of (spreadsheet_id, sheet_name)
        sheet_name will be None if not specified in URL
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

        # Try to extract sheet name from various URL formats
        sheet_name = None

        # Method 1: From fragment (#gid=123456)
        if parsed.fragment:
            if parsed.fragment.startswith('gid='):
                gid = parsed.fragment[4:]
                # We have the gid but would need to map it to sheet name
                # For now, we'll leave sheet_name as None and let the API use default
                pass
            elif 'gid=' in parsed.fragment:
                gid_match = re.search(r'gid=([0-9]+)', parsed.fragment)
                if gid_match:
                    # Same as above - we have gid but not sheet name
                    pass

        # Method 2: From query parameters (?gid=123456)
        if parsed.query:
            query_params = parse_qs(parsed.query)
            if 'gid' in query_params:
                # Same issue - we have gid but not sheet name
                pass

        # Method 3: Some URLs might have sheet name directly (less common)
        # This would be custom handling for specific URL formats

        return spreadsheet_id, sheet_name

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