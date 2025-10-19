"""Google Sheets utility functions - framework-agnostic

These helper functions have NO FastMCP dependency and can be used in any Python project.
They provide common functionality for working with Google Sheets API.

These are NEW utilities for the Stage 4 refactoring. The old utilities in utils.py
will be deprecated after migration is complete.
"""
import re
import asyncio
from typing import Tuple, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

# Type checking for optional Polars import
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None


def parse_google_sheets_uri(uri: str) -> Tuple[str, Optional[str]]:
    """
    Parse Google Sheets URI to extract spreadsheet_id and gid

    Args:
        uri: Google Sheets URL

    Returns:
        (spreadsheet_id, gid)

    Raises:
        ValueError: If URI is not a valid Google Sheets URL

    Example:
        >>> parse_google_sheets_uri("https://docs.google.com/spreadsheets/d/ABC123/edit?gid=456#gid=456")
        ("ABC123", "456")
    """
    pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    match = re.search(pattern, uri)
    if not match:
        raise ValueError(f"Invalid Google Sheets URI: {uri}")

    spreadsheet_id = match.group(1)

    # Extract gid from URL (check both # and ? parameter formats)
    gid_match = re.search(r'[#&?]gid=(\d+)', uri)
    gid = gid_match.group(1) if gid_match else None

    return spreadsheet_id, gid


async def get_sheet_by_gid(service, spreadsheet_id: str, gid: Optional[str]) -> dict:
    """
    Get sheet properties by gid or return first sheet if gid not provided

    Args:
        service: Google Sheets API service object
        spreadsheet_id: Spreadsheet ID
        gid: Sheet gid (optional)

    Returns:
        Sheet properties dict with keys: sheetId, title, index, gridProperties, etc.

    Raises:
        ValueError: If no sheets found or gid not found

    Example:
        properties = await get_sheet_by_gid(service, "ABC123", "456")
        print(properties['title'])  # "Sheet1"
    """
    metadata = await asyncio.to_thread(
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute
    )

    sheets = metadata.get('sheets', [])
    if not sheets:
        raise ValueError(f"No sheets found in spreadsheet {spreadsheet_id}")

    # If gid provided, find matching sheet
    if gid:
        for sheet in sheets:
            properties = sheet.get('properties', {})
            if properties.get('sheetId') == int(gid):
                return properties
        # If gid not found, fall back to first sheet
        logger.warning(f"Sheet gid {gid} not found, falling back to first sheet")
        return sheets[0]['properties']

    # No gid provided, return first sheet
    return sheets[0]['properties']


def auto_detect_headers(values: list[list]) -> Tuple[list[str], list[list]]:
    """
    Auto-detect if first row contains headers

    Uses heuristic: First row has short strings AND second row has longer content

    Args:
        values: 2D array of cell values

    Returns:
        (headers, data_rows) - headers is empty list if not detected

    Example:
        >>> data = [["Name", "Age"], ["Alice Johnson with a long description", "30"]]
        >>> headers, rows = auto_detect_headers(data)
        >>> headers
        ["Name", "Age"]
        >>> rows
        [["Alice Johnson with a long description", "30"]]
    """
    if not values or len(values) < 2:
        return [], values

    first_row = values[0]
    second_row = values[1]

    # Filter out empty cells for average calculation
    first_row_non_empty = [x for x in first_row if x]
    second_row_non_empty = [x for x in second_row if x]

    if not first_row_non_empty or not second_row_non_empty:
        return [], values

    # Calculate average string length per row
    first_row_avg = sum(len(str(x)) for x in first_row_non_empty) / len(first_row_non_empty)
    second_row_avg = sum(len(str(x)) for x in second_row_non_empty) / len(second_row_non_empty)

    # Heuristic: If first row is short (avg < 30 chars) and second row is long (avg > 50 chars),
    # assume first row is headers
    if first_row_avg < 30 and second_row_avg > 50:
        logger.debug(f"Auto-detected headers: {first_row}")
        return [str(x) for x in first_row], values[1:]

    # Headers not detected
    return [], values


def calculate_range_notation(
    sheet_title: str,
    start_row: int = 1,
    start_col: str = "A",
    end_row: Optional[int] = None,
    end_col: Optional[str] = None
) -> str:
    """
    Calculate A1 notation range with proper sheet name escaping

    Args:
        sheet_title: Sheet name
        start_row: Starting row (1-indexed)
        start_col: Starting column letter
        end_row: Ending row (optional, omit for open-ended range)
        end_col: Ending column letter (optional)

    Returns:
        Range notation like "Sheet1!A1:D10" or "'My Sheet'!A1:D10"

    Example:
        >>> calculate_range_notation("Sheet1", 1, "A", 10, "D")
        "Sheet1!A1:D10"
        >>> calculate_range_notation("My Sheet", 1, "A", 10, "D")
        "'My Sheet'!A1:D10"
    """
    if end_row and end_col:
        range_str = f"{start_col}{start_row}:{end_col}{end_row}"
    elif end_col:
        range_str = f"{start_col}{start_row}:{end_col}"
    else:
        range_str = f"{start_col}{start_row}"

    # Escape sheet title if it contains spaces or special chars
    if ' ' in sheet_title or '!' in sheet_title or "'" in sheet_title:
        # Escape single quotes by doubling them
        escaped_title = sheet_title.replace("'", "''")
        return f"'{escaped_title}'!{range_str}"

    return f"{sheet_title}!{range_str}"


def column_index_to_letter(index: int) -> str:
    """
    Convert column index (0-based) to Excel column letter

    Args:
        index: 0-based column index

    Returns:
        Column letter (A, B, ..., Z, AA, AB, ...)

    Example:
        >>> column_index_to_letter(0)
        'A'
        >>> column_index_to_letter(25)
        'Z'
        >>> column_index_to_letter(26)
        'AA'
        >>> column_index_to_letter(701)
        'ZZ'
    """
    result = ""
    while index >= 0:
        result = chr(index % 26 + ord('A')) + result
        index = index // 26 - 1
    return result


def column_letter_to_index(letter: str) -> int:
    """
    Convert Excel column letter to index (0-based)

    Args:
        letter: Column letter (A, B, ..., Z, AA, AB, ...)

    Returns:
        0-based column index

    Example:
        >>> column_letter_to_index('A')
        0
        >>> column_letter_to_index('Z')
        25
        >>> column_letter_to_index('AA')
        26
        >>> column_letter_to_index('ZZ')
        701
    """
    letter = letter.upper()
    index = 0
    for i, char in enumerate(reversed(letter)):
        index += (ord(char) - ord('A') + 1) * (26 ** i)
    return index - 1


def process_data_input(data: Union[list, Any]) -> Tuple[Optional[list[str]], list[list]]:
    """
    Process data input supporting multiple formats including Polars DataFrames.

    Supports:
    - List[List[Any]]: 2D array (traditional format)
    - List[Dict[str, Any]]: List of dicts (pandas DataFrame-like)
    - List[Any]: 1D array (single row or column) - NEW
    - pl.DataFrame: Polars DataFrame (NEW in Stage 5)

    Args:
        data: One of the supported data formats:
              - List[List[Any]]: 2D array
              - List[Dict[str, Any]]: List of dicts
              - List[Any]: 1D array (single row or column)
              - pl.DataFrame: Polars DataFrame (if polars is installed)

    Returns:
        (headers, data_rows) tuple:
            - headers: List of column names (None if 2D array/1D array without headers)
            - data_rows: 2D array of data values

    Raises:
        ValueError: If Polars DataFrame is passed but polars is not installed

    Examples:
        >>> # Polars DataFrame (NEW)
        >>> import polars as pl
        >>> df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        >>> headers, rows = process_data_input(df)
        >>> headers
        ['name', 'age']
        >>> rows
        [['Alice', 30], ['Bob', 25]]

        >>> # List of dicts (DataFrame-like)
        >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        >>> headers, rows = process_data_input(data)
        >>> headers
        ['name', 'age']
        >>> rows
        [['Alice', 30], ['Bob', 25]]

        >>> # 2D array
        >>> data = [["Alice", 30], ["Bob", 25]]
        >>> headers, rows = process_data_input(data)
        >>> headers
        None
        >>> rows
        [['Alice', 30], ['Bob', 25]]

        >>> # 1D array (NEW) - single row
        >>> data = ["Alice", 30, "New York"]
        >>> headers, rows = process_data_input(data)
        >>> headers
        None
        >>> rows
        [['Alice', 30, 'New York']]
    """
    # NEW: Handle Polars DataFrame
    if POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
        # Extract headers from column names
        headers = data.columns  # Returns List[str]

        # Convert DataFrame to 2D array using .rows()
        data_rows = data.rows()  # Returns List[Tuple], convert to List[List]
        data_rows = [list(row) for row in data_rows]

        logger.debug(f"Converted Polars DataFrame to 2D array. Headers: {headers}, Rows: {len(data_rows)}")
        return headers, data_rows

    # Check if polars DataFrame was passed but polars not installed
    if not POLARS_AVAILABLE and hasattr(data, 'columns') and hasattr(data, 'rows'):
        raise ValueError(
            "Polars DataFrame detected but polars library is not installed. "
            "Install polars with: pip install polars"
        )

    if not data:
        return None, []

    # Check if data is list of dicts
    if isinstance(data[0], dict):
        # Extract headers from first dict keys
        headers = list(data[0].keys())

        # Convert list of dicts to 2D array
        data_rows = []
        for row_dict in data:
            # Ensure all rows use the same column order
            row = [row_dict.get(key, None) for key in headers]
            data_rows.append(row)

        logger.debug(f"Converted list of dicts to 2D array. Headers: {headers}, Rows: {len(data_rows)}")
        return headers, data_rows

    # NEW: Check if data is 1D array (list of primitives, not list of lists)
    # Detect by checking if first element is NOT a list or dict
    if not isinstance(data[0], (list, dict)):
        # This is a 1D array - wrap it in a list to make it 2D with single row
        data_rows = [data]
        logger.debug(f"Converted 1D array to 2D array (single row). Length: {len(data)}")
        return None, data_rows

    # Already a 2D array (list of lists)
    return None, data
