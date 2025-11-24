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
    # Handle both /spreadsheets/d/ and /spreadsheets/u/0/d/ patterns
    pattern = r'/spreadsheets(?:/u/\d+)?/d/([a-zA-Z0-9-_]+)'
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


def serialize_cell_value(value: Any) -> Any:
    """
    Serialize cell values for Google Sheets storage.

    Converts nested structures (lists, dicts) to JSON strings.
    Leaves primitive types unchanged.

    Args:
        value: Cell value to serialize

    Returns:
        Serialized value suitable for Google Sheets

    Examples:
        >>> serialize_cell_value([{"url": "http://..."}])
        '[{"url": "http://..."}]'

        >>> serialize_cell_value([])
        '[]'

        >>> serialize_cell_value("string")
        'string'

        >>> serialize_cell_value(42)
        42
    """
    # Convert lists and dicts to JSON strings for Google Sheets storage
    if isinstance(value, (list, dict)):
        import json
        return json.dumps(value, ensure_ascii=False)

    # Return primitive values as-is
    return value


def serialize_row(row: list) -> list:
    """
    Serialize all values in a row for Google Sheets storage.

    Args:
        row: List of cell values

    Returns:
        List of serialized cell values
    """
    return [serialize_cell_value(v) for v in row]


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


def detect_header_row(values: list[list]) -> Tuple[int, list[str], list[list]]:
    """
    Simplified header detection: analyze first 5 rows to find the real header.

    Solves merged cell issues by scanning multiple rows instead of assuming row 0 is the header.

    Simple heuristic:
    - Skip rows with significantly fewer columns than subsequent rows (merged cells)
    - Skip rows with too many empty cells (>50% empty)
    - Pick the first row that looks like a header (short strings, unique values)
    - Fallback to row 0 if nothing detected

    **LIMITATION**: If Google Sheets API returns ALL rows with collapsed columns due to
    heavy merging (e.g., entire row 1 merged), this function cannot detect the correct
    header. In such cases, use `range_address` parameter to skip the merged row manually.

    Args:
        values: 2D array of cell values (first 5+ rows)

    Returns:
        (header_row_index, headers, data_rows)

    Example:
        >>> # Row 0 has merged cells (mostly empty), Row 1 is real header
        >>> data = [["", "", "Merged Title", "", ""],
        ...         ["Name", "Age", "City", "Status", "Score"],
        ...         ["Alice", "30", "NYC", "Active", "95"]]
        >>> idx, headers, data = detect_header_row(data)
        >>> idx
        1
        >>> headers
        ["Name", "Age", "City", "Status", "Score"]
        >>> len(data)
        1
    """
    if not values or len(values) < 1:
        return 0, [], []

    # Analyze first 5 rows (or less if sheet is smaller)
    sample_size = min(5, len(values))

    # Calculate max column count from sample rows (to detect merged cell rows)
    max_cols = max(len(row) for row in values[:sample_size])

    # CRITICAL PATTERN: If row 0 has 1-2 columns but later rows have 10+ columns,
    # this indicates heavily merged row 1 (e.g., "每日库存报表" merged across ALL columns)
    # Google Sheets API collapses it to 1-2 columns instead of full width
    if len(values) > 1 and len(values[0]) <= 2 and max_cols >= 10:
        logger.warning(
            f"Detected heavily merged row 0: {len(values[0])} columns vs {max_cols} max. "
            f"Automatically using row 1 as header. "
            f"Row 0 value: '{values[0][0] if values[0] else 'empty'}'"
        )
        # Skip row 0, analyze from row 1 onwards
        for row_idx in range(1, sample_size):
            row = values[row_idx]
            if not row:
                continue
            # Use first valid row after the merged row as header
            headers = [str(h) if h else "" for h in row]
            data_rows = values[row_idx + 1:]
            logger.info(f"Using row {row_idx} as header (auto-skipped merged row 0)")
            return row_idx, headers, data_rows

    # WARNING: If ALL rows have 1-2 columns, this indicates Google Sheets API
    # has collapsed heavily merged data. Smart detection CANNOT fix this.
    if max_cols <= 2 and len(values) > 10:
        logger.error(
            f"WARNING: All rows have only {max_cols} columns but {len(values)} data rows detected. "
            f"This indicates heavily merged cells that Google Sheets API has collapsed. "
            f"Smart detection cannot fix this. RECOMMENDATION: Use range_address='2:10000' "
            f"to skip the merged title row manually."
        )
        # Fall through to regular detection, which will use row 0 as fallback

    for row_idx in range(sample_size):
        row = values[row_idx]

        if not row:
            continue

        # CRITICAL: Skip rows with significantly fewer columns than later rows
        # This catches merged title rows that Google Sheets API returns as 1-2 columns
        if len(row) < max_cols * 0.5:  # Less than 50% of max columns
            logger.debug(f"Row {row_idx} skipped: too few columns ({len(row)}/{max_cols}) - likely merged cells")
            continue

        # Skip if too many empty cells
        non_empty = [x for x in row if x]
        if len(non_empty) < len(row) * 0.5:  # Less than 50% filled
            logger.debug(f"Row {row_idx} skipped: too many empty cells ({len(non_empty)}/{len(row)})")
            continue

        # Calculate average string length
        avg_len = sum(len(str(x)) for x in non_empty) / len(non_empty)

        # Headers are typically short (<30 chars average)
        if avg_len < 30:
            # Found likely header row
            headers = [str(h) if h else "" for h in row]
            data_rows = values[row_idx + 1:]
            logger.info(f"Detected header row at index {row_idx} (avg length: {avg_len:.1f}, columns: {len(row)})")
            return row_idx, headers, data_rows

    # Fallback: use row 0 (original behavior)
    logger.debug("No clear header detected, falling back to row 0")
    headers = [str(h) if h else "" for h in values[0]]
    data_rows = values[1:] if len(values) > 1 else []
    return 0, headers, data_rows


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


def is_single_column_range(range_address: str) -> bool:
    """
    Check if range_address represents a single column (e.g., "B", "C", "AA", "J5:J8").

    Single column ranges are:
    - Just a column letter: "B", "C", "AA", "ZZ"
    - Column with colon: "B:B", "C:C"
    - Column with row numbers: "J5:J8", "B1:B10", "AA3:AA20"
    - NOT: "A1:C3", "B1:C10" (multiple columns)

    Args:
        range_address: A1 notation range address

    Returns:
        True if it's a single column range, False otherwise

    Examples:
        >>> is_single_column_range("B")
        True
        >>> is_single_column_range("B:B")
        True
        >>> is_single_column_range("AA")
        True
        >>> is_single_column_range("J5:J8")
        True
        >>> is_single_column_range("B1:B10")
        True
        >>> is_single_column_range("B1")
        False
        >>> is_single_column_range("A1:C3")
        False
    """
    if not range_address:
        return False

    # Remove worksheet prefix if present (e.g., "Sheet1!B" -> "B")
    if '!' in range_address:
        range_address = range_address.split('!', 1)[1]

    range_address = range_address.strip()

    # Check for "B:B" format or "J5:J8" format
    if ':' in range_address:
        parts = range_address.split(':')
        if len(parts) == 2:
            # Extract column letters from both parts
            import re
            match1 = re.match(r'^([A-Z]+)(\d*)$', parts[0])
            match2 = re.match(r'^([A-Z]+)(\d*)$', parts[1])

            if match1 and match2:
                col1 = match1.group(1)
                col2 = match2.group(1)
                # Check if both parts have the same column letter
                return col1 == col2
        return False

    # Check for single letter(s) only: "B", "AA", "ZZ"
    return range_address.isalpha()


def parse_polars_dataframe_string(data_str: str) -> Tuple[list[str], list[list]]:
    """
    Parse Polars DataFrame string representation into headers and data rows.

    This handles cases where a Polars DataFrame is serialized to a string through
    the MCP protocol (e.g., when passed from FastestAI workflow).

    **WARNING**: Polars display format truncates long text and wraps multi-line content,
    which can result in data loss. This is a defensive fallback only.
    The proper fix is to use `.to_dicts()` or pass the DataFrame directly with `direct_call=True`.

    Args:
        data_str: String representation of a Polars DataFrame

    Returns:
        (headers, data_rows) tuple

    Raises:
        ValueError: If the DataFrame display is truncated (contains "…") which indicates data loss
    """
    logger.error(
        "Received Polars DataFrame as string representation. "
        "This is problematic because:\n"
        "  1. Display format truncates long text (indicated by '…')\n"
        "  2. Multi-line text in cells gets wrapped\n"
        "  3. This can result in incomplete or incorrect data\n"
        "\nProper solutions:\n"
        "  - Use MCPPlus with DataFrame.to_dicts() conversion (should be automatic)\n"
        "  - Or pass DataFrame directly with direct_call=True in MCPPlus bridge\n"
        "\nThis parser is a defensive fallback only and may not work correctly for all cases."
    )

    # Check if the DataFrame display is truncated (contains "…")
    if "…" in data_str:
        raise ValueError(
            "Polars DataFrame string representation is truncated (contains '…'). "
            "This indicates data loss during string conversion. "
            "Cannot reliably parse truncated DataFrame. "
            "\n\nPlease fix the upstream code to pass DataFrames properly using:\n"
            "  1. MCPPlus with .to_dicts() conversion (automatic)\n"
            "  2. Or direct_call=True for native DataFrame support"
        )

    lines = data_str.strip().split('\n')

    # Find the header row (first row with │ and ┆, but not containing --- or ═)
    header_idx = None
    for i, line in enumerate(lines):
        if '│' in line and '┆' in line:
            if '---' not in line and '═' not in line:
                header_idx = i
                break

    if header_idx is None:
        raise ValueError("Could not find header row in Polars DataFrame string representation")

    # Extract headers
    header_line = lines[header_idx]
    header_line = header_line.strip()
    if header_line.startswith('│'):
        header_line = header_line[1:]
    if header_line.endswith('│'):
        header_line = header_line[:-1]

    headers = [h.strip() for h in header_line.split('┆')]

    # Find data separator (╞═══)
    data_start_idx = None
    for i, line in enumerate(lines):
        if '╞' in line and '═' in line:
            data_start_idx = i + 1
            break

    if data_start_idx is None:
        raise ValueError("Could not find data separator in Polars DataFrame string representation")

    # Extract data rows
    # We need to handle multi-line wrapping by combining wrapped lines into single rows
    data_rows = []
    current_row = None
    num_columns = len(headers)

    for i in range(data_start_idx, len(lines)):
        line = lines[i].strip()

        # Stop at bottom border
        if line.startswith('└'):
            # Add last row if it exists
            if current_row and len(current_row) == num_columns:
                data_rows.append(current_row)
            break

        # Skip empty lines
        if not line or not line.startswith('│'):
            continue

        # Remove box drawing characters
        if line.startswith('│'):
            line = line[1:]
        if line.endswith('│'):
            line = line[:-1]

        # Split by delimiter
        values = [v.strip() for v in line.split('┆')]

        # Check if this starts a new row (has expected number of columns)
        if len(values) == num_columns:
            # Save previous row if it exists
            if current_row and len(current_row) == num_columns:
                data_rows.append(current_row)

            # Start new row with type conversion
            current_row = []
            for v in values:
                if v == '' or v == 'null':
                    current_row.append(None)
                elif v.replace('.', '', 1).replace('-', '', 1).isdigit():
                    try:
                        if '.' in v:
                            current_row.append(float(v))
                        else:
                            current_row.append(int(v))
                    except ValueError:
                        current_row.append(v)
                else:
                    current_row.append(v)
        else:
            # This is a continuation of a multi-line cell
            # We should append to the current row's cells
            # But Polars wrapping is complex, so we'll just warn
            logger.warning(f"Detected multi-line wrapping in Polars display. Data may be incomplete.")

    # Add the last row if it exists
    if current_row and len(current_row) == num_columns:
        data_rows.append(current_row)

    logger.warning(f"Parsed Polars DataFrame string: {len(headers)} columns, {len(data_rows)} rows. Data may be incomplete due to display wrapping.")
    return headers, data_rows


def process_data_input(data: Union[list, str, Any]) -> Tuple[Optional[list[str]], list[list]]:
    """
    Process data input supporting multiple formats including Polars DataFrames.

    Supports:
    - List[List[Any]]: 2D array (traditional format)
    - List[Dict[str, Any]]: List of dicts (pandas DataFrame-like)
    - List[Any]: 1D array (single row or column) - NEW
    - pl.DataFrame: Polars DataFrame (NEW in Stage 5)
    - str: Polars DataFrame string representation (when serialized through MCP)

    Args:
        data: One of the supported data formats:
              - List[List[Any]]: 2D array
              - List[Dict[str, Any]]: List of dicts
              - List[Any]: 1D array (single row or column)
              - pl.DataFrame: Polars DataFrame (if polars is installed)
              - str: Polars DataFrame string representation

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
    # NEW: Handle Polars DataFrame string representation (from MCP serialization)
    if isinstance(data, str):
        # Check if it looks like a Polars DataFrame string
        if data.startswith('shape:') and '┌' in data and '│' in data:
            return parse_polars_dataframe_string(data)
        else:
            raise ValueError(
                f"String data input is not supported unless it's a Polars DataFrame representation. "
                f"Received: {data[:100]}..."
            )

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


async def parse_range_address(
    service,
    spreadsheet_id: str,
    range_address: Optional[str],
    sheet_title: str,
    sheet_id: int
) -> Tuple[str, str, int]:
    """
    Parse range_address to handle worksheet!range format.

    This function handles cases where range_address includes a worksheet name
    (e.g., "Sheet1!A1:D10") by parsing it and validating the worksheet exists.

    Args:
        service: Authenticated Google Sheets API service object
        spreadsheet_id: The spreadsheet ID
        range_address: Optional range in A1 notation, may include worksheet name
                      (e.g., "A2:M1000", "Sheet1!A1:D10", "'My Sheet'!B:Z")
        sheet_title: Default sheet title from URI (used as fallback)
        sheet_id: Default sheet ID from URI (used as fallback)

    Returns:
        Tuple of (range_name, final_sheet_title, final_sheet_id):
            - range_name: Full range notation for API call (e.g., "'Sheet1'!A1:D10")
            - final_sheet_title: The sheet title to use (either parsed or default)
            - final_sheet_id: The sheet ID to use (either parsed or default)

    Examples:
        >>> # Range with sheet name
        >>> parse_range_address(service, "ABC123", "Sheet1!A:D", "DefaultSheet", 0)
        ("'Sheet1'!A:D", "Sheet1", 123456)

        >>> # Range without sheet name
        >>> parse_range_address(service, "ABC123", "A:D", "DefaultSheet", 0)
        ("'DefaultSheet'!A:D", "DefaultSheet", 0)
    """
    if not range_address:
        # No range specified, use default full sheet
        return f"'{sheet_title}'!A:ZZ", sheet_title, sheet_id

    # Parse worksheet name from range_address if present (e.g., "Sheet1!A1:J6")
    final_range = range_address
    final_sheet_title = sheet_title
    final_sheet_id = sheet_id

    if '!' in range_address:
        worksheet_from_range, final_range = range_address.split('!', 1)
        worksheet_from_range = worksheet_from_range.strip("'\"")
        logger.info(f"Parsed worksheet '{worksheet_from_range}' from range_address")

        # Validate if worksheet exists and use it if found
        try:
            metadata = await asyncio.to_thread(
                service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute
            )
            sheets = metadata.get('sheets', [])
            found = False
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == worksheet_from_range:
                    final_sheet_title = worksheet_from_range
                    final_sheet_id = sheet.get('properties', {}).get('sheetId')
                    found = True
                    break

            if not found:
                logger.warning(
                    f"Worksheet '{worksheet_from_range}' from range_address not found. "
                    f"Falling back to worksheet from URI: '{sheet_title}'."
                )
        except Exception as e:
            logger.warning(
                f"Error validating worksheet '{worksheet_from_range}': {e}. "
                f"Falling back to worksheet from URI: '{sheet_title}'."
            )

    # Construct full range notation
    range_name = f"'{final_sheet_title}'!{final_range}"
    logger.info(f"Using range: {range_name}")

    return range_name, final_sheet_title, final_sheet_id
