"""Formula Adapter - Convert Google Sheets formulas based on cell position changes

This module provides utilities to adapt Google Sheets formulas when copying cells
from one location to another, respecting absolute ($) and relative cell references.

Key Concepts:
- Relative reference (A1): Both column and row change when copied
- Absolute column ($A1): Column fixed, row changes
- Absolute row (A$1): Column changes, row fixed
- Absolute cell ($A$1): Both fixed, no changes

Examples:
    >>> # Copy from B5 to B6 (row offset +1)
    >>> adapt_formula("=SUM(A5:A10)", row_offset=1, col_offset=0)
    '=SUM(A6:A11)'

    >>> # Copy from B5 to C5 (col offset +1)
    >>> adapt_formula("=SUM($A5:$A10)", row_offset=0, col_offset=1)
    '=SUM($A5:$A10)'  # Absolute column stays fixed

    >>> # Copy from B5 to C6 (row +1, col +1)
    >>> adapt_formula("=SUMIFS($J:$J,$F:$F,$A5,$A:$A,B$1)", row_offset=1, col_offset=1)
    '=SUMIFS($J:$J,$F:$F,$A6,$A:$A,C$1)'
"""

import re
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


def column_letter_to_index(letter: str) -> int:
    """Convert Excel column letter to 0-based index.

    Args:
        letter: Column letter (A, B, ..., Z, AA, AB, ...)

    Returns:
        0-based column index

    Examples:
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


def column_index_to_letter(index: int) -> str:
    """Convert 0-based column index to Excel column letter.

    Args:
        index: 0-based column index

    Returns:
        Column letter (A, B, ..., Z, AA, AB, ...)

    Examples:
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


def adapt_cell_reference(
    cell_ref: str,
    row_offset: int,
    col_offset: int
) -> str:
    """Adapt a single cell reference (e.g., A1, $A1, A$1, $A$1) based on offsets.

    Respects absolute ($) references:
    - $A1: Column fixed, row changes
    - A$1: Column changes, row fixed
    - $A$1: Both fixed
    - A1: Both change

    Args:
        cell_ref: Cell reference string (e.g., "A1", "$A$1", "$A1", "A$1")
        row_offset: Number of rows to offset (positive = down, negative = up)
        col_offset: Number of columns to offset (positive = right, negative = left)

    Returns:
        Adapted cell reference

    Examples:
        >>> adapt_cell_reference("A5", row_offset=1, col_offset=0)
        'A6'
        >>> adapt_cell_reference("$A5", row_offset=1, col_offset=0)
        '$A6'
        >>> adapt_cell_reference("A$5", row_offset=1, col_offset=0)
        'A$5'
        >>> adapt_cell_reference("$A$5", row_offset=1, col_offset=1)
        '$A$5'
        >>> adapt_cell_reference("B1", row_offset=0, col_offset=1)
        'C1'
    """
    # Pattern: optional $ for column, column letters, optional $ for row, row number
    # Examples: A1, $A1, A$1, $A$1, AA10, $AA$10
    match = re.match(r'^(\$?)([A-Z]+)(\$?)(\d+)$', cell_ref, re.IGNORECASE)

    if not match:
        # Not a standard cell reference, return as-is
        return cell_ref

    col_absolute, col_letters, row_absolute, row_num = match.groups()

    # Convert column letter to index
    col_idx = column_letter_to_index(col_letters)
    row_num = int(row_num)

    # Apply offsets based on absolute markers
    new_col_idx = col_idx if col_absolute else col_idx + col_offset
    new_row_num = row_num if row_absolute else row_num + row_offset

    # Ensure indices don't go negative
    new_col_idx = max(0, new_col_idx)
    new_row_num = max(1, new_row_num)  # Rows are 1-indexed

    # Convert back to letter
    new_col_letters = column_index_to_letter(new_col_idx)

    # Reconstruct with absolute markers
    return f"{col_absolute}{new_col_letters}{row_absolute}{new_row_num}"


def adapt_range_reference(
    range_ref: str,
    row_offset: int,
    col_offset: int
) -> str:
    """Adapt a range reference (e.g., A1:B10, $A$1:$B$10) based on offsets.

    Handles:
    - Simple ranges: A1:B10
    - Absolute ranges: $A$1:$B$10
    - Mixed: $A1:B10
    - Column ranges: A:A, $A:$A
    - Row ranges: 1:1, $1:$1
    - Sheet-qualified ranges: 'Sheet1'!A1:B10

    Args:
        range_ref: Range reference (e.g., "A1:B10", "'Sheet1'!$A$1:B10")
        row_offset: Number of rows to offset
        col_offset: Number of columns to offset

    Returns:
        Adapted range reference

    Examples:
        >>> adapt_range_reference("A1:B10", row_offset=1, col_offset=0)
        'A2:B11'
        >>> adapt_range_reference("$A$1:$B$10", row_offset=1, col_offset=1)
        '$A$1:$B$10'
        >>> adapt_range_reference("A:A", row_offset=0, col_offset=1)
        'B:B'
        >>> adapt_range_reference("$A:$A", row_offset=0, col_offset=1)
        '$A:$A'
    """
    # Check for sheet prefix (e.g., 'Sheet1'!A1:B10 or Sheet1!A1:B10)
    sheet_prefix = ""
    if "!" in range_ref:
        sheet_prefix, range_ref = range_ref.rsplit("!", 1)
        sheet_prefix += "!"

    # Handle column-only ranges (e.g., A:A, $A:$A)
    col_only_match = re.match(r'^(\$?)([A-Z]+):(\$?)([A-Z]+)$', range_ref, re.IGNORECASE)
    if col_only_match:
        abs1, col1, abs2, col2 = col_only_match.groups()

        # Apply column offset if not absolute
        col1_idx = column_letter_to_index(col1)
        col2_idx = column_letter_to_index(col2)

        new_col1_idx = col1_idx if abs1 else col1_idx + col_offset
        new_col2_idx = col2_idx if abs2 else col2_idx + col_offset

        new_col1_idx = max(0, new_col1_idx)
        new_col2_idx = max(0, new_col2_idx)

        new_col1 = column_index_to_letter(new_col1_idx)
        new_col2 = column_index_to_letter(new_col2_idx)

        return f"{sheet_prefix}{abs1}{new_col1}:{abs2}{new_col2}"

    # Handle row-only ranges (e.g., 1:1, $1:$1)
    row_only_match = re.match(r'^(\$?)(\d+):(\$?)(\d+)$', range_ref)
    if row_only_match:
        abs1, row1, abs2, row2 = row_only_match.groups()

        row1 = int(row1)
        row2 = int(row2)

        new_row1 = row1 if abs1 else row1 + row_offset
        new_row2 = row2 if abs2 else row2 + row_offset

        new_row1 = max(1, new_row1)
        new_row2 = max(1, new_row2)

        return f"{sheet_prefix}{abs1}{new_row1}:{abs2}{new_row2}"

    # Handle cell ranges (e.g., A1:B10)
    if ":" in range_ref:
        start_cell, end_cell = range_ref.split(":", 1)
        adapted_start = adapt_cell_reference(start_cell, row_offset, col_offset)
        adapted_end = adapt_cell_reference(end_cell, row_offset, col_offset)
        return f"{sheet_prefix}{adapted_start}:{adapted_end}"

    # Single cell reference
    return sheet_prefix + adapt_cell_reference(range_ref, row_offset, col_offset)


def adapt_formula(
    formula: str,
    row_offset: int,
    col_offset: int
) -> str:
    """Adapt a Google Sheets formula based on cell position changes.

    This function parses a formula and adapts all A1-notation cell/range references
    based on the provided row and column offsets. It respects absolute ($) references.

    Non-A1 references (named ranges, structured references) are left unchanged.

    Args:
        formula: Formula string (with or without leading =)
        row_offset: Number of rows to offset (positive = down, negative = up)
        col_offset: Number of columns to offset (positive = right, negative = left)

    Returns:
        Adapted formula string (preserves leading = if present)

    Examples:
        >>> # Basic formula with relative references
        >>> adapt_formula("=SUM(A1:A10)", row_offset=1, col_offset=0)
        '=SUM(A2:A11)'

        >>> # Formula with absolute column references
        >>> adapt_formula("=SUMIFS($J:$J,$F:$F,$A5,$A:$A,B$1)", row_offset=1, col_offset=1)
        '=SUMIFS($J:$J,$F:$F,$A6,$A:$A,C$1)'

        >>> # Cross-sheet reference
        >>> adapt_formula("='Sheet1'!A1+'Sheet2'!B2", row_offset=1, col_offset=0)
        "='Sheet1'!A2+'Sheet2'!B3"

        >>> # Named range (unchanged)
        >>> adapt_formula("=SUM(Revenue)", row_offset=1, col_offset=0)
        '=SUM(Revenue)'

        >>> # Mixed formula
        >>> adapt_formula("=IF(A1>10,SUM($B$1:$B$10),0)", row_offset=2, col_offset=1)
        '=IF(B3>10,SUM($B$1:$B$10),0)'
    """
    if not formula:
        return formula

    # Check if formula starts with =
    has_equals = formula.startswith("=")
    working_formula = formula[1:] if has_equals else formula

    # Strategy: First, find and protect quoted strings, then replace cell references
    # Step 1: Find all quoted strings and replace them with placeholders
    quoted_strings = []

    def save_quoted_string(match):
        """Save quoted string and return placeholder"""
        quoted_strings.append(match.group(0))
        return f"__QUOTED_STRING_{len(quoted_strings) - 1}__"

    # Match double-quoted strings (escaped quotes handled)
    protected_formula = re.sub(r'"(?:[^"\\]|\\.)*"', save_quoted_string, working_formula)

    # Pattern to match cell/range references:
    # - Optional sheet reference: 'Sheet Name'! or SheetName!
    # - Cell/range reference with optional $ markers
    #
    # This pattern matches:
    # - A1, $A1, A$1, $A$1 (single cells)
    # - A1:B10, $A$1:$B$10 (cell ranges)
    # - A:A, $A:$A (column ranges)
    # - 1:1, $1:$1 (row ranges)
    # - 'Sheet1'!A1, 'Sheet1'!A1:B10 (sheet-qualified)
    # - Sheet1!A1 (unquoted sheet names)

    # Pattern explanation:
    # (?:'[^']+'|[A-Za-z0-9_]+)?!?  - Optional sheet reference (quoted or unquoted)
    # \$?[A-Z]+\$?\d+               - Cell reference (e.g., A1, $A$1)
    # |\$?[A-Z]+:\$?[A-Z]+          - Column range (e.g., A:A, $A:$A)
    # |\$?\d+:\$?\d+                - Row range (e.g., 1:1, $1:$1)

    pattern = r"((?:'[^']+'|[A-Za-z0-9_]+)?!?)(\$?[A-Z]+\$?\d+(?::\$?[A-Z]+\$?\d+)?|\$?[A-Z]+:\$?[A-Z]+|\$?\d+:\$?\d+)"

    def replace_reference(match):
        sheet_prefix = match.group(1)  # e.g., 'Sheet1'! or Sheet1! or ''
        reference = match.group(2)      # e.g., A1, A1:B10, A:A, 1:1

        # Adapt the reference
        adapted_ref = adapt_range_reference(reference, row_offset, col_offset)

        return sheet_prefix + adapted_ref

    # Replace all cell/range references in the formula (with quoted strings protected)
    adapted_formula = re.sub(pattern, replace_reference, protected_formula, flags=re.IGNORECASE)

    # Step 3: Restore quoted strings
    def restore_quoted_string(match):
        idx = int(match.group(1))
        return quoted_strings[idx]

    adapted_formula = re.sub(r'__QUOTED_STRING_(\d+)__', restore_quoted_string, adapted_formula)

    # Restore leading = if present
    if has_equals:
        adapted_formula = "=" + adapted_formula

    logger.debug(f"Adapted formula: '{formula}' -> '{adapted_formula}' (row_offset={row_offset}, col_offset={col_offset})")

    return adapted_formula
