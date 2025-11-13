"""
Response Models for DataTable MCP Tools

Pydantic models for structured API responses across all operations.
Also defines shared type aliases for data input formats.
"""

from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel


# ============================================================================
# Type Aliases for Data Input Formats
# ============================================================================
# These types are shared across all DataTable operations to ensure consistency

TableData = Union[
    List[List[Any]],                 # 2D array: list of rows (allows nested structures)
    List[Dict[str, Any]],            # DataFrame-like: list of dicts (allows nested structures)
    List[Any],                       # 1D array: single row or single column
    str                              # Polars DataFrame string representation (from MCP serialization)
]
"""
Unified data input type for all DataTable operations.

Supports four formats:
- List[List[CellValue]]: Traditional 2D array (rows × columns), supports nested structures
- List[Dict[str, CellValue]]: DataFrame-like list of dicts, supports nested structures
- List[CellValue]: 1D array (automatically converted to single row)
- str: Polars DataFrame string representation (when serialized through MCP protocol)

**Nested Structure Support:**
- Cells can contain lists: `image_urls=[]` or `image_urls=[{"url": "..."}]`
- Cells can contain dicts: `metadata={"key": "value"}`
- Nested structures are preserved during data processing
- Google Sheets will store these as JSON strings

Examples:
    >>> # 2D array
    >>> data = [["Alice", 30], ["Bob", 25]]

    >>> # List of dicts
    >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    >>> # 1D array (single row)
    >>> data = ["Alice", 30, "New York"]

    >>> # With nested structures (common in API responses)
    >>> data = [
    ...     {"name": "Alice", "images": [{"url": "http://..."}], "tags": []},
    ...     {"name": "Bob", "images": [], "tags": ["tag1"]}
    ... ]

    >>> # Polars DataFrame string (from MCP serialization)
    >>> data = "shape: (2, 2)\\n┌──────┬─────┐\\n│ name ┆ age │\\n..."
"""


# ============================================================================
# Response Models
# ============================================================================


class TableResponse(BaseModel):
    """Response type for Google Sheets table operations"""
    success: bool
    table_id: Optional[str] = None
    name: Optional[str] = None
    shape: Optional[str] = None
    data: List[Dict[str, Any]] = []
    source_info: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    message: str


class SpreadsheetResponse(BaseModel):
    """Response type for creating new Google Sheets spreadsheet"""
    success: bool
    spreadsheet_url: str
    rows_created: int
    columns_created: int
    shape: str
    error: Optional[str] = None
    message: str


class UpdateResponse(BaseModel):
    """Response type for append/update operations on Google Sheets"""
    success: bool
    spreadsheet_url: str
    spreadsheet_id: str
    worksheet: str
    range: str
    updated_cells: int
    shape: str
    error: Optional[str] = None
    message: str


class WorksheetInfo(BaseModel):
    """Information about a single worksheet"""
    sheet_id: int
    title: str
    index: int
    row_count: int
    column_count: int


class WorksheetsListResponse(BaseModel):
    """Response type for listing worksheets in a Google Sheets spreadsheet"""
    success: bool
    spreadsheet_id: str
    spreadsheet_url: str
    spreadsheet_title: str
    worksheets: List[WorksheetInfo]
    total_worksheets: int
    error: Optional[str] = None
    message: str

