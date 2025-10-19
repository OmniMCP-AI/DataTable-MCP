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

PrimitiveValue = int | str | float | bool | None
"""Primitive types that can be stored in a cell"""

TableData = Union[
    List[List[PrimitiveValue]],      # 2D array: list of rows
    List[Dict[str, PrimitiveValue]], # DataFrame-like: list of dicts
    List[PrimitiveValue]             # 1D array: single row or single column
]
"""
Unified data input type for all DataTable operations.

Supports three formats:
- List[List[PrimitiveValue]]: Traditional 2D array (rows × columns)
- List[Dict[str, PrimitiveValue]]: DataFrame-like list of dicts
- List[PrimitiveValue]: 1D array (automatically converted to single row)

Examples:
    >>> # 2D array
    >>> data = [["Alice", 30], ["Bob", 25]]

    >>> # List of dicts
    >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    >>> # 1D array (single row)
    >>> data = ["Alice", 30, "New York"]
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

