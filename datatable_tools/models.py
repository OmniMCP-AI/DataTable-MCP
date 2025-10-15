"""
Response Models for DataTable MCP Tools

Pydantic models for structured API responses across all operations.
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel


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

