"""
MCP Tools - Lifecycle Operations

Thin wrapper layer for data loading operations.
Delegates to GoogleSheetDataTable implementation.
"""

from typing import Dict, List, Optional, Any
from typing_extensions import TypedDict
import logging
from pydantic import Field
from fastmcp import Context
from core.server import mcp
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

logger = logging.getLogger(__name__)


class TableResponse(TypedDict):
    """Response type for Google Sheets table operations"""
    success: bool
    table_id: Optional[str]
    name: Optional[str]
    shape: Optional[tuple[int, int]]
    headers: Optional[List[str]]
    data: Optional[List[List[Any]]]
    source_info: Optional[Dict[str, Any]]
    error: Optional[str]
    message: str


class SpreadsheetResponse(TypedDict):
    """Response type for creating new Google Sheets spreadsheet"""
    success: bool
    spreadsheet_url: str
    rows_created: int
    columns_created: int
    data_shape: tuple[int, int]
    error: Optional[str]
    message: str


@mcp.tool
async def load_data_table(
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    )
) -> TableResponse:
    """
    Load a table from Google Sheets using URI-based auto-detection

    Args:
        uri: Google Sheets URI. Supports:
             - Google Sheets: https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}

    Returns:
        Dict containing table_id and loaded Google Sheets table information

    Examples:
        # Google Sheets URL
        uri = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit?gid=0#gid=0"
    """
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.load_data_table(ctx, uri)

