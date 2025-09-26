from typing import List, Optional, Union, Any, Dict
from pydantic import BaseModel, Field


class WorkSheetInfo(BaseModel):
    """Worksheet information model"""
    name: Optional[str] = None
    id: Optional[int] = None


class ReadSheetRequest(BaseModel):
    """Request model for reading spreadsheet data"""
    spreadsheet_id: str = Field(
        ...,
        description="Google Spreadsheet ID from the URL"
    )
    worksheet: Union[str, int, WorkSheetInfo] = Field(
        ...,
        description="Worksheet name, id, or WorkSheetInfo object"
    )


class ReadSheetResponse(BaseModel):
    """Response model for reading spreadsheet data"""
    success: bool
    message: str
    spreadsheet_id: str
    worksheet: WorkSheetInfo
    used_range: str = Field(
        ...,
        description="The detected range containing all data (e.g., 'A1:C10')"
    )
    values: List[List[str]] = Field(
        ...,
        description="2D array of cell values from the used range"
    )
    headers: Optional[List[str]] = Field(
        None,
        description="First row values treated as column headers if detected"
    )
    row_count: int
    column_count: int
    worksheet_url: str


class WriteSheetRequest(BaseModel):
    """Request model for writing spreadsheet data"""
    spreadsheet_id: Optional[str] = Field(
        None,
        description="Google Spreadsheet ID from the URL. If None or empty, a new spreadsheet will be created."
    )
    spreadsheet_name: Optional[str] = Field(
        None,
        description="Name for new spreadsheet. Used only when spreadsheet_id is None/empty."
    )
    worksheet: Optional[Union[str, int, WorkSheetInfo]] = Field(
        None,
        description="Worksheet name, id, or WorkSheetInfo object"
    )
    columns_name: Optional[List[str]] = Field(
        None,
        description="Column headers to match against"
    )
    values: List[List[str]] = Field(
        ...,
        description="2D array of values to write"
    )
    start_row: Optional[int] = Field(
        None,
        description="Row number to start writing (1-based)"
    )


class WriteSheetResponse(BaseModel):
    """Response model for writing spreadsheet data"""
    success: bool
    message: str
    spreadsheet_id: str
    worksheet: WorkSheetInfo
    updated_range: str = Field(
        ...,
        description="The actual range that was updated"
    )
    updated_cells: int
    matched_columns: Optional[List[str]] = Field(
        None,
        description="Column headers that were matched and used"
    )
    worksheet_url: str


class LoadDataTableRequest(BaseModel):
    """Request model for loading data into DataTable from spreadsheet"""
    user_id: str = Field(..., description="User ID for authentication")
    spreadsheet_id: str = Field(..., description="Google Spreadsheet ID")
    worksheet: Union[str, int, WorkSheetInfo] = Field(..., description="Worksheet to read from")
    name: Optional[str] = Field(None, description="Optional name for the created table")


class UpdateRangeRequest(BaseModel):
    """Request model for updating specific range in spreadsheet"""
    spreadsheet_id: str = Field(..., description="Google Spreadsheet ID")
    worksheet: Union[str, int, WorkSheetInfo] = Field(..., description="Worksheet to update")
    range: str = Field(..., description="Cell range in A1 notation (e.g., 'A1:C10', 'B5')")
    values: List[List[str]] = Field(..., description="2D array of values to write")
    create_new_on_permission_error: Optional[bool] = Field(
        default=True,
        description="Create new spreadsheet if permission denied"
    )


class UpdateRangeResponse(BaseModel):
    """Response model for range update operation"""
    success: bool
    message: str
    spreadsheet_id: str
    worksheet: WorkSheetInfo
    updated_range: str = Field(..., description="The actual range that was updated")
    updated_cells: int
    worksheet_url: str


class ExportDataTableRequest(BaseModel):
    """Request model for exporting DataTable to spreadsheet"""
    user_id: str = Field(..., description="User ID for authentication")
    table_id: str = Field(..., description="DataTable ID to export")
    spreadsheet_id: Optional[str] = Field(None, description="Target spreadsheet ID (creates new if None)")
    spreadsheet_name: Optional[str] = Field(None, description="Name for new spreadsheet")
    worksheet: Optional[Union[str, int, WorkSheetInfo]] = Field(None, description="Target worksheet")
    columns_name: Optional[List[str]] = Field(None, description="Column headers to match")