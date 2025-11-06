"""
DataTable Interface - Abstract Base Class

Defines the standard DataTable API that all implementations must follow.
Future implementations (Excel, CSV, Database, etc.) will inherit from this interface.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict
from fastmcp import Context
from datatable_tools.models import TableData


class DataTableInterface(ABC):
    """
    Abstract base class defining standard DataTable API.

    This interface ensures all DataTable implementations provide the same
    core functionality, regardless of the underlying storage mechanism.

    Implementations:
    - GoogleSheetDataTable: Google Sheets backend
    - ExcelDataTable (future): Excel backend
    - CSVDataTable (future): CSV file backend
    """

    @abstractmethod
    async def write_new_sheet(
        self,
        ctx: Context,
        data: TableData,
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new sheet with the provided data.

        Args:
            ctx: FastMCP context
            data: Data in multiple formats:
                  - List[List[Any]]: 2D array (rows x columns)
                  - List[Dict[str, Any]]: List of dicts (DataFrame-like)
                  - List[Any]: 1D array (single row/column)
            sheet_name: Optional name for the sheet

        Returns:
            Dict with success status, URL, and metadata
        """
        pass

    @abstractmethod
    async def append_rows(
        self,
        ctx: Context,
        uri: str,
        data: TableData
    ) -> Dict[str, Any]:
        """
        Append rows to an existing sheet.

        Args:
            ctx: FastMCP context
            uri: URI identifying the target sheet
            data: Data in multiple formats:
                  - List[List[Any]]: 2D array of rows
                  - List[Dict[str, Any]]: List of dicts (DataFrame-like)
                  - List[Any]: 1D array (single row)

        Returns:
            Dict with success status and update info
        """
        pass

    @abstractmethod
    async def append_columns(
        self,
        ctx: Context,
        uri: str,
        data: TableData
    ) -> Dict[str, Any]:
        """
        Append columns to an existing sheet.

        Args:
            ctx: FastMCP context
            uri: URI identifying the target sheet
            data: Data in multiple formats:
                  - List[List[Any]]: 2D array where each inner list is a column
                  - List[Dict[str, Any]]: List of dicts (DataFrame-like)
                  - List[Any]: 1D array (single column)

        Returns:
            Dict with success status and update info
        """
        pass

    @abstractmethod
    async def update_range(
        self,
        ctx: Context,
        uri: str,
        data: TableData,
        range_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a specific range in an existing sheet.

        Args:
            ctx: FastMCP context
            uri: URI identifying the target sheet
            data: Data in multiple formats:
                  - List[List[Any]]: 2D array of data
                  - List[Dict[str, Any]]: List of dicts (DataFrame-like)
                  - List[Any]: 1D array (single row/column)
            range_address: Optional range address (e.g., "A1:C10")
                         If None, auto-expands from A1

        Returns:
            Dict with success status and update info
        """
        pass

    @abstractmethod
    async def load_data_table(
        self,
        ctx: Context,
        uri: str
    ) -> Dict[str, Any]:
        """
        Load data from a sheet.

        Args:
            ctx: FastMCP context
            uri: URI identifying the source sheet

        Returns:
            Dict with success status, headers, data, and metadata
        """
        pass
