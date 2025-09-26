from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
import logging
import pandas as pd
from datatable_tools.spreadsheet_client import spreadsheet_client
from datatable_tools.spreadsheet_models import ReadSheetRequest, WorkSheetInfo

logger = logging.getLogger(__name__)


class DataSource(ABC):
    """Abstract base class for data sources"""

    @abstractmethod
    async def load_data(self) -> Tuple[List[List[Any]], List[str], Dict[str, Any]]:
        """
        Load data from the source

        Returns:
            Tuple of (data, headers, source_info)
        """
        pass


class SpreadsheetDataSource(DataSource):
    """Data source for Google Spreadsheets via SPREADSHEET_API"""

    def __init__(self, user_id: str, spreadsheet_id: str, worksheet: str = None):
        self.user_id = user_id
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = worksheet or "Sheet1"

    async def load_data(self) -> Tuple[List[List[Any]], List[str], Dict[str, Any]]:
        """Load data from Google Spreadsheet"""
        try:
            # Create request
            request = ReadSheetRequest(
                spreadsheet_id=self.spreadsheet_id,
                worksheet=self.worksheet
            )

            # Call spreadsheet API
            response = await spreadsheet_client.read_sheet(request, self.user_id)

            if not response.success:
                raise Exception(f"Failed to read spreadsheet: {response.message}")

            # Process data
            data = []
            headers = []

            if response.values:
                if response.headers:
                    # Use detected headers
                    headers = response.headers
                    # Skip header row in data
                    data = response.values[1:] if len(response.values) > 1 else []
                else:
                    # No headers detected, use first row as headers
                    if response.values:
                        headers = response.values[0]
                        data = response.values[1:] if len(response.values) > 1 else []
                    else:
                        headers = []
                        data = []

            # Ensure consistent column count
            if headers and data:
                max_cols = len(headers)
                for row in data:
                    while len(row) < max_cols:
                        row.append("")
                    if len(row) > max_cols:
                        row = row[:max_cols]

            source_info = {
                "type": "google_sheets",
                "spreadsheet_id": self.spreadsheet_id,
                "worksheet": response.worksheet.name,
                "used_range": response.used_range,
                "worksheet_url": response.worksheet_url,
                "row_count": response.row_count,
                "column_count": response.column_count
            }

            logger.info(f"Loaded {len(data)} rows, {len(headers)} columns from spreadsheet {self.spreadsheet_id}")
            return data, headers, source_info

        except Exception as e:
            logger.error(f"Error loading from spreadsheet {self.spreadsheet_id}: {e}")
            raise


class ExcelDataSource(DataSource):
    """Data source for Excel files"""

    def __init__(self, file_path: str, sheet_name: str = None):
        self.file_path = file_path
        self.sheet_name = sheet_name

    async def load_data(self) -> Tuple[List[List[Any]], List[str], Dict[str, Any]]:
        """Load data from Excel file"""
        try:
            # Load Excel file
            if self.sheet_name:
                df = pd.read_excel(self.file_path, sheet_name=self.sheet_name)
            else:
                df = pd.read_excel(self.file_path)

            # Convert to lists
            headers = df.columns.tolist()
            data = df.values.tolist()

            # Convert all values to strings for consistency
            data = [[str(cell) if cell is not None else "" for cell in row] for row in data]

            source_info = {
                "type": "excel",
                "file_path": self.file_path,
                "sheet_name": self.sheet_name,
                "row_count": len(data),
                "column_count": len(headers)
            }

            logger.info(f"Loaded {len(data)} rows, {len(headers)} columns from Excel file {self.file_path}")
            return data, headers, source_info

        except Exception as e:
            logger.error(f"Error loading from Excel file {self.file_path}: {e}")
            raise


class CSVDataSource(DataSource):
    """Data source for CSV files"""

    def __init__(self, file_path: str, encoding: str = None, delimiter: str = None):
        self.file_path = file_path
        self.encoding = encoding or "utf-8"
        self.delimiter = delimiter or ","

    async def load_data(self) -> Tuple[List[List[Any]], List[str], Dict[str, Any]]:
        """Load data from CSV file"""
        try:
            # Load CSV file
            df = pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter
            )

            # Convert to lists
            headers = df.columns.tolist()
            data = df.values.tolist()

            # Convert all values to strings for consistency
            data = [[str(cell) if cell is not None else "" for cell in row] for row in data]

            source_info = {
                "type": "csv",
                "file_path": self.file_path,
                "encoding": self.encoding,
                "delimiter": self.delimiter,
                "row_count": len(data),
                "column_count": len(headers)
            }

            logger.info(f"Loaded {len(data)} rows, {len(headers)} columns from CSV file {self.file_path}")
            return data, headers, source_info

        except Exception as e:
            logger.error(f"Error loading from CSV file {self.file_path}: {e}")
            raise


class DatabaseDataSource(DataSource):
    """Data source for database connections"""

    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query

    async def load_data(self) -> Tuple[List[List[Any]], List[str], Dict[str, Any]]:
        """Load data from database"""
        try:
            # Use pandas to execute query
            df = pd.read_sql(self.query, self.connection_string)

            # Convert to lists
            headers = df.columns.tolist()
            data = df.values.tolist()

            # Convert all values to strings for consistency
            data = [[str(cell) if cell is not None else "" for cell in row] for row in data]

            source_info = {
                "type": "database",
                "connection_string": self.connection_string,
                "query": self.query,
                "row_count": len(data),
                "column_count": len(headers)
            }

            logger.info(f"Loaded {len(data)} rows, {len(headers)} columns from database")
            return data, headers, source_info

        except Exception as e:
            logger.error(f"Error loading from database: {e}")
            raise


def create_data_source(source_type: str, **kwargs) -> DataSource:
    """Factory function to create appropriate data source"""

    if source_type == "google_sheets":
        return SpreadsheetDataSource(
            user_id=kwargs.get("user_id"),
            spreadsheet_id=kwargs.get("spreadsheet_id"),
            worksheet=kwargs.get("worksheet")
        )
    elif source_type == "excel":
        return ExcelDataSource(
            file_path=kwargs.get("file_path"),
            sheet_name=kwargs.get("sheet_name")
        )
    elif source_type == "csv":
        return CSVDataSource(
            file_path=kwargs.get("file_path"),
            encoding=kwargs.get("encoding"),
            delimiter=kwargs.get("delimiter")
        )
    elif source_type == "database":
        return DatabaseDataSource(
            connection_string=kwargs.get("connection_string"),
            query=kwargs.get("query")
        )
    else:
        raise ValueError(f"Unsupported source type: {source_type}")