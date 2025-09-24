from typing import Dict, List, Optional, Any, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import logging
import threading
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class TableMetadata:
    """Metadata for a DataTable"""
    name: str
    created_at: str
    source_info: Dict[str, Any]
    last_modified: str
    ttl_minutes: int = 60  # Default TTL of 60 minutes

class DataTable:
    """Core DataTable class representing an in-memory table"""

    def __init__(self, table_id: str, df: pd.DataFrame, metadata: TableMetadata):
        self.table_id = table_id
        self.df = df
        self.metadata = metadata
        self._lock = threading.RLock()

    @property
    def headers(self) -> List[str]:
        return self.df.columns.tolist()

    @property
    def data(self) -> List[List[Any]]:
        return self.df.values.tolist()

    @property
    def shape(self) -> List[int]:
        return list(self.df.shape)

    @property
    def dtypes(self) -> Dict[str, str]:
        return {col: str(dtype) for col, dtype in self.df.dtypes.items()}

    def to_dict(self) -> Dict[str, Any]:
        """Convert table to dictionary format for API responses"""
        return {
            "table_id": self.table_id,
            "headers": self.headers,
            "data": self.data,
            "shape": self.shape,
            "dtypes": self.dtypes,
            "metadata": asdict(self.metadata)
        }

    def get_slice(self, rows: Optional[slice] = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Get a slice of the dataframe"""
        with self._lock:
            df = self.df
            if columns:
                df = df[columns]
            if rows:
                df = df.iloc[rows]
            return df

    def set_values(self, row_indices: List[int], column_names: List[str], values: List[List[Any]]):
        """Set values at specified locations"""
        with self._lock:
            for i, row_idx in enumerate(row_indices):
                for j, col_name in enumerate(column_names):
                    if i < len(values) and j < len(values[i]):
                        self.df.at[row_idx, col_name] = values[i][j]
            self._update_modified_time()

    def append_row(self, row_data: List[Any], fill_strategy: str = "none"):
        """Append a row to the table with dimension mismatch handling"""
        with self._lock:
            current_cols = len(self.df.columns)
            row_length = len(row_data)

            if row_length < current_cols:
                if fill_strategy == "fill_na":
                    row_data.extend([np.nan] * (current_cols - row_length))
                elif fill_strategy == "fill_empty":
                    row_data.extend([""] * (current_cols - row_length))
                elif fill_strategy == "fill_zero":
                    row_data.extend([0] * (current_cols - row_length))
            elif row_length > current_cols:
                # Add new columns if row has more data
                for i in range(current_cols, row_length):
                    self.df[f"Column_{i+1}"] = np.nan

            new_row = pd.DataFrame([row_data[:len(self.df.columns)]], columns=self.df.columns)
            self.df = pd.concat([self.df, new_row], ignore_index=True)
            self._update_modified_time()

    def add_column(self, column_name: str, default_value: Any = None):
        """Add a new column with optional default value"""
        with self._lock:
            if column_name not in self.df.columns:
                self.df[column_name] = default_value
                self._update_modified_time()

    def delete_rows(self, row_indices: List[int]):
        """Delete specified rows"""
        with self._lock:
            self.df = self.df.drop(index=row_indices).reset_index(drop=True)
            self._update_modified_time()

    def delete_columns(self, column_names: List[str]):
        """Delete specified columns"""
        with self._lock:
            self.df = self.df.drop(columns=column_names)
            self._update_modified_time()

    def filter_rows(self, conditions: List[Dict[str, Any]], logic: str = "AND") -> pd.DataFrame:
        """Filter rows based on conditions"""
        with self._lock:
            mask = pd.Series([True] * len(self.df))

            for condition in conditions:
                column = condition.get("column")
                operator = condition.get("operator")
                value = condition.get("value")

                if column not in self.df.columns:
                    continue

                col_mask = self._apply_condition(self.df[column], operator, value)

                if logic.upper() == "AND":
                    mask = mask & col_mask
                else:  # OR
                    mask = mask | col_mask

            return self.df[mask]

    def filter_by_query(self, query: str) -> pd.DataFrame:
        """
        Filter rows using pandas query syntax (simplified DataFrame filtering).

        Examples:
        - "Age > 25"
        - "Name == 'John'"
        - "Age > 20 and Role == 'Engineer'"
        - "Age >= 25 or Department == 'IT'"
        """
        with self._lock:
            try:
                return self.df.query(query)
            except Exception as e:
                logger.error(f"Error applying query '{query}': {e}")
                # Fallback to return empty dataframe with same structure
                return self.df.iloc[0:0]

    def sort_table(self, sort_columns: List[str], ascending: List[bool] = None) -> pd.DataFrame:
        """Sort table by specified columns"""
        with self._lock:
            if ascending is None:
                ascending = [True] * len(sort_columns)
            return self.df.sort_values(by=sort_columns, ascending=ascending)

    def _apply_condition(self, series: pd.Series, operator: str, value: Any) -> pd.Series:
        """Apply a filter condition to a pandas Series"""
        try:
            if operator == "eq":
                return series == value
            elif operator == "ne":
                return series != value
            elif operator == "gt":
                return series > value
            elif operator == "gte":
                return series >= value
            elif operator == "lt":
                return series < value
            elif operator == "lte":
                return series <= value
            elif operator == "contains":
                return series.astype(str).str.contains(str(value), na=False)
            elif operator == "startswith":
                return series.astype(str).str.startswith(str(value), na=False)
            elif operator == "endswith":
                return series.astype(str).str.endswith(str(value), na=False)
            elif operator == "isnull":
                return series.isnull()
            elif operator == "notnull":
                return series.notnull()
            else:
                return pd.Series([False] * len(series))
        except Exception:
            return pd.Series([False] * len(series))

    def _update_modified_time(self):
        """Update the last modified timestamp"""
        self.metadata.last_modified = datetime.now().isoformat()

class TableManager:
    """Manages all DataTables in memory with session-based cleanup"""

    def __init__(self):
        self.tables: Dict[str, DataTable] = {}
        self._lock = threading.RLock()

    def create_table(self, data: List[List[Any]], headers: Optional[List[str]] = None,
                    name: str = "Untitled", source_info: Dict[str, Any] = None) -> str:
        """Create a new table and return its ID"""
        with self._lock:
            table_id = f"dt_{uuid.uuid4().hex[:8]}"

            # Auto-detect headers if not provided
            if headers is None:
                if data:
                    headers = [f"Column_{i+1}" for i in range(len(data[0]))]
                else:
                    headers = []

            # Create DataFrame
            df = pd.DataFrame(data, columns=headers) if data else pd.DataFrame(columns=headers)

            # Create metadata
            metadata = TableMetadata(
                name=name,
                created_at=datetime.now().isoformat(),
                source_info=source_info or {},
                last_modified=datetime.now().isoformat()
            )

            # Create and store table
            table = DataTable(table_id, df, metadata)
            self.tables[table_id] = table

            logger.info(f"Created table {table_id} with shape {df.shape}")
            return table_id

    def get_table(self, table_id: str) -> Optional[DataTable]:
        """Get a table by ID"""
        with self._lock:
            return self.tables.get(table_id)

    def list_tables(self) -> List[Dict[str, Any]]:
        """List all tables with basic info"""
        with self._lock:
            return [
                {
                    "table_id": table_id,
                    "name": table.metadata.name,
                    "shape": table.shape,
                    "created_at": table.metadata.created_at,
                    "last_modified": table.metadata.last_modified
                }
                for table_id, table in self.tables.items()
            ]

    def clone_table(self, source_table_id: str, new_name: str = None) -> Optional[str]:
        """Clone an existing table"""
        with self._lock:
            source_table = self.tables.get(source_table_id)
            if not source_table:
                return None

            new_table_id = f"dt_{uuid.uuid4().hex[:8]}"
            new_df = source_table.df.copy()

            new_metadata = TableMetadata(
                name=new_name or f"{source_table.metadata.name} (Copy)",
                created_at=datetime.now().isoformat(),
                source_info=source_table.metadata.source_info.copy(),
                last_modified=datetime.now().isoformat()
            )

            new_table = DataTable(new_table_id, new_df, new_metadata)
            self.tables[new_table_id] = new_table

            logger.info(f"Cloned table {source_table_id} to {new_table_id}")
            return new_table_id

    def delete_table(self, table_id: str) -> bool:
        """Delete a table"""
        with self._lock:
            if table_id in self.tables:
                del self.tables[table_id]
                logger.info(f"Deleted table {table_id}")
                return True
            return False

    def cleanup_expired_tables(self, force: bool = False):
        """Clean up expired tables based on TTL"""
        with self._lock:
            current_time = datetime.now()
            expired_tables = []

            for table_id, table in self.tables.items():
                created_time = datetime.fromisoformat(table.metadata.created_at)
                ttl_delta = timedelta(minutes=table.metadata.ttl_minutes)

                if force or (current_time - created_time) > ttl_delta:
                    expired_tables.append(table_id)

            for table_id in expired_tables:
                del self.tables[table_id]
                logger.info(f"Cleaned up expired table {table_id}")

            return len(expired_tables)

# Global table manager instance
table_manager = TableManager()

def cleanup_expired_tables():
    """Cleanup function for expired tables"""
    return table_manager.cleanup_expired_tables()