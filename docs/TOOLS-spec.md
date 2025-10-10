# DataTable Tools Reference v2

This document provides an improved, LLM-friendly reference for all available tools in the DataTable server. This version follows best practices for tool naming, parameter design, and schema clarity.
This tools spec will be use to build a in-memory table as a standard, and Google Sheet MCP tool, Excel MCP tool will use it as guileline for development.

## Design Principles

1. **Explicit Function Names**: Action-oriented verb + object pattern (e.g., `create_table`, `add_row`)
2. **No Parameter Overloading**: Separate functions instead of optional parameters where it improves clarity
3. **Descriptive Types**: Avoid `Any` types; use specific types like `List`, `Dict[str, Any]`, etc.
4. **Minimal Optionals**: Provide separate functions rather than many optional parameters
5. **LLM-Friendly**: Clear intent matching, easy parameter inference, intuitive descriptions

## Overview

The DataTable MCP server provides **16 tools** organized into **5 categories**:
- **Table Lifecycle Management** (4 tools)
- **Data Manipulation** (7 tools)
- **Data Query & Access** (2 tools)
- **Export & Persistence** (1 tool)
- **Advanced Operations** (3 tools)

## Common Response Format

All tools return responses in this standardized format:

```json
{
  "success": true/false,
  "error": "error message if success=false",
  "message": "human-readable status message",
  // ... tool-specific data
}
```

---

## Table Lifecycle Management

### 1. create_table

**Signature:** `create_table(values: list[list], headers: list[str] | None = None, name: str = "Untitled Table") -> dict`

Create a new DataTable from a 2D data array with optional headers.

**Purpose**: Initialize a new in-memory table from structured data for further manipulation and analysis.

**Input Parameters:**
```json
{
  "values": [
    ["John", 25, "Engineer"],
    ["Jane", 30, "Designer"]
  ],                                    // Required: 2D array of table data
  "headers": ["Name", "Age", "Role"],  // Optional: Column headers (auto-generated if not provided)
  "name": "Employee Data"              // Optional: Table display name (default: "Untitled Table")
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "name": "Employee Data",
  "shape": [2, 3],
  "headers": ["Name", "Age", "Role"],
  "message": "Created table 'Employee Data' with 2 rows and 3 columns"
}
```

---

### 2. load_table_by_uri

**Signature:** `load_table_by_uri(source_uri: str, sheet_name: str | None = None, name: str | None = None) -> dict`

Load a table from external sources using a URI (file path, URL, or connection string).

**Purpose**: Import existing data from various sources (local files, remote URLs, Google Sheets, databases) into a DataTable for manipulation. File type is auto-detected from URI extension.

**Input Parameters:**
```json
{
  "source_uri": "/path/to/file.csv",       // Required: File path, URL, or connection string
  "sheet_name": "Sheet1",                  // Optional: For Excel/Google Sheets only
  "name": "Imported Data"                  // Optional: Table display name
}
```

**Example Usage:**
```json
// Load CSV (auto-detected)
{"source_uri": "/path/to/data.csv"}

// Load Excel with specific sheet
{"source_uri": "/path/to/workbook.xlsx", "sheet_name": "Sales"}

// Load Google Sheets
{"source_uri": "https://docs.google.com/spreadsheets/d/...", "sheet_name": "Sheet1"}

// Load JSON
{"source_uri": "/path/to/data.json"}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_def456",
  "name": "Imported Data",
  "shape": [100, 5],
  "headers": ["Col1", "Col2", "Col3", "Col4", "Col5"],
  "values": [
    ["John", 25, "Engineer", "IT", 75000],
    ["Jane", 30, "Designer", "Marketing", 80000],
    ...
  ],
  "source_type": "csv",
  "message": "Loaded table from csv with 100 rows and 5 columns"
}
```

---

### 3. clone_table

**Signature:** `clone_table(source_table_id: str, new_name: str | None = None) -> dict`

Create an independent deep copy of an existing table.

**Purpose**: Duplicate a table for testing modifications without affecting the original data.

**Input Parameters:**
```json
{
  "source_table_id": "table_abc123",     // Required: ID of table to clone
  "new_name": "Copy of Employee Data"    // Optional: Name for cloned table
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_ghi789",
  "source_table_id": "table_abc123",
  "name": "Copy of Employee Data",
  "shape": [2, 3],
  "message": "Cloned table table_abc123 to table_ghi789"
}
```

---

### 4. list_tables_by_filter

**Signature:** `list_tables_by_filter(filter: dict = {}, limit: int = 10) -> dict`

List tables in the current session with optional filtering and pagination.

**Purpose**: Query and browse tables with filters (e.g., by name pattern, row count, date range) and limit results for better control.

**Input Parameters:**
```json
{
  "filter": {},                    // Optional: Filter criteria (default: {} matches all)
  "limit": 10                      // Optional: Maximum tables to return (default: 10)
}
```

**Example Filters:**
```json
// No filter - list all tables (up to limit)
{"filter": {}, "limit": 10}

// Filter by name pattern
{"filter": {"name": "Employee"}, "limit": 5}

```

**Response:**
```json
{
  "success": true,
  "count": 2,
  "total_matched": 5,
  "limit": 10,
  "tables": [
    {
      "table_id": "table_abc123",
      "name": "Employee Data",
      "shape": [2, 3],
      "created_at": "2024-01-01T12:00:00",
      "last_modified": "2024-01-01T12:05:00"
    },
    {
      "table_id": "table_def456",
      "name": "Imported Data",
      "shape": [100, 5],
      "created_at": "2024-01-01T12:10:00",
      "last_modified": "2024-01-01T12:10:00"
    }
  ],
  "message": "Found 2 tables (showing 2 of 5 total matches)"
}
```

---

## Data Manipulation

### 5. insert_row_at_index

**Signature:** `insert_row_at_index(table_id: str, row_index: int, values: list) -> dict`

Insert a new row at a specific position in the table.

**Purpose**: Add new data records at a precise location in the table. Use index -1 to append to the end.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required: Target table ID
  "row_index": 1,                       // Required: Position to insert (0-based, -1 for append)
  "values": ["Bob", 28, "Manager"]      // Required: List of values matching table columns
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "row_index": 1,
  "original_shape": [2, 3],
  "new_shape": [3, 3],
  "message": "Inserted row at index 1 in table table_abc123. Shape changed from [2, 3] to [3, 3]"
}
```

---

### 6. insert_column_at_index

**Signature:** `insert_column_at_index(table_id: str, column_index: int, column_name: str, values: list) -> dict`

Insert a new column at a specific position with a default value for all rows.

**Purpose**: Extend table schema by adding a new field at a precise location. Use index -1 to append to the end.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",      // Required: Target table ID
  "column_index": 2,               // Required: Position to insert (0-based, -1 for append)
  "column_name": "Department",     // Required: Name of new column
  "values": ["IT", "HR", "IT"]     // Required: List of values, one per row
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "column_index": 2,
  "column_name": "Department",
  "original_shape": [3, 3],
  "new_shape": [3, 4],
  "message": "Inserted column 'Department' at index 2 in table table_abc123. Shape changed from [3, 3] to [3, 4]"
}
```

---

### 7. update_range

**Signature:** `update_range(table_id: str, range_address: str, values: list[list]) -> dict`

Update cell values in a rectangular range using spreadsheet-style address notation.

**Purpose**: Modify existing data in a specific range (Excel/Google Sheets style with A1:B3 notation).

**Input Parameters:**
```json
{
  "table_id": "table_abc123",                    // Required: Target table ID
  "range_address": "B1:C2",                      // Required: Range in A1 notation (e.g., "A1:B3", "C2:E5")
  "values": [[26, "Senior Engineer"],
             [31, "Lead Designer"]]              // Required: 2D array of new values (rows × columns)
}
```

**Example Usage:**
```json
// Update single cell
{"range_address": "A1", "values": [[42]]}

// Update row range
{"range_address": "A1:C1", "values": [["John", 25, "Engineer"]]}

// Update column range
{"range_address": "B1:B3", "values": [[25], [30], [28]]}

// Update rectangular block
{"range_address": "A1:C2", "values": [["John", 25, "Engineer"], ["Jane", 30, "Designer"]]}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "range_address": "B1:C2",
  "rows_updated": 2,
  "columns_updated": 2,
  "message": "Updated range B1:C2 (2 rows × 2 columns) in table table_abc123"
}
```

---

### 8. clear_range

**Signature:** `clear_range(table_id: str, range_address: str) -> dict`

Clear (set to None/null) all cell values in a specified range.

**Purpose**: Remove data from cells while preserving table structure (spreadsheet-style clear operation).

**Input Parameters:**
```json
{
  "table_id": "table_abc123",       // Required: Target table ID
  "range_address": "B1:C2"          // Required: Range in A1 notation (e.g., "A1:B3", "C2:E5")
}
```

**Example Usage:**
```json
// Clear single cell
{"range_address": "A1"}

// Clear row range
{"range_address": "A1:C1"}

// Clear column range
{"range_address": "B1:B3"}

// Clear rectangular block
{"range_address": "A1:C2"}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "range_address": "B1:C2",
  "rows_cleared": 2,
  "columns_cleared": 2,
  "message": "Cleared range B1:C2 (2 rows × 2 columns) in table table_abc123"
}
```

---

### 9. delete_rows_by_index

**Signature:** `delete_rows_by_index(table_id: str, row_indices: list[int]) -> dict`

Delete specific rows from the table by their index positions.

**Purpose**: Remove unwanted data records from the table.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",     // Required: Target table ID
  "row_indices": [1, 2]           // Required: List of row indices to delete (0-based)
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "target_type": "rows",
  "deleted_count": 2,
  "original_shape": [3, 4],
  "new_shape": [1, 4],
  "message": "Deleted 2 rows from table table_abc123"
}
```

---

### 10. delete_columns_by_name

**Signature:** `delete_columns_by_name(table_id: str, column_names: list[str]) -> dict`

Delete specific columns from the table by their names.

**Purpose**: Remove unwanted fields from the table schema.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",        // Required: Target table ID
  "column_names": ["Department", "Role"]  // Required: List of column names to delete
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "target_type": "columns",
  "deleted_count": 2,
  "original_shape": [1, 4],
  "new_shape": [1, 2],
  "message": "Deleted 2 columns from table table_abc123"
}
```

---

### 11. sort_table_by_columns

**Signature:** `sort_table_by_columns(table_id: str, sort_by: list[str], ascending: list[bool] | None = None) -> dict`

Sort table rows by one or more columns in ascending or descending order.

**Purpose**: Reorder table rows for better readability or analysis.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required: Target table ID
  "sort_by": ["Age", "Name"],           // Required: List of column names to sort by (priority order)
  "ascending": [true, false]            // Optional: Sort direction per column (default: all true)
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "sort_by": ["Age", "Name"],
  "ascending": [true, false],
  "message": "Sorted table table_abc123 by columns: ['Age', 'Name']"
}
```

---

## Data Query & Access

### 12. get_data

**Signature:** `get_data(table_id: str, start_row: int = 0, limit: int | None = None, columns: list[str] | None = None) -> dict`

Retrieve table data with optional row range and column filtering.

**Purpose**: View or extract specific portions of table data for display or analysis.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",      // Required: Target table ID
  "start_row": 0,                  // Optional: Starting row index (default: 0)
  "limit": 10,                     // Optional: Maximum rows to return (default: all)
  "columns": ["Name", "Age"]       // Optional: Specific columns to include (default: all)
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "data": [
    {"Name": "John", "Age": 25},
    {"Name": "Jane", "Age": 30}
  ],
  "headers": ["Name", "Age"],
  "shape": [2, 2],
  "output_format": "records",
  "slice_info": {
    "start_row": 0,
    "end_row": 10,
    "columns": ["Name", "Age"]
  },
  "message": "Retrieved 2 rows and 2 columns from table table_abc123"
}
```

---

### 13. query_table

**Signature:** `query_table(table_id: str, query_expr: str | None = None, columns: list[str] | None = None, limit: int | None = None) -> dict`

Query and filter table data using pandas query expressions with optional pagination.

**Purpose**: Retrieve filtered data using powerful pandas query syntax. Supports complex conditions, text search, and column selection.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",       // Required: Target table ID
  "query_expr": "Age > 25 and Role.str.contains('Engineer')",  // Optional: Pandas query string (omit to get all rows)
  "columns": ["Name", "Age"],       // Optional: Specific columns to return (default: all)
  "limit": 10                       // Optional: Maximum rows to return (default: all)
}
```

**Example Queries:**
```json
// Simple comparison
{"query_expr": "Age > 25"}

// Text search (case-insensitive)
{"query_expr": "Role.str.contains('Engineer', case=False)"}

// Multiple conditions with AND
{"query_expr": "Age > 25 and Role.str.contains('Engineer')"}

// Multiple conditions with OR
{"query_expr": "Age < 25 or Age > 60"}

// Text starts with
{"query_expr": "Name.str.startswith('J')"}

// Null checks
{"query_expr": "Role.notna()"}

// No filter - just select columns and limit
{"columns": ["Name", "Age"], "limit": 10}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "original_rows": 10,
  "filtered_rows": 2,
  "query_expr": "Age > 25 and Role.str.contains('Engineer')",
  "data": [
    {"Name": "John", "Age": 26, "Role": "Senior Engineer"},
    {"Name": "Alice", "Age": 28, "Role": "Engineer"}
  ],
  "headers": ["Name", "Age", "Role"],
  "message": "Query returned 2 rows from table table_abc123"
}
```

---

## Export & Persistence

### 14. export_table

**Signature:** `export_table(table_id: str, format: str, file_path: str | None = None, include_headers: bool = True) -> dict`

Export table data to various formats, optionally saving to a file.

**Purpose**: Convert table to different formats (CSV, JSON, HTML, Markdown) for display, sharing, or persistence.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",          // Required: Target table ID
  "format": "csv",                     // Required: "csv" | "json" | "html" | "markdown"
  "file_path": "/path/to/file.csv",   // Optional: If provided, saves to file; if omitted, returns content
  "include_headers": true              // Optional: Include column headers (default: true)
}
```

**Example Usage:**
```json
// Export as string content (for display)
{
  "table_id": "table_abc123",
  "format": "csv"
}

// Export and save to file
{
  "table_id": "table_abc123",
  "format": "json",
  "file_path": "/path/to/data.json"
}
```

**Response (content mode - no file_path):**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "export_format": "csv",
  "rows_exported": 3,
  "columns_exported": 3,
  "table_name": "Employee Data",
  "content": "Name,Age,Role\nJohn,26,Senior Engineer\n...",
  "content_type": "text",
  "message": "Exported table table_abc123 as csv content"
}
```

**Response (file mode - with file_path):**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "export_format": "csv",
  "file_path": "/path/to/employees.csv",
  "rows_exported": 3,
  "columns_exported": 3,
  "message": "Exported table table_abc123 to csv at /path/to/employees.csv"
}
```

---

## Advanced Operations

### 15. describe_table

**Signature:** `describe_table(table_id: str, columns: list[str] | None = None, statistics: list[str] | None = None) -> dict`

Calculate descriptive statistics for numeric columns.

**Purpose**: Get statistical summary (count, mean, std, min, max, etc.) for data analysis. Matches pandas `df.describe()` behavior.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required: Target table ID
  "columns": ["Age"],                   // Optional: Specific columns (default: all numeric)
  "statistics": ["count", "mean", "std", "min", "max"]  // Optional: Stats to compute (default: all)
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "statistics": {
    "Age": {
      "count": 3,
      "mean": 27.67,
      "std": 2.52,
      "min": 25,
      "max": 30
    }
  },
  "columns_analyzed": ["Age"],
  "message": "Calculated statistics for 1 columns in table table_abc123"
}
```

---

### 16. group_table

**Signature:** `group_table(table_id: str, group_by_columns: list[str], aggregations: list[dict]) -> dict`

Group table rows by columns and apply aggregation functions.

**Purpose**: Perform GROUP BY operations with aggregations (sum, count, mean, etc.). Matches pandas `df.groupby()` behavior.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",        // Required: Target table ID
  "group_by_columns": ["Role"],      // Required: Columns to group by
  "aggregations": [                  // Required: Aggregation operations
    {
      "column": "Age",
      "function": "mean",            // "count" | "sum" | "mean" | "min" | "max" | "std"
      "output_name": "avg_age"       // Optional: Name for aggregated column
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "new_table_id": "table_jkl012",
  "new_table_name": "Employee Data (Grouped)",
  "source_table_id": "table_abc123",
  "group_by_columns": ["Role"],
  "aggregations": [...],
  "original_rows": 3,
  "grouped_rows": 2,
  "grouped_columns": 2,
  "message": "Grouped table table_abc123 by ['Role']. Result: 2 rows, 2 columns"
}
```

---

### 17. create_pivot_table

**Signature:** `create_pivot_table(table_id: str, index_columns: list[str], pivot_column: str, value_column: str, aggregation_function: str = "mean") -> dict`

Transform table into a pivot table with rows, columns, and aggregated values.

**Purpose**: Reshape data by pivoting columns into rows with aggregations (Excel-style pivot).

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required: Target table ID
  "index_columns": ["Role"],            // Required: Columns for pivot table rows
  "pivot_column": "Department",         // Required: Column whose values become new columns
  "value_column": "Age",                // Required: Column to aggregate
  "aggregation_function": "mean"        // Optional: "sum" | "mean" | "count" | "min" | "max" (default: "mean")
}
```

**Response:**
```json
{
  "success": true,
  "new_table_id": "table_mno345",
  "new_table_name": "Employee Data (Pivoted)",
  "source_table_id": "table_abc123",
  "pivot_columns": ["IT", "HR", "Finance"],
  "index_columns": ["Role"],
  "aggregation_function": "mean",
  "original_rows": 10,
  "pivoted_rows": 3,
  "pivoted_columns": 4,
  "message": "Created pivot table from table_abc123. Result: 3 rows, 4 columns"
}
```

---

## Error Handling

All tools follow consistent error handling patterns:

### Common Error Responses:
```json
{
  "success": false,
  "error": "Table table_xyz not found",
  "message": "Target table does not exist"
}
```

### Common Error Types:
- **Table Not Found**: `"Table {table_id} not found"`
- **Invalid Parameters**: `"Invalid {parameter}: {details}"`
- **Column Not Found**: `"Column '{column}' not found in table"`
- **Operation Failed**: `"Failed to {operation} {resource}"`
- **Type Mismatch**: `"Expected {expected_type}, got {actual_type}"`

---

## Key Improvements in v2

### 1. Clearer Function Names
- `list_tables` → `list_tables_by_filter`
- `load_table` → `load_table_by_uri`
- `add_row` → `insert_row_at_index`
- `add_column` → `insert_column_at_index`
- `update_cell` → `update_range` (pairs with `clear_range`)
- `delete_row` → `delete_rows_by_index`
- `delete_column` → `delete_columns_by_name`
- `sort_table` → `sort_table_by_columns`
- `search_table` / `filter_rows` / `filter_table` → `query_table` (unified querying)
- `export_table` / `save_table` → `export_table` (unified export with optional file save)
- `get_table_data` → `get_data`
- `calculate_statistics` → `describe_table`
- `group_by` → `group_table`
- `pivot_table` → `create_pivot_table`

### 2. Removed Optional Parameters (Following Pandas `insert()` Pattern)
- Changed `position` (optional) to `row_index` and `column_index` (required) - matches pandas `df.insert(loc, ...)`
- Use index `-1` to append (explicit intent)
- Eliminated `in_place` parameter (operations now always modify the original table)
- Removed `fill_strategy` from most operations (simplified default behavior)
- Replaced generic `columns` with specific names like `column_names`, `group_by_columns`, `sort_columns`

### 3. More Specific Types
- Changed `column: Union[str, int]` to `range_address: str` using A1 notation (e.g., "A1:B3")
- Unified all data parameters to use `values` (replaces `data`, `row_data`, `default_value`, etc.)
- Changed `query` to `query_expr` or `search_term` based on context

### 4. Better Parameter Names
- `source_uri` instead of generic `path` or `source_path`
- `values` for all data input (replaces `data`, `row_data`, etc.)
- `sort_by` instead of `sort_columns` (matches SQL/pandas convention)
- `row_indices` instead of ambiguous `rows`
- `column_names` instead of `columns` in most cases
- `group_by_columns` instead of `group_columns`
- `pivot_column` instead of `column_to_pivot`
- `output_name` instead of `alias`
- `file_path` instead of `filename`
- `range_address` using A1 notation (e.g., "A1:B3")

### 5. Enhanced Descriptions
- Every tool now has a **Purpose** section explaining when and why to use it
- More explicit parameter documentation
- Clearer return value descriptions
