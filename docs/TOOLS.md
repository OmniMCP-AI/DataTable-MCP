# DataTable MCP Tools Reference

This document provides a comprehensive reference for all available tools in the DataTable MCP server. Each tool includes input parameters, response format, and usage examples.

## Overview

The DataTable MCP server provides 22 tools organized into 6 categories:
- **Table Lifecycle Management** (4 tools)
- **Data Manipulation** (6 tools)
- **Data Query & Access** (4 tools)
- **Export & Persistence** (2 tools)
- **Advanced Operations** (3 tools)
- **Session Management** (3 tools)

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

Create a new DataTable from data array with auto-detected headers.

**Input Parameters:**
```json
{
  "data": [
    ["John", 25, "Engineer"],
    ["Jane", 30, "Designer"]
  ],
  "headers": ["Name", "Age", "Role"],  // Optional
  "name": "Employee Data"             // Optional, default: "Untitled Table"
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

### 2. load_table

Load a table from external sources (spreadsheet/excel/database).

**Input Parameters:**
```json
{
  "source_type": "csv",                    // Required: "google_sheets", "excel", "csv", "database"
  "source_path": "/path/to/file.csv",     // Required: path/URL to source
  "name": "Imported Data",                // Optional table name
  "sheet_name": "Sheet1",                 // Optional for Excel/Google Sheets
  "encoding": "utf-8",                    // Optional for CSV
  "delimiter": ","                        // Optional for CSV
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_def456",
  "name": "Imported Data",
  "shape": [100, 5],
  "headers": ["Col1", "Col2", "Col3", "Col4", "Col5"],
  "message": "Loaded table from csv with 100 rows and 5 columns"
}
```

### 3. clone_table

Create a deep copy of an existing table.

**Input Parameters:**
```json
{
  "source_table_id": "table_abc123",     // Required
  "new_name": "Copy of Employee Data"    // Optional
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

### 4. list_tables

Get inventory of all tables in the current session.

**Input Parameters:**
```json
{}
```

**Response:**
```json
{
  "success": true,
  "count": 2,
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
  "message": "Found 2 active tables"
}
```

---

## Data Manipulation

### 5. add_row

Add a new row to an existing table.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required
  "row_data": ["Bob", 28, "Manager"],   // Required
  "fill_strategy": "fill_na"            // Optional: "none", "fill_na", "fill_empty", "fill_zero"
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "original_shape": [2, 3],
  "new_shape": [3, 3],
  "fill_strategy": "fill_na",
  "message": "Added row to table table_abc123. Shape changed from [2, 3] to [3, 3]"
}
```

### 6. add_column

Add a new column to an existing table.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",      // Required
  "column_name": "Department",     // Required
  "default_value": "IT",          // Optional
  "position": 2                   // Optional insertion position
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "column_name": "Department",
  "default_value": "IT",
  "original_columns": 3,
  "new_columns": 4,
  "message": "Added column 'Department' to table table_abc123"
}
```

### 7. update_cell

Update values in specified ranges using pandas .loc style updates.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required
  "row_indices": [0, 1],               // Required: list of row indices
  "column_names": ["Age", "Role"],      // Required: list of column names
  "values": [[26, "Senior Engineer"], [31, "Lead Designer"]],  // Required: 2D array
  "fill_strategy": "none"              // Optional: "none", "fill_na", "fill_empty", "fill_zero"
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "rows_updated": 2,
  "columns_updated": 2,
  "fill_strategy": "none",
  "message": "Updated 2 rows and 2 columns in table table_abc123"
}
```

### 8. delete_row

Delete rows from the table by index.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",     // Required
  "row_indices": [1, 2]          // Required: list of row indices to delete
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

### 9. delete_column

Delete columns from the table.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",        // Required
  "columns": ["Department", "Role"]  // Required: list of column names
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

### 10. sort_table

Sort table by one or more columns.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required
  "sort_columns": ["Age", "Name"],      // Required: list of column names to sort by
  "ascending": [true, false],           // Optional: list of boolean values for sort direction
  "in_place": true                      // Optional: if True (default), modifies original table; if False, creates new table
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "sort_columns": ["Age", "Name"],
  "ascending": [true, false],
  "in_place": true,
  "message": "Sorted table table_abc123 by columns: ['Age', 'Name'] (in-place)"
}
```

---

## Data Query & Access

### 11. get_table_data

Get table data with optional pagination and filtering.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",      // Required
  "start_row": 0,                  // Optional: starting row index
  "limit": 10,                     // Optional: max rows to return
  "columns": ["Name", "Age"]       // Optional: specific columns
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

### 12. search_table

Search for rows matching specific criteria.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",        // Required
  "query": "Engineer",              // Required: search term
  "columns": ["Role"],              // Optional: columns to search in
  "case_sensitive": false           // Optional: default false
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "original_rows": 3,
  "filtered_rows": 1,
  "conditions": [{"column": "Role", "operator": "contains", "value": "Engineer"}],
  "data": [
    {"Name": "John", "Age": 26, "Role": "Senior Engineer"}
  ],
  "message": "Search found 1 matching rows in table table_abc123"
}
```

### 13. filter_rows

Filter table rows based on multiple conditions with AND/OR logic.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",       // Required
  "conditions": [                   // Required: list of filter conditions
    {
      "column": "Age",
      "operator": "gte",           // "eq", "ne", "gt", "gte", "lt", "lte", "contains", "startswith", "endswith", "isnull", "notnull"
      "value": 25
    },
    {
      "column": "Role",
      "operator": "contains",
      "value": "Engineer"
    }
  ],
  "logic": "AND",                   // Optional: "AND" or "OR"
  "in_place": true                  // Optional: if True (default), modifies original table; if False, creates new table
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "original_rows": 3,
  "filtered_rows": 1,
  "conditions": [...],
  "logic": "AND",
  "data": [
    {"Name": "John", "Age": 26, "Role": "Senior Engineer"}
  ],
  "headers": ["Name", "Age", "Role"],
  "message": "Filtered table table_abc123: 1 rows match the criteria (in-place)"
}
```

### 14. filter_table

Filter table rows using pandas query syntax.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",       // Required
  "query": "Age > 25 and Role.str.contains('Engineer')",  // Required: pandas query string
  "in_place": true                  // Optional: if True (default), modifies original table; if False, creates new table
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "original_rows": 3,
  "filtered_rows": 1,
  "query": "Age > 25 and Role.str.contains('Engineer')",
  "data": [
    {"Name": "John", "Age": 26, "Role": "Senior Engineer"}
  ],
  "headers": ["Name", "Age", "Role"],
  "message": "Filtered table table_abc123: 1 rows match the query 'Age > 25 and Role.str.contains('Engineer')' (in-place)"
}
```

---

## Export & Persistence

### 15. export_table

Export table data to various formats.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",     // Required
  "format": "csv",               // Required: "csv", "json", "html", "markdown"
  "include_headers": true        // Optional: default true
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "export_format": "csv",
  "return_content": true,
  "rows_exported": 3,
  "columns_exported": 3,
  "table_name": "Employee Data",
  "content": "Name,Age,Role\nJohn,26,Senior Engineer\n...",
  "content_type": "text",
  "message": "Exported table table_abc123 as csv content"
}
```

### 16. save_table

Save table to persistent storage.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",          // Required
  "filename": "employees.csv",         // Required
  "format": "csv"                     // Optional: "csv", "json", default "csv"
}
```

**Response:**
```json
{
  "success": true,
  "table_id": "table_abc123",
  "destination_type": "csv",
  "destination_path": "./employees.csv",
  "rows_saved": 3,
  "columns_saved": 3,
  "if_exists": "replace",
  "message": "Successfully saved table table_abc123 to csv at ./employees.csv"
}
```

---

## Advanced Operations

### 17. calculate_statistics

Calculate basic statistics for numeric columns.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required
  "columns": ["Age"],                  // Optional: specific columns
  "statistics": ["count", "mean", "std", "min", "max"]  // Optional: stats to calculate
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

### 18. group_by

Group table data by one or more columns with aggregation.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",        // Required
  "group_columns": ["Role"],         // Required: columns to group by
  "aggregations": [                  // Required: aggregation functions
    {
      "column": "Age",
      "function": "mean",            // "count", "sum", "mean", "min", "max", "std"
      "alias": "avg_age"             // Optional alias
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
  "group_columns": ["Role"],
  "aggregations": [...],
  "original_rows": 3,
  "grouped_rows": 2,
  "grouped_columns": 2,
  "message": "Grouped table table_abc123 by ['Role']. Result: 2 rows, 2 columns"
}
```

### 19. pivot_table

Create a pivot table from the data.

**Input Parameters:**
```json
{
  "table_id": "table_abc123",           // Required
  "index_columns": ["Role"],            // Required: columns for row index
  "column_to_pivot": "Department",      // Required: column to pivot
  "value_column": "Age",               // Required: values to aggregate
  "aggregation_function": "mean"        // Optional: "sum", "mean", "count", "min", "max"
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

## Session Management

### 20. get_session_info

Get information about the current session.

**Input Parameters:**
```json
{}
```

**Response:**
```json
{
  "success": true,
  "session_stats": {
    "total_tables": 3,
    "total_memory_mb": 12.5,
    "total_rows": 105,
    "total_columns": 12,
    "average_table_age_minutes": 15.2,
    "expired_tables_count": 0,
    "expired_tables": []
  },
  "cleanup_recommendations": {
    "should_cleanup": false,
    "expired_count": 0,
    "memory_savings_mb": 0
  },
  "message": "Session has 3 active tables using 12.5 MB memory"
}
```

### 21. clear_session

Clear all tables and data from the current session.

**Input Parameters:**
```json
{
  "confirm": true    // Required: must be true to prevent accidental clearing
}
```

**Response:**
```json
{
  "success": true,
  "cleanup_type": "all_tables",
  "cleaned_count": 3,
  "force_cleanup": true,
  "message": "Cleared all 3 tables from session"
}
```

### 22. cleanup_expired_tables

Clean up expired tables based on TTL settings.

**Input Parameters:**
```json
{
  "force": false     // Optional: force cleanup even if not expired
}
```

**Response:**
```json
{
  "success": true,
  "cleanup_type": "expired_tables",
  "cleaned_count": 1,
  "force_cleanup": false,
  "message": "Cleaned up 1 expired tables"
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
- **Permission Denied**: `"Access denied for {operation}"`

---

## Usage Examples

### Basic Workflow:
```bash
# 1. Create a table
curl -X POST http://localhost:8321/tools/create_table \
  -d '{"data": [["John", 25], ["Jane", 30]], "headers": ["Name", "Age"]}'

# 2. Add more data
curl -X POST http://localhost:8321/tools/add_row \
  -d '{"table_id": "table_abc123", "row_data": ["Bob", 28]}'

# 3. Query the data
curl -X POST http://localhost:8321/tools/get_table_data \
  -d '{"table_id": "table_abc123"}'

# 4. Export results
curl -X POST http://localhost:8321/tools/export_table \
  -d '{"table_id": "table_abc123", "format": "csv"}'
```

This reference provides complete documentation for all available DataTable MCP tools, their parameters, and expected responses.