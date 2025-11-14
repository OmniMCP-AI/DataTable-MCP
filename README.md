# DataTable MCP Server

A comprehensive Model Context Protocol (MCP) server for in-memory tabular data manipulation with external source integration. Built following the requirements specification for optimized LLM-driven data operations.

## Features

### 🗃️ Core Capabilities
- **In-Memory Processing**: All table operations performed in memory for speed
- **Session-Based Management**: Tables exist with unique IDs during session lifecycle
- **External I/O**: Load from and save to Google Sheets, Excel, databases
- **Pandas-like Interface**: Familiar DataFrame-style operations
- **Column Name Focused**: Use semantic column names to reduce LLM errors

### 🛠️ Google Sheets Tools (10 Total)

#### 📥 Data Loading (3 tools)
- `load_data_table` - Load table data from Google Sheets (returns calculated values)
- `read_worksheet_with_formulas` - Read worksheet data with raw formulas (returns formula strings like "=SUM(A1:A10)")
- `preview_worksheet_formulas` - Preview first N rows with formulas (quick preview, default 5 rows)

#### 📤 Data Writing (3 tools)
- `write_new_sheet` - Create new Google Sheets spreadsheet with data
- `write_new_worksheet` - Create new worksheet (tab) in existing spreadsheet
- `append_rows` - Append rows to existing sheet

#### ✏️ Data Modification (3 tools)
- `append_columns` - Append columns to existing sheet
- `update_range` - Update specific cell range (A1 notation)
- `update_range_by_lookup` - Update rows by lookup key (SQL-like UPDATE...JOIN)

#### 🗂️ Metadata (1 tool)
- `list_worksheets` - List all worksheets (tabs) in a spreadsheet with metadata

---

### 🛠️ Legacy In-Memory Tools (22 Total)

#### 📊 Table Lifecycle Management (4 tools)
- `create_table` - Create from data array with auto-detected headers
- `load_table` - Unified loading from spreadsheet/excel/database
- `clone_table` - Deep copy existing table
- `list_tables` - Session table inventory

#### ✏️ Data Manipulation (6 tools)
- `append_row` - Robust handling of dimension mismatches
- `add_column` - Add with optional default values
- `set_range_values` - Pandas .loc style updates with fill strategies
- `delete_from_table` - Unified row/column deletion
- `rename_columns` - Bulk column renaming
- `clear_range` - Clear values while preserving structure

#### 🔍 Data Query & Access (4 tools)
- `get_table_data` - Flexible slicing, multiple output formats
- `filter_rows` - Multi-condition filtering with AND/OR logic
- `filter_table` - Pandas query syntax filtering
- `sort_table` - Multi-column sorting, in-place or new table

#### 💾 Export & Persistence (2 tools)
- `save_table` - Unified save to spreadsheet/excel/database
- `export_table` - Multiple formats: CSV, JSON, Excel, Parquet

#### 🔧 Advanced Operations (3 tools)
- `merge_tables` - Pandas-style joins (inner/left/right/outer)
- `aggregate_data` - Group by with multiple aggregation functions
- `map_values` - Value transformation and mapping

#### 🧹 Session Management (3 tools)
- `cleanup_tables` - Clean expired tables or specific tables
- `get_table_info` - Detailed table information and statistics
- `get_session_stats` - Session-wide statistics and cleanup recommendations

## Installation

```bash
# Clone the repository
cd DataTable-MCP

# Install dependencies
pip install -e .

# Or install from requirements
pip install -r requirements.txt
```

## Usage

### Starting the Server

#### stdio mode (default)
```bash
python main.py
```

#### HTTP mode
```bash
python main.py --transport streamable-http --port 8321
```

### Core Data Structure

Tables are represented with the following structure:

```json
{
  "table_id": "dt_abc123",
  "headers": ["column1", "column2"],
  "data": [["value1", "value2"], ["value3", "value4"]],
  "shape": [2, 2],
  "dtypes": {"column1": "string", "column2": "number"},
  "metadata": {
    "name": "table_name",
    "created_at": "2024-01-01T12:00:00",
    "source_info": {}
  }
}
```

### Example Usage

#### Creating a Table
```python
# Create a simple table
result = await create_table(
    data=[["Alice", 25], ["Bob", 30]],
    headers=["Name", "Age"],
    name="People"
)
# Returns: {"success": True, "table_id": "dt_abc123", ...}
```

#### Loading from External Source
```python
# Load from CSV
result = await load_table(
    source_type="csv",
    source_path="/path/to/data.csv",
    name="Sales Data"
)
```

#### Filtering Data
```python
# Filter rows where Age > 25
result = await filter_rows(
    table_id="dt_abc123",
    conditions=[{"column": "Age", "operator": "gt", "value": 25}],
    logic="AND"
)
```

#### Advanced Operations
```python
# Merge two tables
result = await merge_tables(
    left_table_id="dt_abc123",
    right_table_id="dt_def456",
    join_type="inner",
    on="ID"
)

# Aggregate data
result = await aggregate_data(
    table_id="dt_abc123",
    group_by=["Department"],
    aggregations={"Salary": ["mean", "sum"], "Count": "count"}
)
```

## Architecture

### Memory Management
- Session-based table storage with TTL (Time To Live)
- Automatic cleanup of expired tables
- Thread-safe operations with locks
- Memory usage monitoring and statistics

### Error Handling
- Graceful degradation for dimension mismatches
- Comprehensive validation and type checking
- Detailed error messages for LLM context
- Transaction-like rollback for complex operations

### Performance
- Optimized for tables up to 100K rows
- < 2s response time for most operations
- Concurrent session support
- Efficient pandas-based operations

## Configuration


### Table TTL
Default TTL is 60 minutes. Tables are automatically cleaned up when expired.

## External Integrations

### Supported Formats
- **CSV**: Read/write CSV files
- **Excel**: Read/write .xlsx/.xls files
- **JSON**: Import/export JSON data
- **Parquet**: High-performance columnar format

### Planned Integrations
- **Google Sheets**: Read/write via Google Sheets API
- **Databases**: Generic SQL connection support
- **Cloud Storage**: S3, GCS, Azure Blob integration

## API Reference

All tools return a consistent response format:

```json
{
  "success": true|false,
  "message": "Human-readable message",
  "error": "Error details (if success=false)",
  ...additional tool-specific fields
}
```

### Table ID Format
- Format: `dt_` + 8-character unique identifier
- Example: `dt_abc12345`

### Filter Operators
- `eq`, `ne` - Equality/inequality
- `gt`, `gte`, `lt`, `lte` - Comparison
- `contains`, `startswith`, `endswith` - String operations
- `isnull`, `notnull` - Null checks

### Aggregation Functions
- `count`, `sum`, `mean`, `median`
- `min`, `max`, `std`, `var`
- `first`, `last`, `nunique`

## Development

### Requirements
- Python 3.11+
- pandas >= 2.0.0
- fastmcp >= 2.3.3
- numpy >= 1.24.0

### Testing
```bash
# Run basic functionality test
python test_basic.py

# Test specific tool
python -c "
import asyncio
from datatable_tools.lifecycle_tools import create_table
result = asyncio.run(create_table([[1,2], [3,4]], ['A', 'B']))
print(result)
"
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Support

For issues and questions:
- GitHub Issues: [Repository Issues](link-to-issues)
- Documentation: This README and inline code documentation