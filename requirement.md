
⏺ DataTable MCP Requirements Document

  Project Overview

  Develop a new DataTable MCP (Model Context Protocol) tool series to replace existing /sheet/ APIs with
  in-memory tabular data manipulation capabilities. The system will handle tabular data in memory while
  providing seamless integration with external sources (Google Sheets, Excel, databases).

  ---
  Core Requirements

  1. Architecture

  - In-Memory Processing: All table operations performed in memory for speed
  - Session-Based: Tables exist with unique IDs during session lifecycle
  - External I/O: Load from and save to Google Sheets, Excel, databases
  - Pandas-like Interface: Familiar DataFrame-style operations
  - Column Name Focused: Use semantic column names instead of indices to reduce LLM errors

  2. API Design Principles

  - Minimal Tool Count: 18 optimized tools (vs 25+ granular tools)
  - Unified Operations: Merge similar functions (SaveTable vs SaveToExcel/SaveToSpreadsheet)
  - Consistent Parameters: Standardized request/response patterns
  - Robust Handling: Handle mismatched data dimensions gracefully
  - Clear Naming: Verb+Object naming convention (FilterRows, SaveTable)

  ---
  Technical Specifications

  Base Configuration

  - API Prefix: /datatable
  - Protocol: HTTP REST API
  - Data Format: JSON request/response
  - Table ID Format: dt_ + unique identifier
  - Memory Management: Session-based cleanup with TTL

  Core Data Structure

  {
    "table_id": "dt_abc123",
    "headers": ["column1", "column2"],
    "data": [["value1", "value2"], ["value3", "value4"]],
    "shape": [rows, columns],
    "dtypes": {"column1": "string", "column2": "number"},
    "metadata": {
      "name": "table_name",
      "created_at": "ISO_timestamp",
      "source_info": {}
    }
  }

  ---
  Tool Requirements (18 Tools)

  1. Table Lifecycle Management (4 Tools)

  | Tool        | Endpoint               | Key Features                                    |
  |-------------|------------------------|-------------------------------------------------|
  | CreateTable | POST /datatable/create | Create from data array, auto-detect headers     |
  | LoadTable   | POST /datatable/load   | Unified loading from spreadsheet/excel/database |
  | CloneTable  | POST /datatable/clone  | Deep copy existing table                        |
  | ListTables  | GET /datatable/list    | Session table inventory                         |

  2. Data Manipulation (6 Tools)

  | Tool            | Endpoint                       | Key Features                                   |
  |-----------------|--------------------------------|------------------------------------------------|
  | AppendRow       | POST /datatable/add_row        | Robust handling of dimension mismatches        |
  | AddColumn       | POST /datatable/add_column     | Add with optional default values               |
  | SetRangeValues  | POST /datatable/set_values     | Pandas .loc style updates with fill strategies |
  | DeleteFromTable | POST /datatable/delete         | Unified row/column deletion                    |
  | RenameColumns   | POST /datatable/rename_columns | Bulk column renaming                           |
  | ClearRange      | POST /datatable/clear_range    | Clear values while preserving structure        |

  3. Data Query & Access (3 Tools)

  | Tool         | Endpoint                       | Key Features                                |
  |--------------|--------------------------------|---------------------------------------------|
  | GetTableData | GET /datatable/data/{table_id} | Flexible slicing, multiple output formats   |
  | FilterRows   | POST /datatable/filter         | Multi-condition filtering with AND/OR logic |
  | SortTable    | POST /datatable/sort           | Multi-column sorting, in-place or new table |

  4. Export & Persistence (2 Tools)

  | Tool        | Endpoint                         | Key Features                                |
  |-------------|----------------------------------|---------------------------------------------|
  | SaveTable   | POST /datatable/save             | Unified save to spreadsheet/excel/database  |
  | ExportTable | GET /datatable/export/{table_id} | Multiple formats: CSV, JSON, Excel, Parquet |

  5. Advanced Operations (3 Tools)

  | Tool          | Endpoint                  | Key Features                                 |
  |---------------|---------------------------|----------------------------------------------|
  | MergeTables   | POST /datatable/merge     | Pandas-style joins (inner/left/right/outer)  |
  | AggregateData | POST /datatable/aggregate | Group by with multiple aggregation functions |
  | MapValues     | POST /datatable/map       | Value transformation and mapping             |

  ---
  Integration Requirements

  External Data Sources

  - Google Sheets API: Read/write via existing /sheet/ infrastructure
  - Excel Files: Support .xlsx/.xls formats
  - Databases: Generic SQL connection support
  - File Formats: CSV, JSON, Parquet import/export

  Error Handling

  - Graceful Degradation: Handle dimension mismatches
  - Validation: Type checking and data validation
  - Warnings: Non-fatal issues (e.g., data padding)
  - Rollback: Transaction-like operations for complex updates

  Performance Requirements

  - Memory Efficiency: Optimize for tables up to 100K rows
  - Response Time: < 2s for most operations
  - Concurrent Sessions: Support multiple simultaneous table sessions
  - Cleanup: Automatic memory cleanup for expired tables

  ---
  Implementation Phases

  Phase 1: Core Infrastructure

  1. Table memory management system
  2. Basic CRUD operations (CreateTable, GetTableData, AppendRow)
  3. Session management and cleanup

  Phase 2: Data Operations

  1. Advanced manipulation tools (SetRangeValues, FilterRows)
  2. Query operations (SortTable, DeleteFromTable)
  3. Robust error handling and validation

  Phase 3: External Integration

  1. LoadTable and SaveTable implementations
  2. ExportTable with multiple formats
  3. Integration with existing /sheet/ APIs

  Phase 4: Advanced Features

  1. MergeTables and AggregateData
  2. MapValues and advanced transformations
  3. Performance optimization and caching

  ---
  Success Criteria

  Functional Requirements

  - ✅ All 18 tools implemented and tested
  - ✅ Seamless integration with Google Sheets
  - ✅ Support for Excel and CSV import/export
  - ✅ Robust handling of real-world data inconsistencies

  Performance Requirements

  - ✅ < 2s response time for 95% of operations
  - ✅ Support tables up to 100K rows
  - ✅ Memory usage < 1GB per active session
  - ✅ 99.9% uptime for core operations

  User Experience Requirements

  - ✅ Intuitive tool naming for LLM agents
  - ✅ Clear error messages and warnings
  - ✅ Consistent API patterns across all tools
  - ✅ Comprehensive documentation and examples

  ---
  Dependencies

  Technical Dependencies

  - Backend Framework: FastAPI (existing infrastructure)
  - Data Processing: Pandas or similar library
  - External APIs: Google Sheets API, Excel libraries
  - Storage: Session-based memory management
  - Authentication: Existing user authentication system

  External Services

  - Google Sheets API access
  - File storage for export functionality
  - Database connectivity (optional)

  ---
  This requirements document provides a comprehensive foundation for implementing the DataTable MCP with
  optimized tool count and robust functionality suitable for LLM-driven tabular data operations.

