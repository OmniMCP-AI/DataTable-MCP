# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataTable MCP Server - A Model Context Protocol (MCP) server providing Google Sheets integration with 10 core tools for reading, writing, and manipulating spreadsheet data. Built with FastMCP and designed for LLM-driven operations.

**Architecture Style**: Clean layered architecture with decorator-based authentication
- MCP Layer: Tool definitions with decorators (`datatable_tools/mcp_tools.py`)
- Implementation Layer: Framework-agnostic business logic (`datatable_tools/third_party/google_sheets/datatable.py`)
- Auth Layer: OAuth2 flow with service caching (`datatable_tools/auth/`)
- Interface Layer: Abstract base class for future implementations (`datatable_tools/interfaces/datatable.py`)

## Common Commands

### Running the Server

```bash
# stdio mode (default, for MCP clients)
python main.py --transport stdio

# HTTP mode (for testing and development)
python main.py --transport streamable-http --port 8321

# Using justfile shortcuts
just api          # Run HTTP server on port 8321
just dev          # Run with auto-reload using watchexec
```

### Running in local 

there might be VPN issue , might need to 
```bash
export http_proxy=http://127.0.0.1:7897 https_proxy=http://127.0.0.1:7897 all_proxy=socks5://127.0.0.1:7897
```

### Testing

**Test Pattern**: All tests follow the pattern in `tests/test_mcp_client_calltool.py` using real MCP client connections.

#### major test case with category
    tests/test_mcp_client_calltool.py --env=local --test=basic

```bash

# Run specific integration test (requires TEST_GOOGLE_OAUTH_* env vars)
if [ -n "$TEST_GOOGLE_OAUTH_REFRESH_TOKEN" ]; then
    .venv/bin/python tests/test_mcp_client_calltool.py --env=local --test=basic
fi

# Common test commands (check pre-approved Bash tool patterns)
timeout 60s .venv/bin/python tests/test_*.py --env=local
timeout 60s .venv/bin/python tests/test_*.py --env=test

# Examples:
.venv/bin/python tests/test_update_by_lookup.py --env=local --test=basic
.venv/bin/python tests/test_append_rows_resize.py
.venv/bin/python tests/test_numeric_values.py --env=test
```

**Environment Variables Required for Tests**:
- `TEST_GOOGLE_OAUTH_REFRESH_TOKEN`
- `TEST_GOOGLE_OAUTH_CLIENT_ID`
- `TEST_GOOGLE_OAUTH_CLIENT_SECRET`

**Test Environments**: Use `--env=local` for local OAuth testing, `--env=test` for remote server testing.

### Dependencies

```bash
# Install dependencies
pip install -e .
# or
pip install -r requirements.txt

# Install dev dependencies
rye add pytest pytest-asyncio  # Project uses rye for dep management
```

## Architecture Details

### Tool Registration Pattern

All MCP tools are registered in `datatable_tools/mcp_tools.py` using the `@mcp.tool` decorator combined with `@require_google_service` for automatic OAuth2 authentication:

```python
@mcp.tool
@require_google_service("sheets", "sheets_read")
async def read_sheet(
    service,  # Injected by @require_google_service decorator
    ctx: Context,
    uri: str,
    range_address: Optional[str] = None
) -> TableResponse:
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.load_data_table(service, uri, range_address)
```

**Key Points**:
- `@require_google_service` handles OAuth2 flow, token refresh, and service caching
- Service instances are cached for 30 minutes per user/scope combination
- Decorator injects authenticated `service` parameter automatically
- MCP tools delegate to `GoogleSheetDataTable` implementation class

### Authentication Flow

Located in `datatable_tools/auth/`:
- `service_decorator.py`: Main decorator and service caching logic
- `google_auth.py`: OAuth2 flow implementation (57KB - handles full OAuth2 lifecycle)
- `service_factory.py`: Google API service construction
- `scopes.py`: Scope definitions (sheets_read, sheets_write, etc.)

**Authentication Sequence**:
1. Tool called → `@require_google_service` checks cache
2. If not cached → Extract OAuth tokens from MCP context
3. Build credentials → Construct Google API service
4. Cache service → Inject into tool function
5. Auto-refresh tokens if expired

### Data Input Processing

`datatable_tools/google_sheets_helpers.py` contains `process_data_input()` which normalizes input formats:
- **List[List[Any]]**: 2D array (standard format)
- **List[Dict[str, Any]]**: DataFrame-like dicts (auto-converts to 2D array)
- **List[Any]**: 1D array (converts to single row/column based on context)

**Column Alignment**: `align_dict_data_to_headers()` in `datatable.py` ensures dict keys match sheet header order using case-insensitive matching.

### Range Address Parsing

Complex range parsing logic in `google_sheets_helpers.py`:
- Supports A1 notation: `"A2:M1000"`, `"2:1000"` (row range), `"B:Z"` (column range)
- Auto-detection for merged cells and header rows
- `detect_header_row()` analyzes first 5 rows to find real headers

### Formula Handling

Two reading modes (controlled by `value_render_option`):
1. **FORMATTED_VALUE** (default): Returns calculated values as displayed in UI
2. **FORMULA**: Returns raw formulas (e.g., `"=SUM(A1:A10)"`)

Tools:
- `read_sheet`: Returns formatted values
- `read_worksheet_with_formulas`: Returns raw formulas
- `preview_worksheet_with_formulas`: Returns formulas for first N rows

### Logging System

Structured logging using `structlog` (configured in `core/logging_config.py`):
- **Local/Dev**: Console output only (colorized)
- **Production**: JSON logs to `LOG_FOLDER` (default: `./logs/`)
- Automatic fallback locations if log folder creation fails
- Log files: `datatable-mcp-{date}.log` and `datatable-mcp-{date}.json`

**Usage Pattern**:
```python
from core.logging_config import get_logger
logger = get_logger(__name__)
logger.info("operation_completed", table_id=table_id, rows=len(data))
```

### Error Handling

- `GoogleAuthenticationError`: OAuth/auth failures (defined in `google_auth.py`)
- `core/error.py`: Base error classes
- All tools return structured responses with `success`, `message`, `error` fields
- Auto-retry on token refresh errors (handled by decorator)

## Development Guidelines

### Adding New Tools

1. Add `@mcp.tool` decorated function to `datatable_tools/mcp_tools.py`
2. Stack `@require_google_service("service_name", "scope_group")` for auth
3. Delegate to implementation class (e.g., `GoogleSheetDataTable`)
4. Return typed response model (e.g., `TableResponse`, `UpdateResponse`)
5. Write integration test following `tests/test_mcp_client_calltool.py` pattern

### Google Sheets URI Format

Expected format: `https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid}#gid={gid}`

Parsed by `parse_google_sheets_uri()` to extract `spreadsheet_id` and `gid`.

### Docker Deployment

```bash
# Build image
docker build -t datatable-mcp .

# Run container
docker run -p 8321:8321 \
  -e GOOGLE_CLIENT_ID=your_client_id \
  -e GOOGLE_CLIENT_SECRET=your_client_secret \
  datatable-mcp

# Health check endpoint
curl http://localhost:8321/health
```

See `docs/docker.md` for detailed deployment instructions.

## Key Files Reference

- `main.py`: Server entry point, argument parsing, transport selection
- `core/server.py`: FastMCP instance creation, health check endpoint
- `core/settings.py`: Environment configuration (ENV, LOG_LEVEL, LOG_FOLDER)
- `datatable_tools/mcp_tools.py`: All 10 MCP tool definitions (63KB)
- `datatable_tools/third_party/google_sheets/datatable.py`: GoogleSheetDataTable implementation
- `datatable_tools/google_sheets_helpers.py`: Utility functions (35KB)
- `datatable_tools/models.py`: Pydantic response models
- `datatable_tools/auth/service_decorator.py`: Auth decorator and caching (20KB)
- `datatable_tools/interfaces/datatable.py`: Abstract interface for future backends

## Important Patterns

### Auto-Header Detection

When `range_address` is None, `read_sheet` automatically detects header row by:
1. Reading first 5 rows from sheet
2. Analyzing for merged cells, empty rows, title rows
3. Selecting row with most non-empty unique values as header
4. Skipping rows above detected header

**Override**: Use `range_address="2:10000"` to explicitly start from row 2 (skips merged title rows).

### Update by Lookup Pattern

`update_range_by_lookup` performs SQL-like UPDATE...JOIN:
1. Load existing sheet data with headers
2. Match rows using lookup column (case-insensitive)
3. Update only specified columns (preserves other columns)
4. Supports partial column updates (not all columns required)

See `tests/test_update_by_lookup.py` for usage examples.

### Batch Optimization

For operations affecting multiple ranges, use batch requests:
- `copy_range` internally uses batch API for multiple copy operations
- Reduces API calls from N to 1 for N operations
- Implemented in `datatable.py` using `batchUpdate` endpoint

## Troubleshooting

**Common Issues**:

1. **Authentication Errors**: Check that OAuth tokens are passed in MCP context with correct scopes
2. **Range Parsing Failures**: Verify A1 notation format, check for invalid range addresses
3. **Merged Cell Headers**: Use `range_address="2:10000"` to skip merged title row
4. **Formula vs Value**: Use `read_worksheet_with_formulas` for raw formulas, `read_sheet` for calculated values
5. **Logging Failures**: Check `LOG_FOLDER` permissions, falls back to `/tmp/datatable-mcp-logs` if needed

**Debugging Tests**: Add `--env=local` for detailed OAuth flow logs, use `timeout 60s` to prevent hanging on API calls.
