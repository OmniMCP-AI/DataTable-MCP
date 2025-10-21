# Standalone GoogleSheetDataTable Tests

This directory contains tests demonstrating that `GoogleSheetDataTable` can be used **without FastMCP** in any Python project.

## Stage 4.2 Achievement

After Stage 4.2 refactoring, `GoogleSheetDataTable` is **framework-agnostic**:

- ✅ No FastMCP dependency
- ✅ No MCP Context required
- ✅ Works with standard Google API client library
- ✅ Can be integrated into Flask, Django, FastAPI, or any Python app

## Running the Tests

```bash
# Set environment variables
export TEST_GOOGLE_OAUTH_REFRESH_TOKEN="your_refresh_token"
export TEST_GOOGLE_OAUTH_CLIENT_ID="your_client_id"
export TEST_GOOGLE_OAUTH_CLIENT_SECRET="your_client_secret"

# Run standalone tests
python tests/standard_datatable/test_standalone.py
```

## Test Cases

1. **Load Data** - Load a table from Google Sheets
2. **Update Range** - Update a specific range
3. **Create New Sheet** - Create a new spreadsheet

## Usage Example

```python
import asyncio
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

async def main():
    # Create Google Sheets service (standard Google API)
    creds = Credentials(
        token=None,
        refresh_token="your_refresh_token",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="your_client_id",
        client_secret="your_client_secret"
    )
    service = build('sheets', 'v4', credentials=creds)

    # Use GoogleSheetDataTable (no FastMCP needed!)
    google_sheet = GoogleSheetDataTable()

    # Update a range
    result = await google_sheet.update_range(
        service=service,
        uri="https://docs.google.com/spreadsheets/d/ABC123/edit?gid=0",
        data=[["Name", "Age"], ["Alice", 30]],
        range_address="A1"
    )

    print(result)

asyncio.run(main())
```

## Dependencies

Only standard Google API libraries needed:

```bash
pip install google-api-python-client google-auth google-auth-oauthlib
```

No FastMCP required!

## Architecture

```
Your Python App (Flask/Django/FastAPI/etc.)
    ↓
GoogleSheetDataTable (framework-agnostic)
    ↓
Google Sheets API (standard Google client)
```

This demonstrates the power of Stage 4.2 refactoring - clean separation between MCP layer and business logic.
