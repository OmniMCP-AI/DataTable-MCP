#!/usr/bin/env python3
"""
Utility to generate Google Sheets URLs for manual verification of local spreadsheets
"""

import sys
import os
from pathlib import Path

def generate_sheets_url(spreadsheet_id, worksheet_id=None, worksheet_name=None):
    """Generate a Google Sheets URL for manual verification"""
    base_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/"
    if worksheet_id:
        return base_url + f"edit#gid={worksheet_id}"
    elif worksheet_name:
        return base_url + f"edit#worksheet={worksheet_name}"
    else:
        return base_url + "edit"

def list_local_spreadsheets():
    """List all local spreadsheet files and generate URLs"""
    temp_dir = Path("/tmp/datatable_spreadsheets")

    if not temp_dir.exists():
        print("❌ No local spreadsheet directory found at /tmp/datatable_spreadsheets/")
        return

    files = list(temp_dir.glob("*.xlsx"))
    if not files:
        print("❌ No spreadsheet files found")
        return

    print(f"🗂️  Found {len(files)} local spreadsheet files:")
    print("=" * 70)

    for file in files:
        # Extract spreadsheet ID from filename (remove .xlsx extension)
        spreadsheet_id = file.stem
        file_size = file.stat().st_size

        print(f"📄 {file.name} ({file_size} bytes)")
        print(f"   ID: {spreadsheet_id}")
        print(f"   🌐 URL: {generate_sheets_url(spreadsheet_id)}")
        print()

def main():
    """Main function"""
    if len(sys.argv) == 1:
        # No arguments - list all spreadsheets
        list_local_spreadsheets()
    elif len(sys.argv) == 2:
        # Single argument - generate URL for specific spreadsheet
        spreadsheet_id = sys.argv[1]
        url = generate_sheets_url(spreadsheet_id)
        print(f"🌐 Google Sheets URL for '{spreadsheet_id}':")
        print(url)
    elif len(sys.argv) == 3:
        # Two arguments - spreadsheet ID and worksheet name
        spreadsheet_id, worksheet_name = sys.argv[1], sys.argv[2]
        url = generate_sheets_url(spreadsheet_id, worksheet_name=worksheet_name)
        print(f"🌐 Google Sheets URL for '{spreadsheet_id}' worksheet '{worksheet_name}':")
        print(url)
    else:
        print("Usage:")
        print("  python generate_sheets_urls.py                    # List all local spreadsheets")
        print("  python generate_sheets_urls.py <spreadsheet_id>   # Generate URL for specific spreadsheet")
        print("  python generate_sheets_urls.py <id> <worksheet>   # Generate URL for specific worksheet")
        print()
        print("Examples:")
        print("  python generate_sheets_urls.py")
        print("  python generate_sheets_urls.py mcp-integration-test-001")
        print("  python generate_sheets_urls.py integration-test-001 Employee_Data")

if __name__ == "__main__":
    main()