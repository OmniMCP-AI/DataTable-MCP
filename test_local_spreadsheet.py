#!/usr/bin/env python3
"""
Test script for the local spreadsheet implementation
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from datatable_tools.third_party.spreadsheet import (
    SpreadsheetClient,
    ReadSheetRequest,
    WriteSheetRequest,
    WorkSheetInfo
)

TEST_USER_ID = "68501372a3569b6897673a48"  # Test user ID

async def test_local_spreadsheet():
    """Test local spreadsheet functionality"""
    print("Testing local spreadsheet implementation...")

    client = SpreadsheetClient()

    # Test 1: Write data to a new spreadsheet
    print("\n1. Testing write operation...")
    write_request = WriteSheetRequest(
        spreadsheet_id="test-spreadsheet-001",
        worksheet=WorkSheetInfo(name="TestSheet"),
        values=[
            ["Name", "Age", "City"],
            ["John", "25", "New York"],
            ["Jane", "30", "Los Angeles"],
            ["Bob", "35", "Chicago"]
        ]
    )

    try:
        write_response = await client.write_sheet(write_request, TEST_USER_ID)
        print(f"✓ Write successful: {write_response.message}")
        print(f"  Updated range: {write_response.updated_range}")
        print(f"  Updated cells: {write_response.updated_cells}")
        print(f"  Worksheet URL: {write_response.worksheet_url}")
    except Exception as e:
        print(f"✗ Write failed: {e}")
        return False

    # Test 2: Read data back
    print("\n2. Testing read operation...")
    read_request = ReadSheetRequest(
        spreadsheet_id="test-spreadsheet-001",
        worksheet=WorkSheetInfo(name="TestSheet")
    )

    try:
        read_response = await client.read_sheet(read_request, TEST_USER_ID)
        print(f"✓ Read successful: {read_response.message}")
        print(f"  Used range: {read_response.used_range}")
        print(f"  Row count: {read_response.row_count}")
        print(f"  Column count: {read_response.column_count}")
        print(f"  Headers: {read_response.headers}")
        print(f"  Sample data: {read_response.values[:2] if read_response.values else 'No data'}")
    except Exception as e:
        print(f"✗ Read failed: {e}")
        return False

    # Test 3: Write to another spreadsheet
    print("\n3. Testing second spreadsheet...")
    write_request2 = WriteSheetRequest(
        spreadsheet_id="test-spreadsheet-002",
        worksheet=WorkSheetInfo(name="Sheet1"),
        values=[
            ["Product", "Price", "Stock"],
            ["Laptop", "999", "50"],
            ["Mouse", "25", "100"]
        ]
    )

    try:
        write_response2 = await client.write_sheet(write_request2, TEST_USER_ID)
        print(f"✓ Second spreadsheet write successful: {write_response2.message}")
    except Exception as e:
        print(f"✗ Second write failed: {e}")
        return False

    print("\n✅ All tests passed! Local spreadsheet implementation is working.")
    return True

if __name__ == "__main__":
    success = asyncio.run(test_local_spreadsheet())
    sys.exit(0 if success else 1)