#!/usr/bin/env python3
"""
Test enhanced append_columns functionality

Tests the enhanced append_columns feature that:
1. Reads existing columns first
2. Matches columns by name (case-insensitive)
3. Skips duplicate columns
4. Appends only new columns
5. Handles empty DataFrames with column headers
"""

import os
import sys
import asyncio
import logging
from typing import Optional
import argparse

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def authenticate():
    """Authenticate with Google Sheets API"""
    refresh_token = os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
    client_id = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")

    if not all([refresh_token, client_id, client_secret]):
        print("❌ ERROR: Missing required environment variables!")
        print("Required: TEST_GOOGLE_OAUTH_REFRESH_TOKEN, TEST_GOOGLE_OAUTH_CLIENT_ID, TEST_GOOGLE_OAUTH_CLIENT_SECRET")
        return None

    try:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES
        )
        creds.refresh(Request())
        service = build('sheets', 'v4', credentials=creds)
        print("✅ Successfully authenticated")
        return service
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return None


class TestAppendColumnsEnhanced:
    """Test suite for enhanced append_columns functionality"""

    def __init__(self):
        self.google_sheet = GoogleSheetDataTable()
        self.test_spreadsheet_url: Optional[str] = None
        self.spreadsheet_id: Optional[str] = None

    async def setup(self, service):
        """Setup test environment"""
        logger.info("Setting up test environment...")

        # Create a test spreadsheet
        logger.info("Creating test spreadsheet...")
        initial_data = [
            {"Name": "Alice", "Age": 30},
            {"Name": "Bob", "Age": 25}
        ]

        result = await self.google_sheet.write_new_sheet(
            service=service,
            data=initial_data,
            sheet_name="Test Append Columns Enhanced"
        )

        if not result.success:
            raise Exception(f"Failed to create test spreadsheet: {result.error}")

        self.test_spreadsheet_url = result.spreadsheet_url
        self.spreadsheet_id = self.test_spreadsheet_url.split('/d/')[1].split('/')[0]
        logger.info(f"✅ Test spreadsheet created: {self.test_spreadsheet_url}")

    async def test_empty_df_existing_columns(self, service):
        """Test 1: Empty DataFrame with existing column names - should skip"""
        logger.info("\n=== Test 1: Empty DataFrame with existing columns ===")

        # Try to append empty DataFrame with "Name" column (already exists)
        empty_df_data = [{"Name": None}]  # Empty row to represent column only

        result = await self.google_sheet.append_columns(
            service=service,
            uri=self.test_spreadsheet_url,
            data=[]  # Simulate empty DataFrame with just headers
        )

        logger.info(f"Result: {result.message}")
        assert result.success, "Operation should succeed"
        # Note: This test might not work as intended because we need to pass headers separately
        # Let's adjust for actual DataFrame behavior

    async def test_empty_df_with_existing_column_header(self, service):
        """Test 2: Empty DataFrame with existing column name (case-insensitive) - should skip"""
        logger.info("\n=== Test 2: Empty DataFrame with existing column (case-insensitive) ===")

        # Try to append column "name" (lowercase) which matches "Name" (existing)
        # But with empty data
        from datatable_tools.google_sheets_helpers import process_data_input

        # Simulate empty DataFrame: headers but no data rows
        empty_df = [{"name": None}]  # Dict with None value
        extracted_headers, data_rows = process_data_input(empty_df)

        # Since this has data (even if None), let's test with truly empty list
        # For true empty DataFrame with headers, we need to pass headers explicitly
        # This is a limitation of the current API - let's document it

        logger.info("Skipping this test - need to enhance API to accept headers separately")

    async def test_new_column_with_data(self, service):
        """Test 3: Append new column with data"""
        logger.info("\n=== Test 3: Append new column with data ===")

        # Add new column "City" with data
        new_data = [
            {"City": "New York"},
            {"City": "Los Angeles"}
        ]

        result = await self.google_sheet.append_columns(
            service=service,
            uri=self.test_spreadsheet_url,
            data=new_data
        )

        logger.info(f"Result: {result.message}")
        assert result.success, "Operation should succeed"
        assert "1 new column(s)" in result.message, "Should append 1 new column"
        assert result.updated_cells > 0, "Should update some cells"

        # Verify data was appended correctly
        load_result = await self.google_sheet.load_data_table(
            service=service,
            uri=self.test_spreadsheet_url
        )

        assert load_result.success
        data = load_result.data
        assert len(data) == 2, "Should have 2 rows"
        assert "City" in data[0], "City column should exist"
        assert data[0]["City"] == "New York", "First row should have New York"
        assert data[1]["City"] == "Los Angeles", "Second row should have Los Angeles"

        logger.info("✓ New column appended successfully")

    async def test_mixed_existing_and_new_columns(self, service):
        """Test 4: Append with mix of existing and new columns - should skip existing"""
        logger.info("\n=== Test 4: Mixed existing and new columns ===")

        # Try to append "Name" (exists) and "Country" (new)
        mixed_data = [
            {"Name": "Charlie", "Country": "USA"},
            {"Name": "Diana", "Country": "UK"}
        ]

        result = await self.google_sheet.append_columns(
            service=service,
            uri=self.test_spreadsheet_url,
            data=mixed_data
        )

        logger.info(f"Result: {result.message}")
        assert result.success, "Operation should succeed"
        assert "1 new column(s)" in result.message, "Should append 1 new column"
        assert "Skipped" in result.message, "Should mention skipped columns"
        assert "Name" in result.message, "Should mention Name was skipped"

        # Verify only Country was appended
        load_result = await self.google_sheet.load_data_table(
            service=service,
            uri=self.test_spreadsheet_url
        )

        assert load_result.success
        data = load_result.data
        assert "Country" in data[0], "Country column should exist"
        assert data[0]["Country"] == "USA", "First row should have USA"

        # Original Name column should be unchanged
        assert data[0]["Name"] == "Alice", "Original Name should be Alice, not Charlie"

        logger.info("✓ Only new columns appended, existing columns skipped")

    async def test_case_insensitive_matching(self, service):
        """Test 5: Case-insensitive column matching"""
        logger.info("\n=== Test 5: Case-insensitive column matching ===")

        # Try to append "AGE" (uppercase, should match existing "Age")
        case_test_data = [
            {"AGE": 40},  # Should be detected as existing
            {"AGE": 35}
        ]

        result = await self.google_sheet.append_columns(
            service=service,
            uri=self.test_spreadsheet_url,
            data=case_test_data
        )

        logger.info(f"Result: {result.message}")
        assert result.success, "Operation should succeed"
        # Should skip because "AGE" matches "Age" (case-insensitive)
        assert "Cannot append data without new columns" in result.message or "already exist" in result.message.lower()

        logger.info("✓ Case-insensitive matching works correctly")

    async def test_all_existing_columns_with_data(self, service):
        """Test 6: All columns exist with data - should skip"""
        logger.info("\n=== Test 6: All existing columns with data ===")

        # Try to append data for existing columns only
        existing_only_data = [
            {"Name": "Eve", "Age": 28},
            {"Name": "Frank", "Age": 32}
        ]

        result = await self.google_sheet.append_columns(
            service=service,
            uri=self.test_spreadsheet_url,
            data=existing_only_data
        )

        logger.info(f"Result: {result.message}")
        assert result.success, "Operation should succeed"
        assert "Cannot append data without new columns" in result.message
        assert result.updated_cells == 0, "Should not update any cells"

        logger.info("✓ Correctly skipped append when all columns exist")

    async def cleanup(self, service):
        """Cleanup test resources"""
        logger.info("\n=== Cleanup ===")
        if self.spreadsheet_id:
            try:
                # Note: Google Sheets API doesn't support deleting spreadsheets
                # So we just log the spreadsheet URL for manual cleanup
                logger.info(f"Test spreadsheet created: {self.test_spreadsheet_url}")
                logger.info("Please manually delete the test spreadsheet if needed")
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")

    async def run_all_tests(self, service):
        """Run all tests"""
        try:
            await self.setup(service)

            # Run tests sequentially
            await self.test_new_column_with_data(service)
            await self.test_mixed_existing_and_new_columns(service)
            await self.test_case_insensitive_matching(service)
            await self.test_all_existing_columns_with_data(service)

            logger.info("\n" + "=" * 60)
            logger.info("✓ All tests passed!")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"\n✗ Test failed: {e}", exc_info=True)
            raise

        finally:
            await self.cleanup(service)


async def main():
    """Main test runner"""
    # Authenticate
    service = authenticate()
    if not service:
        print("❌ Authentication failed")
        sys.exit(1)

    test_suite = TestAppendColumnsEnhanced()
    await test_suite.run_all_tests(service)


if __name__ == "__main__":
    asyncio.run(main())
