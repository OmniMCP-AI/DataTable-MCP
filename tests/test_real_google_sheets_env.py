#!/usr/bin/env python3
"""
Real Google Sheets verification test for DataTable MCP
Based on: /Users/dengwei/work/ai/om3/api4/tests/tools/sheet/test_verify_permission.py
Uses the same credential approach as the original service.py
"""

import asyncio
import sys
import os
import logging
from pathlib import Path
from enum import Enum
from typing import Tuple

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

# Gspread asyncio imports (same as original service)
try:
    import gspread_asyncio
    from google.oauth2.credentials import Credentials
    from gspread_asyncio import AsyncioGspreadClient
    from pydantic import BaseModel
except ImportError as e:
    print(f"❌ Missing dependencies: {e}")
    print("💡 Install required packages:")
    print("   pip install gspread-asyncio pydantic")
    sys.exit(1)

# Test configuration - REAL Google Sheets IDs
READ_ONLY_SHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"  # Google Sheets sample data
READ_WRITE_SHEET_ID = "1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M"  # Read-write allowed sheet
INVALID_SHEET_ID = "invalid_sheet_id_that_does_not_exist"
TEST_USER_ID = "68501372a3569b6897673a48"  # Wade's user ID from existing test

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpreadsheetPermission(Enum):
    """Enum for spreadsheet permission levels (same as original service)"""
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    NOT_EXISTS = "not_exists"

class GoogleCredentials(BaseModel):
    """Google credentials model (same as original service)"""
    access_token: str
    refresh_token: str
    scope: str

class RealGoogleSheetsVerifier:
    """Real Google Sheets verifier using the same approach as original service"""

    def __init__(self):
        self.test_results = []
        self.client = None

    def get_credentials_from_env(self) -> GoogleCredentials:
        """
        Get Google credentials from environment variables
        This mimics the MongoDB approach but uses env vars for testing
        """
        access_token = os.getenv('GOOGLE_ACCESS_TOKEN')
        refresh_token = os.getenv('GOOGLE_REFRESH_TOKEN')
        scope = os.getenv('GOOGLE_SCOPE', 'https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive')

        if not access_token:
            raise ValueError("GOOGLE_ACCESS_TOKEN environment variable not found")
        if not refresh_token:
            raise ValueError("GOOGLE_REFRESH_TOKEN environment variable not found")

        return GoogleCredentials(
            access_token=access_token,
            refresh_token=refresh_token,
            scope=scope
        )

    async def get_client(self) -> AsyncioGspreadClient:
        """Get asyncio gspread client (same approach as original service)"""
        try:
            user_credentials = self.get_credentials_from_env()
            creds = Credentials(
                token=user_credentials.access_token,
                refresh_token=user_credentials.refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=os.getenv("GOOGLE_CLIENT_ID"),
                client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
                scopes=user_credentials.scope.split(),
            )
            async_client_manager = gspread_asyncio.AsyncioGspreadClientManager(
                lambda: creds
            )
            return await async_client_manager.authorize()
        except Exception as e:
            print(f"❌ Error getting Google Sheets client: {e}")
            print("💡 Make sure these environment variables are set:")
            print("   - GOOGLE_CLIENT_ID")
            print("   - GOOGLE_CLIENT_SECRET")
            print("   - GOOGLE_ACCESS_TOKEN")
            print("   - GOOGLE_REFRESH_TOKEN")
            print("   - GOOGLE_SCOPE (optional)")
            raise

    def _is_permission_error(self, error: Exception) -> bool:
        """Check if error is permission-related (same logic as original service)"""
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()
        return (
            "permission" in error_str or
            "forbidden" in error_str or
            "access" in error_str or
            "403" in error_str or
            "spreadsheetnotfound" in error_type or
            "not found" in error_str or
            "404" in error_str or
            ("apierror" in error_type and (
                "permission" in error_str or "forbidden" in error_str or
                "403" in error_str or "access" in error_str
            ))
        )

    async def check_spreadsheet_permission(self, spreadsheet_id: str) -> Tuple[SpreadsheetPermission, str]:
        """
        Check spreadsheet permission (same logic as original service)
        """
        if not self.client:
            self.client = await self.get_client()

        try:
            # First, try to open the spreadsheet
            spreadsheet = await self.client.open_by_key(spreadsheet_id)
            # If we can open it, try to get the first worksheet for testing
            try:
                worksheets = await spreadsheet.worksheets()
                if not worksheets:
                    return SpreadsheetPermission.READ_ONLY, "Spreadsheet exists but has no worksheets"
                first_worksheet = worksheets[0]
                # Try a minimal write operation to test permissions
                # We'll try to read a single cell first, then write to it
                try:
                    # Test read access
                    current_value = await first_worksheet.acell('A1')
                    # Test write access with a safe operation
                    # We'll write the same value back to avoid changing data
                    test_value = current_value.value if current_value else ""
                    await first_worksheet.update(
                        range_name='A1', values=[[test_value]], raw=False
                    )
                    return SpreadsheetPermission.READ_WRITE, f"Full read-write access to spreadsheet '{spreadsheet.title}'"
                except Exception as write_error:
                    # If write fails but read succeeded, it's read-only
                    if self._is_permission_error(write_error):
                        return SpreadsheetPermission.READ_ONLY, f"Read-only access to spreadsheet '{spreadsheet.title}' (write permission denied)"
                    else:
                        # Some other error during write test
                        return SpreadsheetPermission.READ_ONLY, f"Read-only access to spreadsheet '{spreadsheet.title}' (write test failed: {str(write_error)})"
            except Exception as worksheet_error:
                # Can open spreadsheet but can't access worksheets
                return SpreadsheetPermission.READ_ONLY, f"Can access spreadsheet '{spreadsheet.title}' but cannot access worksheets: {str(worksheet_error)}"
        except Exception as access_error:
            # Cannot open spreadsheet at all
            error_str = str(access_error).lower()
            if self._is_permission_error(access_error):
                if "not found" in error_str or "404" in error_str:
                    return SpreadsheetPermission.NOT_EXISTS, f"Spreadsheet '{spreadsheet_id}' does not exist or is not accessible"
                else:
                    return SpreadsheetPermission.NOT_EXISTS, f"No access to spreadsheet '{spreadsheet_id}' (permission denied)"
            else:
                return SpreadsheetPermission.NOT_EXISTS, f"Cannot access spreadsheet '{spreadsheet_id}': {str(access_error)}"

    def print_header(self, title: str):
        """Print formatted test header"""
        print(f"🧪 {title}")
        print("=" * 70)

    def print_result(self, test_name: str, success: bool, expected: str, actual: str, message: str, sheet_url: str = None):
        """Print formatted test result"""
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        print(f"  Expected: {expected}")
        print(f"  Actual: {actual}")
        print(f"  Message: {message}")
        if sheet_url:
            print(f"  🌐 URL: {sheet_url}")
        print()

    def record_result(self, test_name: str, success: bool, expected: str, actual: str):
        """Record test result for final summary"""
        self.test_results.append({
            'name': test_name,
            'success': success,
            'expected': expected,
            'actual': actual
        })

    async def test_permission_verification(self):
        """Test permission verification on real spreadsheets"""
        self.print_header("REAL Google Sheets Permission Verification Tests")

        test_cases = [
            ("Read-Write Sheet", READ_WRITE_SHEET_ID, SpreadsheetPermission.READ_WRITE),
            ("Read-Only Sheet", READ_ONLY_SHEET_ID, SpreadsheetPermission.READ_ONLY),
            ("Invalid Sheet", INVALID_SHEET_ID, SpreadsheetPermission.NOT_EXISTS),
        ]

        print(f"Test Configuration:")
        print(f"  Read-Write Sheet: https://docs.google.com/spreadsheets/d/{READ_WRITE_SHEET_ID}/edit")
        print(f"  Read-Only Sheet: https://docs.google.com/spreadsheets/d/{READ_ONLY_SHEET_ID}/edit")
        print(f"  Invalid Sheet: {INVALID_SHEET_ID}")
        print(f"  Test User ID: {TEST_USER_ID}")
        print()

        for test_name, sheet_id, expected_permission in test_cases:
            try:
                permission, message = await self.check_spreadsheet_permission(sheet_id)
                success = permission == expected_permission
                sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit" if sheet_id != INVALID_SHEET_ID else None

                self.print_result(
                    test_name, success,
                    expected_permission.value, permission.value, message, sheet_url
                )
                self.record_result(test_name, success, expected_permission.value, permission.value)

            except Exception as e:
                sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit" if sheet_id != INVALID_SHEET_ID else None
                self.print_result(
                    test_name, False,
                    expected_permission.value, f"ERROR: {type(e).__name__}", str(e), sheet_url
                )
                self.record_result(test_name, False, expected_permission.value, f"ERROR: {str(e)}")

    async def test_read_operations(self):
        """Test actual read operations on real spreadsheets"""
        self.print_header("REAL Google Sheets Read Operations Tests")

        if not self.client:
            self.client = await self.get_client()

        # Test read-write sheet
        print("🧪 Testing Read-Write Sheet")
        print(f"📋 URL: https://docs.google.com/spreadsheets/d/{READ_WRITE_SHEET_ID}/edit")

        try:
            spreadsheet = await self.client.open_by_key(READ_WRITE_SHEET_ID)
            worksheets = await spreadsheet.worksheets()
            if worksheets:
                first_worksheet = worksheets[0]
                # Read first few cells
                values = await first_worksheet.get('A1:E5')
                print(f"  ✅ PASS Read Test: Read {len(values)} rows from '{first_worksheet.title}'")
                if values and values[0]:
                    print(f"    Sample data: {values[0][:3]}")
            else:
                print(f"  ❌ FAIL Read Test: No worksheets found")
        except Exception as e:
            print(f"  ❌ FAIL Read Test: {e}")

        print()

        # Test read-only sheet
        print("🧪 Testing Read-Only Sheet")
        print(f"📋 URL: https://docs.google.com/spreadsheets/d/{READ_ONLY_SHEET_ID}/edit")

        try:
            spreadsheet = await self.client.open_by_key(READ_ONLY_SHEET_ID)
            worksheets = await spreadsheet.worksheets()
            if worksheets:
                first_worksheet = worksheets[0]
                # Read first few cells
                values = await first_worksheet.get('A1:E5')
                print(f"  ✅ PASS Read Test: Read {len(values)} rows from '{first_worksheet.title}'")
                if values and values[0]:
                    print(f"    Sample data: {values[0][:3]}")
            else:
                print(f"  ❌ FAIL Read Test: No worksheets found")
        except Exception as e:
            print(f"  ❌ FAIL Read Test: {e}")

        print()

    def print_summary(self):
        """Print test summary"""
        print("🏁 REAL GOOGLE SHEETS VERIFICATION SUMMARY")
        print("=" * 70)

        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['success'])

        print(f"Total Permission Tests: {total_tests}")
        print(f"Passed: {passed_tests} ✅")
        print(f"Failed: {total_tests - passed_tests} ❌")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "No tests run")
        print()

        print("📋 SPREADSHEET LINKS:")
        print(f"  Read-Write: https://docs.google.com/spreadsheets/d/{READ_WRITE_SHEET_ID}/edit")
        print(f"  Read-Only: https://docs.google.com/spreadsheets/d/{READ_ONLY_SHEET_ID}/edit")

        if passed_tests < total_tests:
            print(f"\n❌ FAILED TESTS:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  - {result['name']}: Expected '{result['expected']}', got '{result['actual']}'")
        else:
            print(f"\n🎉 All permission verification tests passed!")


async def run_real_google_sheets_verification():
    """Run real Google Sheets verification tests"""
    verifier = RealGoogleSheetsVerifier()

    try:
        # Run permission tests
        await verifier.test_permission_verification()

        # Run read operations tests
        await verifier.test_read_operations()

        # Print summary
        verifier.print_summary()

        return True

    except Exception as e:
        print(f"❌ Error during verification: {e}")
        return False


if __name__ == "__main__":
    print("🚀 REAL Google Sheets Verification Test")
    print("📋 This will test ACTUAL Google Sheets permissions and operations")
    print("🔑 Using the same credential approach as the original service")
    print("=" * 70)

    success = asyncio.run(run_real_google_sheets_verification())

    if success:
        print("\n🎉 Real Google Sheets verification completed!")
    else:
        print("\n⚠️  Verification failed. Check credentials and setup.")

    sys.exit(0 if success else 1)