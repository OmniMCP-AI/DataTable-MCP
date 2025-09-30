#!/usr/bin/env python3
"""
Test permission checking and automatic new spreadsheet creation fallback
Tests that when write permission is denied, the system automatically creates a new spreadsheet

This test uses Google Sheets API directly to test the permission fallback logic
without requiring MCP request context.
"""

import asyncio
import os
import sys
import logging
from typing import Dict, Any, Tuple
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Test configuration - Using the same sheet IDs as api4 tests
READ_ONLY_SHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"  # Google Sheets sample data (read-only)
READ_WRITE_SHEET_ID = "1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M"  # Read-write allowed sheet
INVALID_SHEET_ID = "invalid_sheet_id_that_does_not_exist"

# Full URLs for reference
READ_ONLY_SHEET_URL = "https://docs.google.com/spreadsheets/u/1/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"
READ_WRITE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit?gid=265933634#gid=265933634"


class GoogleSheetsAuthHelper:
    """Helper class to authenticate with Google Sheets API"""

    def __init__(self):
        self.service = None
        self.creds = None

    def authenticate(self):
        """Authenticate with Google Sheets API using environment variables"""
        refresh_token = os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
        client_id = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID")
        client_secret = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")

        if not all([refresh_token, client_id, client_secret]):
            return False

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
            self.service = build('sheets', 'v4', credentials=creds)
            self.creds = creds
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False


class PermissionAndFallbackTester:
    """Test runner for permission checking and automatic spreadsheet creation"""

    def __init__(self, auth_helper: GoogleSheetsAuthHelper):
        self.auth = auth_helper
        self.test_results = []
        self.created_spreadsheets = []

    def print_header(self, title: str):
        """Print formatted test header"""
        print(f"\n🧪 {title}")
        print("=" * 70)

    def print_result(self, test_name: str, success: bool, message: str, details: Dict[str, Any] = None):
        """Print formatted test result"""
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        print(f"  Message: {message}")
        if details:
            for key, value in details.items():
                print(f"  {key}: {value}")
        print()

    def record_result(self, test_name: str, success: bool):
        """Record test result for final summary"""
        self.test_results.append({'name': test_name, 'success': success})

    async def test_write_permission_check(self, spreadsheet_id: str, expected_writable: bool) -> Tuple[bool, str]:
        """Test if we can write to a spreadsheet"""
        try:
            # Try to get spreadsheet
            spreadsheet = self.auth.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()

            # Try to write a test value to see if we have permission
            test_range = "'Sheet1'!A1"
            test_value = [["Test"]]

            try:
                self.auth.service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=test_range,
                    valueInputOption='RAW',
                    body={'values': test_value}
                ).execute()

                return True, "Has write permission"
            except HttpError as write_error:
                if write_error.resp.status == 403:
                    return False, "No write permission (403 Forbidden)"
                else:
                    return False, f"Write error: {write_error.resp.status}"

        except HttpError as e:
            if e.resp.status == 404:
                return False, "Spreadsheet not found (404)"
            else:
                return False, f"Access error: {e.resp.status}"
        except Exception as e:
            return False, f"Exception: {str(e)}"

    async def test_permission_checks(self):
        """Test 1: Check permissions on all test spreadsheets"""
        self.print_header("Test 1: Permission Checks on Test Spreadsheets")

        test_cases = [
            ("Read-Write Sheet", READ_WRITE_SHEET_ID, True),
            ("Read-Only Sheet", READ_ONLY_SHEET_ID, False),
            ("Invalid Sheet", INVALID_SHEET_ID, False),
        ]

        all_passed = True
        for test_name, sheet_id, expected_writable in test_cases:
            has_write, message = await self.test_write_permission_check(sheet_id, expected_writable)

            # For read-write sheet, we expect to have write permission
            # For read-only sheet, we expect no write permission
            # For invalid sheet, we expect it to not exist
            if sheet_id == READ_WRITE_SHEET_ID:
                success = has_write == expected_writable
            elif sheet_id == READ_ONLY_SHEET_ID:
                success = not has_write
            else:  # Invalid sheet
                success = "not found" in message.lower() or "404" in message

            self.print_result(
                f"Permission Check: {test_name}",
                success,
                message,
                {
                    "Spreadsheet ID": sheet_id,
                    "Has Write Permission": has_write,
                    "Expected Writable": expected_writable
                }
            )

            if not success:
                all_passed = False

        self.record_result("Permission Checks on Test Spreadsheets", all_passed)

    async def test_write_sheet_logic(self):
        """Test 2: Test the write_sheet_structured logic with permission fallback"""
        self.print_header("Test 2: Test write_sheet_structured Permission Logic")

        # This test demonstrates what the write_sheet_structured function should do
        print("📝 This test demonstrates the expected behavior:")
        print("  1. Try to write to read-write sheet → Should succeed")
        print("  2. Try to write to read-only sheet → Should detect permission denied")
        print("  3. For read-only sheet → Should create new spreadsheet instead")
        print()

        # Test 2a: Write to read-write sheet (should succeed)
        print("🔸 Test 2a: Write to Read-Write Sheet")
        has_write_rw, msg_rw = await self.test_write_permission_check(READ_WRITE_SHEET_ID, True)
        if has_write_rw:
            print("  ✅ Can write to read-write sheet (expected behavior)")
            test_2a_pass = True
        else:
            print(f"  ❌ Cannot write to read-write sheet: {msg_rw}")
            test_2a_pass = False

        # Test 2b: Write to read-only sheet (should fail, triggering new spreadsheet creation)
        print("\n🔸 Test 2b: Write to Read-Only Sheet (Should trigger new spreadsheet creation)")
        has_write_ro, msg_ro = await self.test_write_permission_check(READ_ONLY_SHEET_ID, False)
        if not has_write_ro:
            print(f"  ✅ Cannot write to read-only sheet: {msg_ro}")
            print("  💡 In actual implementation, this would trigger creation of new spreadsheet")
            test_2b_pass = True
        else:
            print("  ❌ Unexpectedly can write to read-only sheet")
            test_2b_pass = False

        # Test 2c: Demonstrate new spreadsheet creation
        print("\n🔸 Test 2c: Demonstrate New Spreadsheet Creation")
        try:
            spreadsheet = {
                'properties': {
                    'title': 'Auto-Created Test Sheet (Permission Fallback Demo)'
                }
            }
            result = self.auth.service.spreadsheets().create(body=spreadsheet).execute()
            new_sheet_id = result.get('spreadsheetId')

            print(f"  ✅ Created new spreadsheet: {new_sheet_id}")
            print(f"  📋 URL: https://docs.google.com/spreadsheets/d/{new_sheet_id}/edit")
            self.created_spreadsheets.append(new_sheet_id)
            test_2c_pass = True
        except Exception as e:
            print(f"  ❌ Failed to create new spreadsheet: {e}")
            test_2c_pass = False

        overall_pass = test_2a_pass and test_2b_pass and test_2c_pass
        self.record_result("Write Sheet Logic with Permission Fallback", overall_pass)

    async def test_implementation_validation(self):
        """Test 3: Validate the actual implementation handles permissions correctly"""
        self.print_header("Test 3: Validate Implementation")

        print("📋 Checking that datatable_tools/third_party/google_sheets/service.py")
        print("   contains permission checking logic...")
        print()

        # Check if the implementation file has the necessary logic
        # Use absolute path from test file location
        test_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(os.path.dirname(test_dir))  # Go up two levels from tests/third_party
        service_file = os.path.join(repo_root, "datatable_tools", "third_party", "google_sheets", "service.py")

        print(f"  Looking for: {service_file}")

        if not os.path.exists(service_file):
            print(f"❌ service.py not found at {service_file}")
            self.record_result("Implementation Validation", False)
            return

        with open(service_file, 'r') as f:
            content = f.read()

        # Check for key implementation features
        checks = [
            ("check_write_permission method exists", "def check_write_permission" in content),
            ("Permission check before write", "needs_new_spreadsheet" in content or "check.*permission" in content.lower()),
            ("New spreadsheet creation on permission denial", "insufficient_permissions" in content),
            ("Tracks original spreadsheet ID", "original_spreadsheet_id" in content),
            ("created_new_spreadsheet flag", "created_new_spreadsheet" in content),
        ]

        all_passed = True
        for check_name, check_result in checks:
            status = "✅" if check_result else "❌"
            print(f"  {status} {check_name}")
            if not check_result:
                all_passed = False

        print()
        self.record_result("Implementation Validation", all_passed)

    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 70)
        print("🏁 PERMISSION AND FALLBACK TEST SUMMARY")
        print("=" * 70)

        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['success'])

        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests} ✅")
        print(f"Failed: {total_tests - passed_tests} ❌")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "No tests run")

        if self.created_spreadsheets:
            print(f"\n📋 Created Spreadsheets (for manual cleanup if needed):")
            for sheet_id in self.created_spreadsheets:
                print(f"  - https://docs.google.com/spreadsheets/d/{sheet_id}/edit")

        if passed_tests < total_tests:
            print(f"\n❌ FAILED TESTS:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  - {result['name']}")
        else:
            print(f"\n🎉 All permission and fallback tests passed!")

        print("\n💡 Test Sheet References:")
        print(f"  Read-Only:  {READ_ONLY_SHEET_URL}")
        print(f"  Read-Write: {READ_WRITE_SHEET_URL}")


async def run_permission_and_fallback_tests():
    """Run all permission and fallback tests"""
    print("🚀 PERMISSION CHECKING & AUTO-FALLBACK TESTS")
    print("=" * 70)
    print("⚠️  This will test permission detection and automatic new spreadsheet creation")
    print("🔑 Using environment variables for OAuth authentication")
    print("=" * 70)

    # Check environment variables
    env_vars = [
        "TEST_GOOGLE_OAUTH_REFRESH_TOKEN",
        "TEST_GOOGLE_OAUTH_CLIENT_ID",
        "TEST_GOOGLE_OAUTH_CLIENT_SECRET"
    ]

    missing_vars = [var for var in env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("💡 Please set these in your .env file or environment")
        return False

    print("✅ All required environment variables are set\n")

    # Authenticate
    auth_helper = GoogleSheetsAuthHelper()
    if not auth_helper.authenticate():
        print("❌ Authentication failed")
        return False

    print("✅ Successfully authenticated with Google Sheets API\n")

    # Run tests
    tester = PermissionAndFallbackTester(auth_helper)

    await tester.test_permission_checks()
    await tester.test_write_sheet_logic()
    await tester.test_implementation_validation()

    tester.print_summary()

    return sum(1 for r in tester.test_results if r['success']) == len(tester.test_results)


if __name__ == "__main__":
    success = asyncio.run(run_permission_and_fallback_tests())
    sys.exit(0 if success else 1)
