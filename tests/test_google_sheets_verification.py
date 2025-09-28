#!/usr/bin/env python3
"""
Real Google Sheets verification test for DataTable MCP
Based on: /Users/dengwei/work/ai/om3/api4/tests/tools/sheet/test_verify_permission.py
"""

import asyncio
import sys
import os
import logging
from pathlib import Path

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

# Google Sheets API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Test configuration - REAL Google Sheets IDs
READ_ONLY_SHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"  # Google Sheets sample data
READ_WRITE_SHEET_ID = "1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M"  # Read-write allowed sheet
INVALID_SHEET_ID = "invalid_sheet_id_that_does_not_exist"
TEST_USER_ID = "68501372a3569b6897673a48"  # Wade's user ID from existing test

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class SpreadsheetPermission:
    """Permission levels for spreadsheet access"""
    READ_WRITE = "READ_WRITE"
    READ_ONLY = "READ_ONLY"
    NOT_EXISTS = "NOT_EXISTS"
    NO_ACCESS = "NO_ACCESS"

class RealGoogleSheetsVerifier:
    """Real Google Sheets API verifier for actual spreadsheet operations"""

    def __init__(self):
        self.service = None
        self.creds = None
        self.test_results = []

    def authenticate(self):
        """Authenticate with Google Sheets API"""
        creds = None

        # Load existing credentials
        token_path = 'token.json'
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)

        # If there are no (valid) credentials available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"⚠️  Token refresh failed: {e}")
                    creds = None

            if not creds:
                credentials_path = 'credentials.json'
                if not os.path.exists(credentials_path):
                    print("❌ ERROR: credentials.json not found!")
                    print("📋 Please follow these steps to set up Google Sheets API:")
                    print("   1. Go to https://console.cloud.google.com/")
                    print("   2. Create a new project or select existing one")
                    print("   3. Enable Google Sheets API")
                    print("   4. Create credentials (OAuth 2.0 client ID)")
                    print("   5. Download the credentials.json file")
                    print("   6. Place it in the project root directory")
                    return False

                try:
                    flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                    creds = flow.run_local_server(port=0)
                except Exception as e:
                    print(f"❌ Authentication failed: {e}")
                    return False

            # Save the credentials for the next run
            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        try:
            self.service = build('sheets', 'v4', credentials=creds)
            self.creds = creds
            print("✅ Successfully authenticated with Google Sheets API")
            return True
        except Exception as e:
            print(f"❌ Failed to build Google Sheets service: {e}")
            return False

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

    async def check_spreadsheet_permission(self, spreadsheet_id: str):
        """Check what permissions we have on a spreadsheet"""
        if not self.service:
            return SpreadsheetPermission.NO_ACCESS, "Not authenticated"

        try:
            # Try to get spreadsheet metadata (read access)
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

            # If we can read, try to write (test write permission)
            try:
                # Try to read existing data first
                values_result = self.service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range='A1:A1'
                ).execute()

                # Try a minimal write operation to test write permissions
                test_write_body = {
                    'values': [['TEST_WRITE_CHECK']]
                }

                # Attempt to write to a test cell
                self.service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range='Z1000',  # Use a cell that's unlikely to contain important data
                    valueInputOption='RAW',
                    body=test_write_body
                ).execute()

                # Clear the test write
                self.service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range='Z1000'
                ).execute()

                return SpreadsheetPermission.READ_WRITE, f"Full access to '{spreadsheet.get('properties', {}).get('title', 'Unknown')}'"

            except HttpError as write_error:
                if write_error.resp.status == 403:
                    return SpreadsheetPermission.READ_ONLY, f"Read-only access to '{spreadsheet.get('properties', {}).get('title', 'Unknown')}'"
                else:
                    return SpreadsheetPermission.READ_ONLY, f"Read access with write error: {write_error}"

        except HttpError as e:
            if e.resp.status == 404:
                return SpreadsheetPermission.NOT_EXISTS, "Spreadsheet not found or no access"
            elif e.resp.status == 403:
                return SpreadsheetPermission.NO_ACCESS, "Access denied"
            else:
                return SpreadsheetPermission.NO_ACCESS, f"Error {e.resp.status}: {e}"
        except Exception as e:
            return SpreadsheetPermission.NO_ACCESS, f"Unexpected error: {e}"

    async def test_read_data(self, spreadsheet_id: str, sheet_name: str = "Sheet1"):
        """Test reading data from a real spreadsheet"""
        if not self.service:
            return False, "Not authenticated"

        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1:Z10'  # Read first 10 rows, all columns
            ).execute()

            values = result.get('values', [])

            if values:
                return True, f"Successfully read {len(values)} rows. Sample: {values[0][:3] if values[0] else 'Empty row'}"
            else:
                return True, "Spreadsheet is empty but accessible"

        except HttpError as e:
            return False, f"HTTP Error {e.resp.status}: {e}"
        except Exception as e:
            return False, f"Error: {e}"

    async def test_write_data(self, spreadsheet_id: str, sheet_name: str = "Sheet1"):
        """Test writing data to a real spreadsheet"""
        if not self.service:
            return False, "Not authenticated"

        try:
            # Write test data to a safe location
            test_data = [['DataTable MCP Test', f'Timestamp: {asyncio.get_event_loop().time()}']]

            body = {
                'values': test_data
            }

            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1:B1',
                valueInputOption='RAW',
                body=body
            ).execute()

            updated_cells = result.get('updatedCells', 0)
            return True, f"Successfully wrote {updated_cells} cells"

        except HttpError as e:
            if e.resp.status == 403:
                return False, "Write permission denied"
            else:
                return False, f"HTTP Error {e.resp.status}: {e}"
        except Exception as e:
            return False, f"Error: {e}"

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
                    expected_permission, permission, message, sheet_url
                )
                self.record_result(test_name, success, expected_permission, permission)

            except Exception as e:
                sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit" if sheet_id != INVALID_SHEET_ID else None
                self.print_result(
                    test_name, False,
                    expected_permission, f"ERROR: {type(e).__name__}", str(e), sheet_url
                )
                self.record_result(test_name, False, expected_permission, f"ERROR: {str(e)}")

    async def test_real_operations(self):
        """Test actual read/write operations on real spreadsheets"""
        self.print_header("REAL Google Sheets Operations Tests")

        # Test read-write sheet
        print("🧪 Testing Read-Write Sheet Operations")
        print(f"📋 URL: https://docs.google.com/spreadsheets/d/{READ_WRITE_SHEET_ID}/edit")

        # Test read
        read_success, read_message = await self.test_read_data(READ_WRITE_SHEET_ID)
        status = "✅ PASS" if read_success else "❌ FAIL"
        print(f"  {status} Read Test: {read_message}")

        # Test write
        write_success, write_message = await self.test_write_data(READ_WRITE_SHEET_ID)
        status = "✅ PASS" if write_success else "❌ FAIL"
        print(f"  {status} Write Test: {write_message}")
        print()

        # Test read-only sheet
        print("🧪 Testing Read-Only Sheet Operations")
        print(f"📋 URL: https://docs.google.com/spreadsheets/d/{READ_ONLY_SHEET_ID}/edit")

        # Test read
        read_success, read_message = await self.test_read_data(READ_ONLY_SHEET_ID)
        status = "✅ PASS" if read_success else "❌ FAIL"
        print(f"  {status} Read Test: {read_message}")

        # Test write (should fail)
        write_success, write_message = await self.test_write_data(READ_ONLY_SHEET_ID)
        expected_fail = not write_success  # We expect this to fail
        status = "✅ PASS" if expected_fail else "❌ FAIL"
        print(f"  {status} Write Test (Expected Fail): {write_message}")
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

    # Authenticate
    if not verifier.authenticate():
        print("❌ Authentication failed. Cannot proceed with real Google Sheets tests.")
        return False

    # Run permission tests
    await verifier.test_permission_verification()

    # Run actual operations tests
    await verifier.test_real_operations()

    # Print summary
    verifier.print_summary()

    return True


if __name__ == "__main__":
    print("🚀 REAL Google Sheets Verification Test")
    print("📋 This will test ACTUAL Google Sheets permissions and operations")
    print("🔑 Make sure you have credentials.json file in the project root")
    print("=" * 70)

    success = asyncio.run(run_real_google_sheets_verification())

    if success:
        print("\n🎉 Real Google Sheets verification completed!")
    else:
        print("\n⚠️  Verification failed. Check authentication and setup.")

    sys.exit(0 if success else 1)