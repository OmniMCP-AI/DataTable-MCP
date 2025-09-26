#!/usr/bin/env python3
"""
Real Google Sheets integration test - writes to actual Google Sheets
"""

import asyncio
import os
import sys
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

# Test configuration
TEST_USER_ID = "68501372a3569b6897673a48"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class RealGoogleSheetsClient:
    """Real Google Sheets API client for actual spreadsheet operations"""

    def __init__(self):
        self.service = None
        self.creds = None

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

    async def create_spreadsheet(self, title="DataTable MCP Test"):
        """Create a new Google Sheets spreadsheet"""
        if not self.service:
            print("❌ Not authenticated. Call authenticate() first.")
            return None

        try:
            spreadsheet = {
                'properties': {
                    'title': title
                }
            }

            result = self.service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
            spreadsheet_id = result.get('spreadsheetId')

            print(f"✅ Created new Google Sheets spreadsheet:")
            print(f"   - Title: {title}")
            print(f"   - ID: {spreadsheet_id}")
            print(f"   - URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")

            return spreadsheet_id

        except HttpError as e:
            print(f"❌ Error creating spreadsheet: {e}")
            return None

    async def write_data(self, spreadsheet_id, worksheet_name, data, start_cell="A1"):
        """Write data to a Google Sheets spreadsheet"""
        if not self.service:
            print("❌ Not authenticated. Call authenticate() first.")
            return False

        try:
            # Prepare the range
            range_name = f"{worksheet_name}!{start_cell}"

            # Prepare the body
            body = {
                'values': data
            }

            # Execute the update
            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

            updated_cells = result.get('updatedCells')
            updated_range = result.get('updatedRange')

            print(f"✅ Successfully wrote data to Google Sheets:")
            print(f"   - Spreadsheet ID: {spreadsheet_id}")
            print(f"   - Range: {updated_range}")
            print(f"   - Updated cells: {updated_cells}")
            print(f"   - URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")

            return True

        except HttpError as e:
            print(f"❌ Error writing data: {e}")
            return False

    async def read_data(self, spreadsheet_id, worksheet_name, range_name="A1:Z1000"):
        """Read data from a Google Sheets spreadsheet"""
        if not self.service:
            print("❌ Not authenticated. Call authenticate() first.")
            return None

        try:
            # Prepare the range
            full_range = f"{worksheet_name}!{range_name}"

            # Execute the read
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=full_range
            ).execute()

            values = result.get('values', [])

            print(f"✅ Successfully read data from Google Sheets:")
            print(f"   - Spreadsheet ID: {spreadsheet_id}")
            print(f"   - Range: {full_range}")
            print(f"   - Rows read: {len(values)}")
            if values:
                print(f"   - Sample data: {values[0]}")
            print(f"   - URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")

            return values

        except HttpError as e:
            print(f"❌ Error reading data: {e}")
            return None


async def test_create_real_spreadsheet():
    """Test creating a real Google Sheets spreadsheet"""
    print("🧪 Test 1: Creating a REAL Google Sheets spreadsheet")
    print("=" * 60)

    client = RealGoogleSheetsClient()

    # Authenticate
    if not client.authenticate():
        print("❌ Authentication failed. Cannot proceed with real Google Sheets test.")
        return None

    # Create spreadsheet
    spreadsheet_id = await client.create_spreadsheet("DataTable MCP Real Test")

    return spreadsheet_id, client


async def test_write_real_data(spreadsheet_id, client):
    """Test writing real data to Google Sheets"""
    if not spreadsheet_id or not client:
        print("\n⏭️  Skipping write test - no spreadsheet or client available")
        return False

    print(f"\n🧪 Test 2: Writing REAL data to Google Sheets {spreadsheet_id}")
    print("=" * 60)

    # Sample employee data
    employee_data = [
        ["Name", "Age", "Department", "Salary", "Start Date"],
        ["Alice Johnson", "28", "Engineering", "$95,000", "2023-01-15"],
        ["Bob Smith", "32", "Marketing", "$78,000", "2022-03-10"],
        ["Carol Davis", "29", "Design", "$85,000", "2023-06-01"],
        ["David Wilson", "35", "Sales", "$92,000", "2021-11-20"],
        ["Eve Chen", "26", "Engineering", "$88,000", "2024-02-01"]
    ]

    success = await client.write_data(spreadsheet_id, "Sheet1", employee_data)

    if success:
        print("🎉 SUCCESS: Data written to REAL Google Sheets!")
        return True
    else:
        print("❌ FAILED: Could not write data to Google Sheets")
        return False


async def test_read_real_data(spreadsheet_id, client):
    """Test reading real data from Google Sheets"""
    if not spreadsheet_id or not client:
        print("\n⏭️  Skipping read test - no spreadsheet or client available")
        return False

    print(f"\n🧪 Test 3: Reading REAL data from Google Sheets {spreadsheet_id}")
    print("=" * 60)

    values = await client.read_data(spreadsheet_id, "Sheet1", "A1:E10")

    if values:
        print("🎉 SUCCESS: Data read from REAL Google Sheets!")
        print(f"   - Headers: {values[0] if values else 'None'}")
        print(f"   - Total rows: {len(values)}")
        if len(values) > 1:
            print(f"   - Sample row: {values[1]}")
        return True
    else:
        print("❌ FAILED: Could not read data from Google Sheets")
        return False


async def test_write_additional_worksheet(spreadsheet_id, client):
    """Test writing data to a new worksheet"""
    if not spreadsheet_id or not client:
        print("\n⏭️  Skipping additional worksheet test - no spreadsheet or client available")
        return False

    print(f"\n🧪 Test 4: Creating new worksheet and writing data")
    print("=" * 60)

    try:
        # Add a new worksheet
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': 'Department_Summary'
                    }
                }
            }]
        }

        client.service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

        # Department summary data
        dept_data = [
            ["Department", "Employees", "Avg Salary", "Budget"],
            ["Engineering", "2", "$91,500", "$183,000"],
            ["Marketing", "1", "$78,000", "$78,000"],
            ["Design", "1", "$85,000", "$85,000"],
            ["Sales", "1", "$92,000", "$92,000"]
        ]

        success = await client.write_data(spreadsheet_id, "Department_Summary", dept_data)

        if success:
            print("🎉 SUCCESS: New worksheet created and data written!")
            print(f"   - 📋 View worksheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid=1")
            return True
        else:
            return False

    except Exception as e:
        print(f"❌ Error creating worksheet: {e}")
        return False


async def run_real_google_sheets_test():
    """Run real Google Sheets integration test"""
    print("🚀 REAL Google Sheets Integration Test")
    print("=" * 70)
    print("⚠️  This will create and write to an ACTUAL Google Sheets spreadsheet!")
    print("🔑 Make sure you have credentials.json file in the project root")
    print("=" * 70)

    # Test 1: Create real spreadsheet
    result = await test_create_real_spreadsheet()
    if not result:
        print("❌ Cannot proceed without authentication")
        return False

    spreadsheet_id, client = result

    # Test 2: Write real data
    write_success = await test_write_real_data(spreadsheet_id, client)

    # Test 3: Read real data
    read_success = await test_read_real_data(spreadsheet_id, client)

    # Test 4: Additional worksheet
    worksheet_success = await test_write_additional_worksheet(spreadsheet_id, client)

    print("\n🏁 Real Google Sheets tests completed!")
    print("=" * 70)

    # Summary
    tests = [
        ("Create Spreadsheet", spreadsheet_id is not None),
        ("Write Data", write_success),
        ("Read Data", read_success),
        ("Additional Worksheet", worksheet_success)
    ]

    passed = sum(1 for _, success in tests if success)
    total = len(tests)

    print(f"\n📊 RESULTS: {passed}/{total} tests passed")
    for test_name, success in tests:
        status = "✅" if success else "❌"
        print(f"  {status} {test_name}")

    if spreadsheet_id:
        print(f"\n🌐 YOUR REAL GOOGLE SHEETS SPREADSHEET:")
        print(f"📋 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")
        print(f"📋 ID: {spreadsheet_id}")
        print("💡 Click the URL above to view your actual spreadsheet!")

    return passed == total


if __name__ == "__main__":
    success = asyncio.run(run_real_google_sheets_test())
    if success:
        print("\n🎉 All tests passed! Real Google Sheets integration working.")
    else:
        print("\n⚠️  Some tests failed. Check authentication and permissions.")
    sys.exit(0 if success else 1)