#!/usr/bin/env python3
"""
Test script for load_data_table_with_formulas tool - loads formulas instead of calculated values
"""

import asyncio
import os
import sys
import logging

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_dir)

# Google Sheets API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Import the GoogleSheetDataTable class
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def authenticate():
    """Authenticate with Google Sheets API using environment variables"""
    # Get OAuth credentials from environment variables
    refresh_token = os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
    client_id = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")

    if not all([refresh_token, client_id, client_secret]):
        print("❌ ERROR: Missing required environment variables!")
        print("📋 Please set the following environment variables:")
        print("   - TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
        print("   - TEST_GOOGLE_OAUTH_CLIENT_ID")
        print("   - TEST_GOOGLE_OAUTH_CLIENT_SECRET")
        return None

    try:
        # Create credentials from environment variables
        creds = Credentials(
            token=None,  # Will be refreshed
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES
        )

        # Refresh the token
        creds.refresh(Request())

        # Build the service
        service = build('sheets', 'v4', credentials=creds)
        print("✅ Successfully authenticated with Google Sheets API")
        return service

    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return None


async def test_compare_values_and_formulas():
    """Compare regular values vs formulas from the same sheet"""
    print("\n" + "=" * 60)
    print("🧪 Testing load_data_table vs load_data_table_with_formulas")
    print("=" * 60)

    # Authenticate
    service = authenticate()
    if not service:
        return

    # Test spreadsheet URI - you can change this to any Google Sheets URL with formulas
    test_uri = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit"

    print(f"\n📄 Testing with URI: {test_uri}")

    try:
        # Create GoogleSheetDataTable instance
        google_sheet = GoogleSheetDataTable()

        # Load regular values
        print("\n🔍 Loading regular values (calculated)...")
        values_result = await google_sheet.load_data_table(service, test_uri)

        # Load formulas
        print("🔍 Loading formulas (raw)...")
        formulas_result = await google_sheet.load_data_table_with_formulas(service, test_uri)

        # Display comparison
        print("\n" + "=" * 60)
        print("📊 Comparison Results:")
        print("=" * 60)

        print(f"\n🔢 Regular Values:")
        print(f"  Success: {values_result.success}")
        print(f"  Table ID: {values_result.table_id}")
        print(f"  Shape: {values_result.shape}")
        print(f"  Message: {values_result.message}")

        print(f"\n📝 Formulas:")
        print(f"  Success: {formulas_result.success}")
        print(f"  Table ID: {formulas_result.table_id}")
        print(f"  Shape: {formulas_result.shape}")
        print(f"  Message: {formulas_result.message}")
        print(f"  Value Render Option: {formulas_result.source_info.get('value_render_option', 'N/A')}")

        # Show first 5 rows of data comparison
        if values_result.success and formulas_result.success:
            print("\n" + "=" * 60)
            print("📋 Data Comparison (First 5 rows):")
            print("=" * 60)

            max_rows = min(5, len(values_result.data), len(formulas_result.data))

            for i in range(max_rows):
                print(f"\nRow {i+1}:")
                print("-" * 60)

                value_row = values_result.data[i]
                formula_row = formulas_result.data[i]

                # Get all column names
                all_columns = set(value_row.keys()) | set(formula_row.keys())

                for col in sorted(all_columns):
                    value = value_row.get(col, "")
                    formula = formula_row.get(col, "")

                    # Check if there's a difference (indicating a formula)
                    if value != formula and formula.startswith("="):
                        print(f"  {col}:")
                        print(f"    Value:   {value}")
                        print(f"    Formula: {formula} ⚡")
                    else:
                        # No formula, just show the value
                        print(f"  {col}: {value}")

        # Metadata comparison
        print("\n" + "=" * 60)
        print("📦 Metadata Comparison:")
        print("=" * 60)
        print(f"\nValues metadata:")
        for key, value in values_result.source_info.items():
            print(f"  {key}: {value}")

        print(f"\nFormulas metadata:")
        for key, value in formulas_result.source_info.items():
            if key not in values_result.source_info or value != values_result.source_info[key]:
                print(f"  {key}: {value} ⚡")
            else:
                print(f"  {key}: {value}")

        print("\n" + "=" * 60)
        if values_result.success and formulas_result.success:
            print("✅ Test completed successfully!")
            print("💡 Cells with formulas are marked with ⚡")
        else:
            print("❌ Test failed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()


async def test_formula_only():
    """Test loading formulas alone"""
    print("\n" + "=" * 60)
    print("🧪 Testing load_data_table_with_formulas standalone")
    print("=" * 60)

    # Authenticate
    service = authenticate()
    if not service:
        return

    # Test spreadsheet URI
    test_uri = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit"

    print(f"\n📄 Testing with URI: {test_uri}")

    try:
        # Create GoogleSheetDataTable instance
        google_sheet = GoogleSheetDataTable()

        # Load formulas
        print("\n🔍 Loading formulas...")
        result = await google_sheet.load_data_table_with_formulas(service, test_uri)

        # Display results
        print("\n" + "=" * 60)
        print("📊 Results:")
        print("=" * 60)
        print(f"Success: {result.success}")
        print(f"Table ID: {result.table_id}")
        print(f"Name: {result.name}")
        print(f"Shape: {result.shape}")
        print(f"Message: {result.message}")

        if result.error:
            print(f"Error: {result.error}")

        if result.success and result.data:
            print("\n📋 Formula Data (First 3 rows):")
            print("-" * 60)
            for i, row in enumerate(result.data[:3], 1):
                print(f"\nRow {i}:")
                for col, value in row.items():
                    if isinstance(value, str) and value.startswith("="):
                        print(f"  {col}: {value} (formula)")
                    else:
                        print(f"  {col}: {value}")

        print("\n" + "=" * 60)
        if result.success:
            print("✅ Test completed successfully!")
        else:
            print("❌ Test failed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("\n🚀 Starting load_data_table_with_formulas tests")

    # Run both tests
    asyncio.run(test_compare_values_and_formulas())
    asyncio.run(test_formula_only())

    print("\n✨ All tests completed!")
