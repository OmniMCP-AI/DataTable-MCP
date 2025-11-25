#!/usr/bin/env python
"""Integration tests for copy_range_with_formulas

Tests the complete workflow of copying ranges with formula adaptation
in Google Sheets.

Usage:
    python tests/test_copy_range_integration.py --env=test
"""

import os
import sys
import argparse
import asyncio

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import modules directly to avoid server initialization
import importlib.util

# Load GoogleSheetDataTable without triggering __init__.py
spec = importlib.util.spec_from_file_location(
    "google_sheets_datatable",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 'datatable_tools', 'third_party', 'google_sheets', 'datatable.py')
)
google_sheets_module = importlib.util.module_from_spec(spec)

# Need to set up required modules first
import importlib
sys.modules['datatable_tools.interfaces.datatable'] = importlib.import_module('datatable_tools.interfaces.datatable')
sys.modules['datatable_tools.models'] = importlib.import_module('datatable_tools.models')
sys.modules['datatable_tools.google_sheets_helpers'] = importlib.import_module('datatable_tools.google_sheets_helpers')

# Now load the module
spec.loader.exec_module(google_sheets_module)
GoogleSheetDataTable = google_sheets_module.GoogleSheetDataTable

# Import auth service (this doesn't trigger server)
from datatable_tools.auth.google_auth import GoogleAuthService


async def setup_test_sheet(service):
    """Create a test sheet with formulas for testing

    Returns:
        (spreadsheet_id, gid, uri)
    """
    google_sheet = GoogleSheetDataTable()

    # Create test sheet with formulas
    test_data = [
        ["Item", "Price", "Quantity", "Total", "Tax", "Final"],
        ["Apple", 1.50, 10, "=B2*C2", "=D2*0.1", "=D2+E2"],
        ["Banana", 0.75, 20, "=B3*C3", "=D3*0.1", "=D3+E3"],
        ["Orange", 2.00, 15, "=B4*C4", "=D4*0.1", "=D4+E4"],
    ]

    result = await google_sheet.write_new_sheet(
        service,
        data=test_data,
        sheet_name="Formula Copy Test"
    )

    if not result.success:
        raise Exception(f"Failed to create test sheet: {result.error}")

    # Extract spreadsheet ID from URL
    spreadsheet_id = result.spreadsheet_url.split("/d/")[1].split("/")[0]
    gid = result.spreadsheet_url.split("gid=")[1] if "gid=" in result.spreadsheet_url else "0"
    uri = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid={gid}"

    print(f"\n✅ Created test sheet: {uri}")
    print(f"   Spreadsheet ID: {spreadsheet_id}")
    print(f"   GID: {gid}")

    return spreadsheet_id, gid, uri


async def test_copy_row_with_formulas(service, uri):
    """Test copying a row with formulas"""
    print("\n" + "="*80)
    print("TEST: Copy Row with Formulas")
    print("="*80)

    google_sheet = GoogleSheetDataTable()

    # Copy row 2 to row 5 (formulas should adapt)
    result = await google_sheet.copy_range_with_formulas(
        service,
        uri=uri,
        from_range="A2:F2",
        to_range="A5:F5"
    )

    if result.success:
        print(f"\n✅ Successfully copied row 2 to row 5")
        print(f"   Updated cells: {result.updated_cells}")
        print(f"   Range: {result.range}")
        print(f"   Message: {result.message}")

        # Read back the formulas to verify
        read_result = await google_sheet.read_worksheet_with_formulas(service, uri)
        if read_result.success:
            print(f"\n   Verification - Row 2 (original):")
            row2 = read_result.data[1] if len(read_result.data) > 1 else {}
            print(f"     Total (D2): {row2.get('Total', 'N/A')}")
            print(f"     Tax (E2): {row2.get('Tax', 'N/A')}")
            print(f"     Final (F2): {row2.get('Final', 'N/A')}")

            print(f"\n   Verification - Row 5 (copied):")
            if len(read_result.data) > 4:
                row5 = read_result.data[4]
                print(f"     Total (D5): {row5.get('Total', 'N/A')}")
                print(f"     Tax (E5): {row5.get('Tax', 'N/A')}")
                print(f"     Final (F5): {row5.get('Final', 'N/A')}")
            else:
                print(f"     ⚠️  Row 5 not found in data")
        return True
    else:
        print(f"\n❌ Failed to copy row: {result.error}")
        return False


async def test_copy_column_with_formulas(service, uri):
    """Test copying a column with formulas"""
    print("\n" + "="*80)
    print("TEST: Copy Column with Formulas")
    print("="*80)

    google_sheet = GoogleSheetDataTable()

    # Copy column D (Total) to column G
    result = await google_sheet.copy_range_with_formulas(
        service,
        uri=uri,
        from_range="D1:D4",
        to_range="G1:G4"
    )

    if result.success:
        print(f"\n✅ Successfully copied column D to column G")
        print(f"   Updated cells: {result.updated_cells}")
        print(f"   Range: {result.range}")
        print(f"   Message: {result.message}")

        # Read back to verify
        read_result = await google_sheet.read_worksheet_with_formulas(service, uri)
        if read_result.success and len(read_result.data) > 1:
            print(f"\n   Verification:")
            print(f"     D2 (original): {read_result.data[1].get('Total', 'N/A')}")
            # Column G might not have a header yet, so check by index
            headers = list(read_result.data[0].keys())
            if len(headers) > 6:
                col_g_header = headers[6]
                print(f"     G2 (copied): {read_result.data[1].get(col_g_header, 'N/A')}")
        return True
    else:
        print(f"\n❌ Failed to copy column: {result.error}")
        return False


async def test_dimension_mismatch_error(service, uri):
    """Test that dimension mismatch raises an error"""
    print("\n" + "="*80)
    print("TEST: Dimension Mismatch Error")
    print("="*80)

    google_sheet = GoogleSheetDataTable()

    # Try to copy 1x6 range to 1x3 range (should fail)
    result = await google_sheet.copy_range_with_formulas(
        service,
        uri=uri,
        from_range="A2:F2",  # 1 row x 6 cols
        to_range="A6:C6"    # 1 row x 3 cols (mismatch!)
    )

    if not result.success:
        print(f"\n✅ Correctly rejected dimension mismatch")
        print(f"   Error message: {result.error}")
        return True
    else:
        print(f"\n❌ Should have failed but succeeded")
        return False


async def test_auto_expand_grid(service, uri):
    """Test that grid auto-expands when copying beyond current bounds"""
    print("\n" + "="*80)
    print("TEST: Auto-Expand Grid")
    print("="*80)

    google_sheet = GoogleSheetDataTable()

    # Copy to a range that likely exceeds initial grid (row 100)
    result = await google_sheet.copy_range_with_formulas(
        service,
        uri=uri,
        from_range="A2:F2",
        to_range="A100:F100"
    )

    if result.success:
        print(f"\n✅ Successfully copied to row 100 (grid auto-expanded)")
        print(f"   Updated cells: {result.updated_cells}")
        print(f"   Range: {result.range}")
        return True
    else:
        print(f"\n❌ Failed to copy with auto-expand: {result.error}")
        return False


async def main():
    parser = argparse.ArgumentParser(description='Integration tests for copy_range_with_formulas')
    parser.add_argument('--env', choices=['local', 'test'], default='test',
                      help='Environment: local or test')
    args = parser.parse_args()

    # Get OAuth token
    token_env_var = {
        'local': 'GOOGLE_OAUTH_REFRESH_TOKEN',
        'test': 'TEST_GOOGLE_OAUTH_REFRESH_TOKEN'
    }[args.env]

    refresh_token = os.environ.get(token_env_var)
    if not refresh_token:
        print(f"❌ Error: {token_env_var} environment variable not set")
        return 1

    # Initialize Google Sheets service
    auth_service = GoogleAuthService()
    service = await auth_service.get_service_with_refresh_token(
        refresh_token,
        "sheets",
        "sheets_write"
    )

    print(f"\n{'='*80}")
    print("INTEGRATION TESTS: copy_range_with_formulas")
    print(f"{'='*80}")
    print(f"Environment: {args.env}")

    try:
        # Setup test sheet
        spreadsheet_id, gid, uri = await setup_test_sheet(service)

        # Run tests
        results = []
        results.append(await test_copy_row_with_formulas(service, uri))
        results.append(await test_copy_column_with_formulas(service, uri))
        results.append(await test_dimension_mismatch_error(service, uri))
        results.append(await test_auto_expand_grid(service, uri))

        # Summary
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        passed = sum(results)
        total = len(results)
        print(f"\nPassed: {passed}/{total}")

        if passed == total:
            print("\n✅ All tests passed!")
            print(f"\n📊 View test sheet: {uri}")
            return 0
        else:
            print(f"\n❌ {total - passed} test(s) failed")
            return 1

    except Exception as e:
        print(f"\n❌ Test setup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
