"""
Test Google Sheets service with MongoDB credentials
"""

import asyncio
from datatable_tools.third_party.google_sheets.service import GoogleSheetsService
import os
print(os.getenv("PLAY__MONGO_URI"))

async def test_google_sheets_mongodb_auth():
    """Test Google Sheets service with MongoDB credential lookup"""
    service = GoogleSheetsService()

    # Test user ID - Wade's user ID from existing tests
    TEST_USER_ID = "68501372a3569b6897673a48"
    user_id = TEST_USER_ID

    # Real spreadsheet IDs from previous tests
    read_write_id = "1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M"
    read_only_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"

    try:
        print(f"\n🔍 Testing Google Sheets MongoDB Auth for user: {user_id}")

        # Test reading from read-only spreadsheet
        print(f"📘 Reading from: https://docs.google.com/spreadsheets/d/{read_only_id}/edit")
        data = await service.read_sheet(user_id, read_only_id, "Class Data")
        print(f"✅ Read {len(data)} rows from Class Data sheet")
        if data:
            print(f"   Headers: {data[0]}")
            print(f"   First row: {data[1] if len(data) > 1 else 'No data rows'}")

        # Test range reading
        print(f"📊 Reading range A1:E5")
        range_data = await service.get_range_values(user_id, read_only_id, "Class Data!A1:E5")
        print(f"✅ Read range data: {len(range_data)} rows")

        # Test writing (if credentials allow)
        print(f"📗 Testing write to: https://docs.google.com/spreadsheets/d/{read_write_id}/edit")
        test_data = [
            ["Test", "Data", "From"],
            ["MongoDB", "Auth", "Service"],
            ["Row 3", "Col 2", "Col 3"]
        ]
        TEST_WORKSHEET = "test-worksheet"
        await service.write_sheet(user_id, read_write_id, test_data, TEST_WORKSHEET)
        print("✅ Successfully wrote test data")

        # Verify the write by reading back
        written_data = await service.read_sheet(user_id, read_write_id, TEST_WORKSHEET)
        print(f"✅ Verified write: read back {len(written_data)} rows")

        # Test range update
        update_data = [["Updated", "Via"], ["Range", "API"]]
        await service.update_range(user_id, read_write_id, f"{TEST_WORKSHEET}!A4:B5", update_data)
        print("✅ Successfully updated range A4:B5")

        print("\n🎉 All Google Sheets MongoDB auth tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(test_google_sheets_mongodb_auth())