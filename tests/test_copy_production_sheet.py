"""
Copy the complex spreadsheet with all worksheets using Drive API
Spreadsheet: https://docs.google.com/spreadsheets/d/1j2IBkos9yYP5dWrEhhgGQjOC1QHCnsz2PS8g9DRtno8
"""

from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession
import asyncio
import json
import os
from dotenv import load_dotenv

load_dotenv()

LOCAL_URL = "http://127.0.0.1:8321/mcp"
TARGET_SHEET = "https://docs.google.com/spreadsheets/d/1j2IBkos9yYP5dWrEhhgGQjOC1QHCnsz2PS8g9DRtno8/edit?gid=635365660#gid=635365660"

async def copy_complex_sheet():
    """Copy the complex spreadsheet and display URLs for manual verification"""

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN"),
        "GOOGLE_OAUTH_CLIENT_ID": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")
    }

    print("=" * 80)
    print("🧪 Copying Complex Spreadsheet with Drive API")
    print("=" * 80)
    print(f"\n📝 Source spreadsheet:")
    print(f"   {TARGET_SHEET}")
    print(f"\n🔄 Using Google Drive API files().copy()...")
    print(f"   This will copy ALL worksheets and preserve ALL formulas")

    async with streamablehttp_client(url=LOCAL_URL, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Copy the spreadsheet
            copy_result = await session.call_tool("copy_sheet", {
                "uri": TARGET_SHEET
            })

            if copy_result.isError:
                print(f"\n❌ Copy operation failed!")
                print(f"   Error: {copy_result}")
                print(f"\n💡 This likely means the spreadsheet is not accessible.")
                print(f"   Please share the spreadsheet with your OAuth account:")
                print(f"   1. Open: {TARGET_SHEET}")
                print(f"   2. Click 'Share' button")
                print(f"   3. Share with the Google account associated with TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
                print(f"      OR make it 'Anyone with the link can view'")
                return

            copy_content = json.loads(copy_result.content[0].text)

            if not copy_content.get('success'):
                print(f"\n❌ Copy failed!")
                print(f"   Message: {copy_content.get('message')}")
                print(f"   Error: {copy_content.get('error')}")

                # Check if it's a permission/access error
                error_msg = str(copy_content.get('error', ''))
                if '404' in error_msg or 'not found' in error_msg.lower():
                    print(f"\n💡 The spreadsheet is not shared with your OAuth account.")
                    print(f"   Please share the spreadsheet:")
                    print(f"   1. Open: https://docs.google.com/spreadsheets/d/1j2IBkos9yYP5dWrEhhgGQjOC1QHCnsz2PS8g9DRtno8")
                    print(f"   2. Click 'Share' button (top right)")
                    print(f"   3. Add the email associated with your TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
                    print(f"      OR click 'Change' under 'General access' and select 'Anyone with the link'")
                return

            # Success! Display results
            print(f"\n✅ ✅ ✅ COPY SUCCESSFUL! ✅ ✅ ✅")
            print(f"\n{'=' * 80}")
            print(f"📊 ORIGINAL SPREADSHEET")
            print(f"{'=' * 80}")
            print(f"   Title: {copy_content.get('original_spreadsheet_title')}")
            print(f"   ID:    {copy_content.get('original_spreadsheet_id')}")
            print(f"   URL:   {copy_content.get('original_spreadsheet_url')}")

            print(f"\n{'=' * 80}")
            print(f"📊 COPIED SPREADSHEET (via Drive API)")
            print(f"{'=' * 80}")
            print(f"   Title: {copy_content.get('new_spreadsheet_title')}")
            print(f"   ID:    {copy_content.get('new_spreadsheet_id')}")
            print(f"   URL:   {copy_content.get('new_spreadsheet_url')}")

            new_url = copy_content.get('new_spreadsheet_url')

            # List worksheets in the copy
            print(f"\n{'=' * 80}")
            print(f"📋 LISTING WORKSHEETS IN COPIED SPREADSHEET")
            print(f"{'=' * 80}")

            list_result = await session.call_tool("list_worksheets", {
                "uri": new_url
            })

            if not list_result.isError:
                list_content = json.loads(list_result.content[0].text)
                if list_content.get('success'):
                    worksheets = list_content.get('worksheets', [])
                    print(f"   Total worksheets: {len(worksheets)}\n")
                    for i, ws in enumerate(worksheets, 1):
                        print(f"   {i}. {ws.get('title')}")
                        print(f"      - Dimensions: {ws.get('row_count')} rows × {ws.get('column_count')} columns")
                        print(f"      - GID: {ws.get('sheet_id')}")
                        print(f"      - URL: {ws.get('worksheet_url')}")
                        print()

            print(f"{'=' * 80}")
            print(f"✅ MANUAL VERIFICATION STEPS")
            print(f"{'=' * 80}")
            print(f"\n1️⃣  Open ORIGINAL spreadsheet in browser:")
            print(f"   {copy_content.get('original_spreadsheet_url')}")
            print(f"\n2️⃣  Open COPIED spreadsheet in NEW browser tab:")
            print(f"   {copy_content.get('new_spreadsheet_url')}")
            print(f"\n3️⃣  Compare the two spreadsheets:")
            print(f"   ✓ Check all worksheets are present")
            print(f"   ✓ Check 'gen-video-split-image-orange-assemble' worksheet")
            print(f"   ✓ Click on cell C2 and verify formula bar shows: =generate_video_shots!$AA2")
            print(f"   ✓ Check formatting (colors, fonts, borders) is identical")
            print(f"   ✓ Check images and charts are present")
            print(f"   ✓ Check data validation rules")
            print(f"\n4️⃣  Expected result:")
            print(f"   ✅ Spreadsheets should be IDENTICAL")
            print(f"   ✅ All formulas preserved exactly")
            print(f"   ✅ All formatting preserved")
            print(f"   ✅ All images/charts preserved")
            print(f"\n{'=' * 80}")

if __name__ == "__main__":
    asyncio.run(copy_complex_sheet())
