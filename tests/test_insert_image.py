#!/usr/bin/env python3
"""
Test suite for inserting images into Google Sheets

Tests cover:
1. Insert embedded image over cells (floating image)
2. Insert image using IMAGE formula into a cell
3. Test with different image URLs and positions

Usage:
    # Run the embedded image test
    python test_insert_image.py --env=test --test=embedded

    # Run the IMAGE formula test
    python test_insert_image.py --env=test --test=formula

Environment Variables Required:
- TEST_GOOGLE_OAUTH_REFRESH_TOKEN
- TEST_GOOGLE_OAUTH_CLIENT_ID
- TEST_GOOGLE_OAUTH_CLIENT_SECRET
"""

from mcp.client.streamable_http import streamablehttp_client
import asyncio
import json
import argparse
import os
from mcp import ClientSession

# Test configuration
TEST_USER_ID = os.environ.get("TEST_USER_ID")

# Test spreadsheet to insert image
TEST_SPREADSHEET_URI = "https://docs.google.com/spreadsheets/d/1jFgc_WYBYPDJzHbtm1LYT8Bh-YH8MrJUcOZYi_OwPvs/edit?gid=307471326#gid=307471326"

# Image URL to insert
TEST_IMAGE_URL = "https://img.freepik.com/free-photo/sea-coast-with-seashells-texture-waves-sea-lanscape_169016-29071.jpg"

# Cell position to insert image
TEST_CELL = "A1"
TEST_ROW = 1
TEST_COL = 1


async def test_insert_image_with_resize(url, headers):
    """Test: Insert image in cell with automatic resizing"""
    print(f"\n{'='*60}")
    print(f"🧪 Test: Insert Image in Cell with Auto-Resize")
    print(f"{'='*60}")
    print(f"Purpose: Insert image using IMAGE formula + auto-resize cell")
    print(f"Test URI: {TEST_SPREADSHEET_URI}")
    print(f"Image URL: {TEST_IMAGE_URL}")
    print(f"Target Cell: {TEST_CELL}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print(f"\n📋 Step 1: Inserting image with auto-resize...")
            print(f"   Cell: {TEST_CELL}")
            print(f"   Image size: 400x300 pixels")
            print(f"   Cell will be auto-resized to match")

            # Insert image in cell with auto-resize
            insert_res = await session.call_tool(
                "insert_image_in_cell",
                {
                    "uri": TEST_SPREADSHEET_URI,
                    "image_url": TEST_IMAGE_URL,
                    "cell_address": TEST_CELL,
                    "width_pixels": 400,
                    "height_pixels": 300
                }
            )

            if insert_res.isError:
                print(f"❌ Failed: {insert_res.content[0].text if insert_res.content else 'Unknown error'}")
                return

            content = json.loads(insert_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Failed: {content.get('message')}")
                return

            print(f"✅ Success!")
            print(f"\n📊 Insert Result:")
            print(f"   Message: {content.get('message', 'Image inserted')}")
            print(f"   Worksheet: {content.get('worksheet', 'N/A')}")
            print(f"   Cell: {content.get('range', 'N/A')}")
            print(f"   Size: {content.get('shape', 'N/A')}")

            # Show the spreadsheet link
            print(f"\n🔗 View Result:")
            print(f"   Open this link to see the image:")
            print(f"   {TEST_SPREADSHEET_URI}")

            print(f"\n{'='*60}")
            print(f"✅ Test Completed!")
            print(f"   The image should be perfectly fitted in cell {TEST_CELL}.")
            print(f"   Both the image and cell are sized at 400x300 pixels.")
            print(f"{'='*60}")


async def test_insert_image_formula(url, headers):
    """Test: Insert image into Google Sheets cell using IMAGE formula"""
    print(f"\n{'='*60}")
    print(f"🧪 Test: Insert Image into Google Sheets Cell")
    print(f"{'='*60}")
    print(f"Purpose: Insert an image using the IMAGE formula")
    print(f"Test URI: {TEST_SPREADSHEET_URI}")
    print(f"Image URL: {TEST_IMAGE_URL}")
    print(f"Target Cell: {TEST_CELL} (Row {TEST_ROW}, Col {TEST_COL})")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: Construct the IMAGE formula
            # Mode 1: Resizes the image to fit the cell, maintaining aspect ratio
            image_formula = f'=IMAGE("{TEST_IMAGE_URL}", 1)'
            print(f"\n📋 Step 1: Constructing IMAGE formula...")
            print(f"   Formula: {image_formula}")

            # Step 2: Prepare the data to write
            # We need to write to a specific cell using write_data_table
            # The data should be structured as a list of rows
            data_to_write = [[image_formula]]

            print(f"\n📝 Step 2: Writing IMAGE formula to cell {TEST_CELL}...")
            print(f"   Using update_range tool")
            print(f"   Data: {data_to_write}")

            # Step 3: Write the formula to the cell
            # IMPORTANT: Use value_input_option='USER_ENTERED' to parse the formula
            write_res = await session.call_tool(
                "update_range",
                {
                    "uri": TEST_SPREADSHEET_URI,
                    "data": data_to_write,
                    "range_address": TEST_CELL,
                    "value_input_option": "USER_ENTERED"
                }
            )

            if write_res.isError:
                print(f"❌ Failed: {write_res.content[0].text if write_res.content else 'Unknown error'}")
                return

            content = json.loads(write_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Failed: {content.get('message')}")
                return

            print(f"✅ Success!")
            print(f"\n📊 Write Result:")
            print(f"   Range updated: {content.get('range', 'N/A')}")
            print(f"   Cells updated: {content.get('updated_cells', 0)}")
            print(f"   Shape: {content.get('shape', 'N/A')}")

            # Step 4: Verify the formula was written correctly
            print(f"\n🔍 Step 3: Verifying the formula was written...")
            print(f"   Reading back cell {TEST_CELL}...")

            # Read the data back to verify
            read_res = await session.call_tool(
                "load_data_table",
                {
                    "uri": TEST_SPREADSHEET_URI
                }
            )

            if not read_res.isError:
                read_content = json.loads(read_res.content[0].text)
                if read_content.get('success'):
                    data = read_content.get('data', [])
                    if data and len(data) > 0:
                        first_cell_value = data[0][0] if len(data[0]) > 0 else None
                        print(f"   ✅ Read back first cell value: {first_cell_value}")

                        # Note: The read back value might be the image URL or the formula text
                        # depending on how the API returns it
                        print(f"\n   📝 Note: The cell now contains an IMAGE formula")
                        print(f"      Open the spreadsheet to see the image displayed")
                    else:
                        print(f"   ⚠️  No data read back")
                else:
                    print(f"   ⚠️  Could not read data: {read_content.get('message')}")
            else:
                print(f"   ⚠️  Could not read data")

            # Step 5: Show the spreadsheet link
            print(f"\n🔗 View Result:")
            print(f"   Open this link to see the IMAGE formula in the cell:")
            print(f"   {TEST_SPREADSHEET_URI}")
            print(f"   Note: You may need to resize the row/column to see the full image.")

            print(f"\n{'='*60}")
            print(f"✅ Test Completed!")
            print(f"{'='*60}")


async def test_insert_multiple_images(url, headers):
    """Test: Insert multiple images in different cells"""
    print(f"\n{'='*60}")
    print(f"🧪 Test: Insert Multiple Images")
    print(f"{'='*60}")
    print(f"Purpose: Insert multiple images in different cells")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Different image URLs for testing
            test_images = [
                ("A1", TEST_IMAGE_URL),
                ("B1", "https://i.imgur.com/gtfe7oc.png"),  # Alternative image
            ]

            for cell, image_url in test_images:
                image_formula = f'=IMAGE("{image_url}", 1)'
                print(f"\n📝 Inserting image into cell {cell}...")
                print(f"   Image URL: {image_url}")
                print(f"   Formula: {image_formula}")

                data_to_write = [[image_formula]]

                write_res = await session.call_tool(
                    "update_range",
                    {
                        "uri": TEST_SPREADSHEET_URI,
                        "data": data_to_write,
                        "range_address": cell,
                        "value_input_option": "USER_ENTERED"
                    }
                )

                if write_res.isError:
                    print(f"   ❌ Failed: {write_res.content[0].text if write_res.content else 'Unknown error'}")
                    continue

                content = json.loads(write_res.content[0].text)
                if content.get('success'):
                    print(f"   ✅ Success! Image formula written to {cell}")
                else:
                    print(f"   ❌ Failed: {content.get('message')}")

            print(f"\n🔗 View Result:")
            print(f"   {TEST_SPREADSHEET_URI}")
            print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='Test image insertion functionality')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against')
    parser.add_argument('--test', choices=['auto', 'formula', 'multiple', 'all'],
                       default='auto', help='Which test to run')

    args = parser.parse_args()

    # Get environment variables
    refresh_token = os.getenv("TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
    client_id = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.getenv("TEST_GOOGLE_OAUTH_CLIENT_SECRET")

    if not all([refresh_token, client_id, client_secret]):
        print("❌ Missing required environment variables:")
        print("   TEST_GOOGLE_OAUTH_REFRESH_TOKEN")
        print("   TEST_GOOGLE_OAUTH_CLIENT_ID")
        print("   TEST_GOOGLE_OAUTH_CLIENT_SECRET")
        return

    # Determine URL based on environment
    if args.env == "test":
        url = "https://datatable-mcp-test.maybe.ai/mcp"
    elif args.env == "prod":
        url = "https://datatable-mcp.maybe.ai/mcp"
    else:
        url = "http://127.0.0.1:8321/mcp"

    print(f"\n🔧 Test Configuration:")
    print(f"   Environment: {args.env}")
    print(f"   Endpoint: {url}")
    print(f"   User ID: {TEST_USER_ID}")
    print(f"   Test Spreadsheet: {TEST_SPREADSHEET_URI}")

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": refresh_token,
        "GOOGLE_OAUTH_CLIENT_ID": client_id,
        "GOOGLE_OAUTH_CLIENT_SECRET": client_secret,
    }

    # Run selected test
    if args.test == 'auto':
        asyncio.run(test_insert_image_with_resize(url, headers))
    elif args.test == 'formula':
        asyncio.run(test_insert_image_formula(url, headers))
    elif args.test == 'multiple':
        asyncio.run(test_insert_multiple_images(url, headers))
    elif args.test == 'all':
        asyncio.run(test_insert_image_with_resize(url, headers))
        asyncio.run(test_insert_image_formula(url, headers))
        asyncio.run(test_insert_multiple_images(url, headers))


if __name__ == "__main__":
    main()
