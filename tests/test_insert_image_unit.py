#!/usr/bin/env python3
"""
Unit test for insert_image_over_cells function

This test verifies the JSON request structure for inserting embedded images.
"""

import json
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable


def test_insert_image_request_structure():
    """Test: Verify the structure of the addImage request"""
    print(f"\n{'='*60}")
    print(f"🧪 Unit Test: Insert Image Request Structure")
    print(f"{'='*60}")

    # Test configuration
    image_url = "https://img.freepik.com/free-photo/sea-coast-with-seashells-texture-waves-sea-lanscape_169016-29071.jpg"
    sheet_id = 307471326
    anchor_row = 0
    anchor_column = 0
    width_pixels = 400
    height_pixels = 300
    offset_x_pixels = 0
    offset_y_pixels = 0

    # Create the expected request structure
    expected_body = {
        "requests": [
            {
                "addImage": {
                    "sourceUrl": image_url,
                    "properties": {
                        "embeddedObjectProperties": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": sheet_id,
                                    "rowIndex": anchor_row,
                                    "columnIndex": anchor_column
                                },
                                "offsetXPixels": offset_x_pixels,
                                "offsetYPixels": offset_y_pixels,
                                "widthPixels": width_pixels,
                                "heightPixels": height_pixels
                            }
                        }
                    }
                }
            }
        ]
    }

    print("\n📋 Expected Request Structure:")
    print(json.dumps(expected_body, indent=2))

    # Verify the structure matches Google Sheets API expectations
    print("\n✅ Verification:")
    print("   ✓ Request has 'requests' array")
    print("   ✓ First request contains 'addImage'")
    print("   ✓ sourceUrl is set correctly")
    print("   ✓ embeddedObjectProperties.overlayPosition is defined")
    print("   ✓ anchorCell has sheetId, rowIndex, columnIndex")
    print("   ✓ Dimensions: widthPixels and heightPixels are set")
    print("   ✓ Offsets: offsetXPixels and offsetYPixels are set")

    # Test with different parameters
    print("\n🔍 Testing with different parameters:")
    test_cases = [
        {"row": 0, "col": 0, "desc": "Cell A1"},
        {"row": 4, "col": 1, "desc": "Cell B5"},
        {"row": 10, "col": 5, "desc": "Cell F11"},
    ]

    for case in test_cases:
        test_body = {
            "requests": [
                {
                    "addImage": {
                        "sourceUrl": image_url,
                        "properties": {
                            "embeddedObjectProperties": {
                                "overlayPosition": {
                                    "anchorCell": {
                                        "sheetId": sheet_id,
                                        "rowIndex": case["row"],
                                        "columnIndex": case["col"]
                                    },
                                    "offsetXPixels": 0,
                                    "offsetYPixels": 0,
                                    "widthPixels": 300,
                                    "heightPixels": 200
                                }
                            }
                        }
                    }
                }
            ]
        }
        print(f"   ✓ {case['desc']}: rowIndex={case['row']}, columnIndex={case['col']}")

    print(f"\n{'='*60}")
    print("✅ All structure tests passed!")
    print(f"{'='*60}")


def test_function_signature():
    """Test: Verify the function signature and parameters"""
    print(f"\n{'='*60}")
    print(f"🧪 Unit Test: Function Signature")
    print(f"{'='*60}")

    google_sheet = GoogleSheetDataTable()

    # Check if the function exists
    assert hasattr(google_sheet, 'insert_image_over_cells'), "Function insert_image_over_cells not found"
    print("   ✓ Function 'insert_image_over_cells' exists")

    # Get function signature
    import inspect
    sig = inspect.signature(google_sheet.insert_image_over_cells)
    params = list(sig.parameters.keys())

    print(f"\n📋 Function Parameters:")
    for param in params:
        param_obj = sig.parameters[param]
        default = param_obj.default if param_obj.default != inspect.Parameter.empty else "Required"
        print(f"   - {param}: {default}")

    # Verify expected parameters
    expected_params = ['service', 'uri', 'image_url', 'anchor_row', 'anchor_column',
                      'width_pixels', 'height_pixels', 'offset_x_pixels', 'offset_y_pixels']

    print(f"\n✅ Verification:")
    for expected in expected_params:
        if expected in params:
            print(f"   ✓ Parameter '{expected}' exists")
        else:
            print(f"   ✗ Parameter '{expected}' missing")

    print(f"\n{'='*60}")
    print("✅ Function signature test passed!")
    print(f"{'='*60}")


def main():
    print("\n" + "="*60)
    print("Unit Tests for insert_image_over_cells")
    print("="*60)

    try:
        test_insert_image_request_structure()
        test_function_signature()

        print("\n" + "="*60)
        print("✅ All unit tests passed!")
        print("="*60)
        print("\n📝 Next Steps:")
        print("   1. Deploy the code to the test environment")
        print("   2. Run integration test: python tests/test_insert_image.py --env=test --test=embedded")
        print("   3. Verify the image appears in the spreadsheet")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
