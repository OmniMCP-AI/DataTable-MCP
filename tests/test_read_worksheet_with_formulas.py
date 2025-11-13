#!/usr/bin/env python3
"""
Test suite for read_worksheet_with_formulas functionality

Tests cover:
1. Compare formulas vs calculated values
2. Identify cells with formulas (starting with =)
3. Verify formula strings are returned correctly
4. Test with sheets containing no formulas
5. Test with sheets containing various formula types (SUM, AVERAGE, IF, etc.)

Usage:
    # Run all formula tests
    python test_read_worksheet_with_formulas.py --env=local --test=all

    # Run specific test
    python test_read_worksheet_with_formulas.py --env=local --test=compare
    python test_read_worksheet_with_formulas.py --env=local --test=identify
    python test_read_worksheet_with_formulas.py --env=local --test=types

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
TEST_USER_ID = "68501372a3569b6897673a48"

# Test sheet with formulas
# You can use any Google Sheets with formulas - this is a sample
TEST_SHEET_URI = "https://docs.google.com/spreadsheets/d/16cLx4H72h8RqCklk2pfKLEixt6D0UIrt62MMOufrU60/edit?gid=0"


async def test_compare_values_vs_formulas(url, headers):
    """Test 1: Compare load_data_table (values) vs read_worksheet_with_formulas (formulas)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 1: Compare Values vs Formulas")
    print(f"{'='*60}")
    print(f"Purpose: Verify that read_worksheet_with_formulas returns formula strings")
    print(f"Test URI: {TEST_SHEET_URI}")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Step 1: Load regular values
            print("\n📊 Step 1: Loading data with calculated values...")
            values_res = await session.call_tool("load_data_table", {"uri": TEST_SHEET_URI})

            if values_res.isError:
                print(f"❌ Failed to load values: {values_res.content[0].text if values_res.content else 'Unknown error'}")
                return

            values_content = json.loads(values_res.content[0].text)
            if not values_content.get('success'):
                print(f"❌ Load values failed: {values_content.get('message')}")
                return

            values_data = values_content.get('data', [])
            print(f"✅ Loaded {len(values_data)} rows with calculated values")
            print(f"   Shape: {values_content.get('shape')}")
            print(f"   Table ID: {values_content.get('table_id')}")

            # Step 2: Load formulas
            print("\n📝 Step 2: Loading data with formulas...")
            formulas_res = await session.call_tool("read_worksheet_with_formulas", {"uri": TEST_SHEET_URI})

            print(f"Table ID: ",formulas_res)

            if formulas_res.isError:
                print(f"❌ Failed to load formulas: {formulas_res.content[0].text if formulas_res.content else 'Unknown error'}")
                return

            formulas_content = json.loads(formulas_res.content[0].text)
            if not formulas_content.get('success'):
                print(f"❌ Load formulas failed: {formulas_content.get('message')}")
                return

            formulas_data = formulas_content.get('data', [])
            print(f"✅ Loaded {len(formulas_data)} rows with formulas")
            print(f"   Shape: {formulas_content.get('shape')}")
            print(f"   Table ID: {formulas_content.get('table_id')}")
            print(f"   Value Render Option: {formulas_content.get('source_info', {}).get('value_render_option', 'N/A')}")

            # Step 3: Compare and identify differences
            print("\n🔍 Step 3: Comparing values vs formulas (first 5 rows)...")

            formula_count = 0
            plain_value_count = 0
            max_rows = min(5, len(values_data), len(formulas_data))

            for i in range(max_rows):
                value_row = values_data[i]
                formula_row = formulas_data[i]

                print(f"\n📋 Row {i+1}:")
                print("-" * 60)

                # Get all column names
                all_columns = set(value_row.keys()) | set(formula_row.keys())

                for col in sorted(all_columns):
                    value = value_row.get(col, "")
                    formula = formula_row.get(col, "")

                    # Check if there's a difference (indicating a formula)
                    if value != formula and isinstance(formula, str) and formula.startswith("="):
                        print(f"  {col}:")
                        print(f"    📊 Calculated: {value}")
                        print(f"    📝 Formula:    {formula} ⚡")
                        formula_count += 1
                    else:
                        # No formula, just show the value
                        print(f"  {col}: {value}")
                        if value:  # Only count non-empty cells
                            plain_value_count += 1

            # Step 4: Summary
            print("\n📊 Summary:")
            print(f"   Formula cells found: {formula_count} ⚡")
            print(f"   Plain value cells: {plain_value_count}")

            if formula_count > 0:
                print(f"   ✅ PASS: Formulas detected successfully")
            else:
                print(f"   ⚠️  WARNING: No formulas found in this sheet")

            print(f"\n{'='*60}")


async def test_identify_formula_cells(url, headers):
    """Test 2: Identify which cells contain formulas"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 2: Identify Formula Cells")
    print(f"{'='*60}")
    print(f"Purpose: List all cells that contain formulas")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("\n🔍 Loading data with formulas...")
            formulas_res = await session.call_tool("read_worksheet_with_formulas", {"uri": TEST_SHEET_URI})

            if formulas_res.isError:
                print(f"❌ Failed: {formulas_res.content[0].text if formulas_res.content else 'Unknown error'}")
                return

            content = json.loads(formulas_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Failed: {content.get('message')}")
                return

            data = content.get('data', [])
            print(f"✅ Loaded {len(data)} rows")

            # Identify all formula cells
            print("\n📝 Formula Cells Detected:")
            print("-" * 60)

            formula_cells = []
            for row_idx, row in enumerate(data, start=2):  # Start from row 2 (row 1 is headers)
                for col_name, cell_value in row.items():
                    if isinstance(cell_value, str) and cell_value.startswith("="):
                        formula_cells.append({
                            'row': row_idx,
                            'column': col_name,
                            'formula': cell_value
                        })

            if formula_cells:
                print(f"   Found {len(formula_cells)} formula cell(s):\n")
                for cell in formula_cells[:10]:  # Show first 10
                    print(f"   Row {cell['row']}, Column '{cell['column']}':")
                    print(f"      Formula: {cell['formula']}")

                if len(formula_cells) > 10:
                    print(f"\n   ... and {len(formula_cells) - 10} more formula cells")

                print(f"\n   ✅ PASS: Successfully identified {len(formula_cells)} formula cells")
            else:
                print(f"   ⚠️  No formula cells found in this sheet")
                print(f"   Note: This sheet may only contain static values")

            print(f"\n{'='*60}")


async def test_formula_types(url, headers):
    """Test 3: Test various formula types (if present)"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 3: Analyze Formula Types")
    print(f"{'='*60}")
    print(f"Purpose: Categorize different types of formulas")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("\n🔍 Loading formulas...")
            formulas_res = await session.call_tool("read_worksheet_with_formulas", {"uri": TEST_SHEET_URI})

            if formulas_res.isError:
                print(f"❌ Failed: {formulas_res.content[0].text if formulas_res.content else 'Unknown error'}")
                return

            content = json.loads(formulas_res.content[0].text)
            if not content.get('success'):
                print(f"❌ Failed: {content.get('message')}")
                return

            data = content.get('data', [])

            # Collect and categorize formulas
            formula_types = {
                'SUM': [],
                'AVERAGE': [],
                'IF': [],
                'VLOOKUP': [],
                'COUNT': [],
                'OTHER': []
            }

            for row_idx, row in enumerate(data, start=2):
                for col_name, cell_value in row.items():
                    if isinstance(cell_value, str) and cell_value.startswith("="):
                        formula_upper = cell_value.upper()

                        categorized = False
                        for func_type in ['SUM', 'AVERAGE', 'IF', 'VLOOKUP', 'COUNT']:
                            if func_type in formula_upper:
                                formula_types[func_type].append({
                                    'row': row_idx,
                                    'column': col_name,
                                    'formula': cell_value
                                })
                                categorized = True
                                break

                        if not categorized:
                            formula_types['OTHER'].append({
                                'row': row_idx,
                                'column': col_name,
                                'formula': cell_value
                            })

            # Display results
            print("\n📊 Formula Type Summary:")
            print("-" * 60)

            total_formulas = sum(len(formulas) for formulas in formula_types.values())

            if total_formulas == 0:
                print("   ⚠️  No formulas found in this sheet")
            else:
                for func_type, formulas in formula_types.items():
                    if formulas:
                        print(f"\n   {func_type} formulas: {len(formulas)}")
                        # Show first example
                        example = formulas[0]
                        print(f"      Example: Row {example['row']}, {example['column']} = {example['formula']}")

                print(f"\n   Total formulas: {total_formulas}")
                print(f"   ✅ PASS: Successfully analyzed formula types")

            print(f"\n{'='*60}")


async def test_metadata_comparison(url, headers):
    """Test 4: Compare metadata between regular load and formula load"""
    print(f"\n{'='*60}")
    print(f"🧪 Test 4: Metadata Comparison")
    print(f"{'='*60}")
    print(f"Purpose: Verify metadata differences between two loading methods")

    async with streamablehttp_client(url=url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Load both versions
            print("\n📥 Loading both versions...")
            values_res = await session.call_tool("load_data_table", {"uri": TEST_SHEET_URI})
            formulas_res = await session.call_tool("read_worksheet_with_formulas", {"uri": TEST_SHEET_URI})

            values_content = json.loads(values_res.content[0].text)
            formulas_content = json.loads(formulas_res.content[0].text)

            # Compare metadata
            print("\n📦 Metadata Comparison:")
            print("-" * 60)

            print(f"\n📊 Regular Load (load_data_table):")
            print(f"   Table ID: {values_content.get('table_id')}")
            print(f"   Name: {values_content.get('name')}")
            print(f"   Shape: {values_content.get('shape')}")
            print(f"   Value Render: {values_content.get('source_info', {}).get('value_render_option', 'DEFAULT/FORMATTED_VALUE')}")

            print(f"\n📝 Formula Load (read_worksheet_with_formulas):")
            print(f"   Table ID: {formulas_content.get('table_id')}")
            print(f"   Name: {formulas_content.get('name')}")
            print(f"   Shape: {formulas_content.get('shape')}")
            print(f"   Value Render: {formulas_content.get('source_info', {}).get('value_render_option', 'N/A')}")

            # Verify key differences
            checks = []

            # Check 1: Table ID should have "_formulas" suffix
            formula_table_id = formulas_content.get('table_id', '')
            if '_formulas' in formula_table_id:
                checks.append(('✅', 'Table ID has "_formulas" suffix'))
            else:
                checks.append(('❌', 'Table ID missing "_formulas" suffix'))

            # Check 2: Name should indicate "(Formulas)"
            formula_name = formulas_content.get('name', '')
            if '(Formulas)' in formula_name or 'Formulas' in formula_name:
                checks.append(('✅', 'Name indicates formula mode'))
            else:
                checks.append(('⚠️', 'Name does not clearly indicate formula mode'))

            # Check 3: Metadata should have value_render_option = FORMULA
            formula_render = formulas_content.get('source_info', {}).get('value_render_option', '')
            if formula_render == 'FORMULA':
                checks.append(('✅', 'Metadata has value_render_option=FORMULA'))
            else:
                checks.append(('❌', f'Metadata has incorrect value_render_option: {formula_render}'))

            # Check 4: Shapes should match (same dimensions)
            if values_content.get('shape') == formulas_content.get('shape'):
                checks.append(('✅', 'Shapes match (same dimensions)'))
            else:
                checks.append(('⚠️', 'Shapes differ'))

            print("\n🔍 Validation Checks:")
            print("-" * 60)
            for status, message in checks:
                print(f"   {status} {message}")

            # Overall result
            passed = sum(1 for status, _ in checks if status == '✅')
            total = len(checks)

            print(f"\n📊 Result: {passed}/{total} checks passed")

            if passed == total:
                print(f"   ✅ PASS: All metadata checks passed")
            elif passed >= total - 1:
                print(f"   ⚠️  PARTIAL: Most checks passed")
            else:
                print(f"   ❌ FAIL: Multiple checks failed")

            print(f"\n{'='*60}")


async def run_all_tests(url, headers):
    """Run all tests"""
    print(f"\n{'#'*60}")
    print(f"# Running All read_worksheet_with_formulas Tests")
    print(f"# Test Sheet: {TEST_SHEET_URI}")
    print(f"{'#'*60}")

    try:
        await test_compare_values_vs_formulas(url, headers)
        await test_identify_formula_cells(url, headers)
        await test_formula_types(url, headers)
        await test_metadata_comparison(url, headers)

        print(f"\n{'#'*60}")
        print(f"# ✅ All Tests Completed!")
        print(f"{'#'*60}")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(description='Test read_worksheet_with_formulas functionality')
    parser.add_argument('--env', choices=['local', 'test', 'prod'], default='local',
                       help='Environment to test against')
    parser.add_argument('--test', choices=['all', 'compare', 'identify', 'types', 'metadata'],
                       default='all', help='Which test to run')

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

    # Determine URL based on environment (must include /mcp endpoint)
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
    print(f"   Test Sheet: {TEST_SHEET_URI}")

    headers = {
        "GOOGLE_OAUTH_REFRESH_TOKEN": refresh_token,
        "GOOGLE_OAUTH_CLIENT_ID": client_id,
        "GOOGLE_OAUTH_CLIENT_SECRET": client_secret,
    }

    # Run selected test
    test_map = {
        'all': run_all_tests,
        'compare': test_compare_values_vs_formulas,
        'identify': test_identify_formula_cells,
        'types': test_formula_types,
        'metadata': test_metadata_comparison,
    }

    asyncio.run(test_map[args.test](url, headers))


if __name__ == "__main__":
    main()
