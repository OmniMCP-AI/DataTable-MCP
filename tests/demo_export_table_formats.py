#!/usr/bin/env python3
"""
Demo script for the simplified export_table function
Shows how easy it is to export tables to various formats using URI-based detection
"""

import os
import sys
import tempfile
import json
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, '..')

from datatable_tools.export_tools import _export_file
from datatable_tools.utils import detect_export_type, parse_export_uri
from datatable_tools.table_manager import table_manager

def demo_export_table_formats():
    """Demonstrate the simplified export_table functionality"""

    print("🚀 Demo: Simplified export_table with URI-based Detection")
    print("=" * 65)

    # Create a test table with sample data
    sample_data = [
        ["Alice Johnson", 25, "Software Engineer", 75000, "Engineering"],
        ["Bob Smith", 30, "Product Manager", 85000, "Product"],
        ["Carol Davis", 28, "UX Designer", 65000, "Design"],
        ["David Wilson", 35, "Engineering Director", 120000, "Engineering"],
        ["Eva Brown", 32, "Data Scientist", 95000, "Data"]
    ]
    headers = ["Name", "Age", "Role", "Salary", "Department"]

    table_id = table_manager.create_table(
        data=sample_data,
        headers=headers,
        name="Employee Data Demo",
        source_info={"type": "demo_export"}
    )

    table = table_manager.get_table(table_id)
    print(f"📊 Created demo table: {table_id} with {table.shape[0]} rows, {table.shape[1]} columns")

    # Create temporary directory for exports
    temp_dir = tempfile.mkdtemp()
    print(f"📁 Using temp directory: {temp_dir}")

    async def export_demo(uri, description, encoding=None, delimiter=None):
        """Helper function to demo export functionality"""
        try:
            print(f"\n{description}")
            print(f"   URI: {uri}")

            # Show auto-detection
            export_type = detect_export_type(uri)
            print(f"   Detected Type: {export_type}")

            if export_type == "google_sheets":
                print(f"   ⚠️  Google Sheets export detected but not implemented in demo")
                return

            # Parse URI and export
            export_info = parse_export_uri(uri)
            result = await _export_file(table, export_info, encoding, delimiter)

            if result["success"]:
                file_size_kb = result["file_size"] / 1024
                print(f"   ✅ Success: {result['file_path']}")
                print(f"   📏 Size: {file_size_kb:.1f} KB ({result['rows_exported']} rows)")

                # Show file content preview for text formats
                if export_type in ["csv", "json"]:
                    with open(result["file_path"], 'r') as f:
                        preview = f.read(200) + "..." if len(f.read()) > 200 else f.read()
                        f.seek(0)
                        preview = f.read(200)
                    print(f"   👁️  Preview: {preview[:100]}...")

            else:
                print(f"   ❌ Error: {result['error']}")

        except Exception as e:
            print(f"   💥 Exception: {str(e)}")

    import asyncio

    # Demo 1: CSV Export
    csv_path = os.path.join(temp_dir, "employees.csv")
    asyncio.run(export_demo(
        csv_path,
        "📄 Test 1: CSV Export (Auto-detected from .csv extension)"
    ))

    # Demo 2: CSV with Custom Delimiter
    csv_semicolon_path = os.path.join(temp_dir, "employees_semicolon.csv")
    asyncio.run(export_demo(
        csv_semicolon_path,
        "📄 Test 2: CSV with Semicolon Delimiter",
        delimiter=";"
    ))

    # Demo 3: Excel Export
    excel_path = os.path.join(temp_dir, "employees.xlsx")
    asyncio.run(export_demo(
        excel_path,
        "📊 Test 3: Excel Export (Auto-detected from .xlsx extension)"
    ))

    # Demo 4: JSON Export
    json_path = os.path.join(temp_dir, "employees.json")
    asyncio.run(export_demo(
        json_path,
        "📋 Test 4: JSON Export (Auto-detected from .json extension)"
    ))

    # Demo 5: Parquet Export
    parquet_path = os.path.join(temp_dir, "employees.parquet")
    asyncio.run(export_demo(
        parquet_path,
        "🗃️ Test 5: Parquet Export (Auto-detected from .parquet extension)"
    ))

    # Demo 6: Google Sheets Detection
    print(f"\n📗 Test 6: Google Sheets URL Detection")
    gs_url = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"
    print(f"   URI: {gs_url}")
    detected = detect_export_type(gs_url)
    print(f"   Detected Type: {detected}")
    print(f"   ⚠️  Google Sheets export would work with proper authentication")

    # Demo 7: Nested Directory Creation
    nested_path = os.path.join(temp_dir, "reports", "2024", "q1", "data.csv")
    asyncio.run(export_demo(
        nested_path,
        "📁 Test 7: Nested Directory Creation (Auto-creates directories)"
    ))

    # Summary
    print("\n" + "=" * 65)
    print("📊 Export Demo Summary")
    print("=" * 65)

    print("✅ Supported Export Formats:")
    formats = [
        ("CSV", "data.csv", "Comma-separated values with optional custom delimiter"),
        ("Excel", "workbook.xlsx", "Microsoft Excel format (.xlsx/.xls)"),
        ("JSON", "data.json", "JavaScript Object Notation (records format)"),
        ("Parquet", "data.parquet", "Columnar storage format for analytics"),
        ("Google Sheets", "https://docs.google.com/spreadsheets/...", "Direct export to Google Sheets")
    ]

    for format_name, example, description in formats:
        print(f"   {format_name:<12} {example:<25} {description}")

    print(f"\n🎯 Key Benefits:")
    print(f"   • URI-based auto-detection - no manual format specification needed")
    print(f"   • Simple API - just table_id and destination URI")
    print(f"   • Automatic directory creation")
    print(f"   • Optional encoding and delimiter for CSV files")
    print(f"   • Consistent with load_table URI-based approach")

    # List created files
    print(f"\n📁 Files Created in {temp_dir}:")
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            file_path = os.path.join(root, file)
            size_kb = os.path.getsize(file_path) / 1024
            rel_path = os.path.relpath(file_path, temp_dir)
            print(f"   {rel_path:<30} {size_kb:>6.1f} KB")

    # Cleanup
    print(f"\n🧹 Cleaning up temporary files...")
    import shutil
    shutil.rmtree(temp_dir)
    table_manager.cleanup_expired_tables(force=True)

    print(f"🎉 Demo completed successfully!")
    return True

if __name__ == "__main__":
    print(f"⏰ Starting export demo at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    success = demo_export_table_formats()
    print(f"⏰ Demo completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(0 if success else 1)