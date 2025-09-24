#!/usr/bin/env python3
"""
Basic test script for DataTable MCP Server
Tests core functionality of all tool categories
"""

import asyncio
import sys
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_lifecycle_tools():
    """Test table lifecycle management tools"""
    print("🧪 Testing Table Lifecycle Management...")

    # Import tools
    from datatable_tools.lifecycle_tools import create_table, list_tables, clone_table

    # Test create_table
    result = await create_table(
        data=[["Alice", 25, "Engineer"], ["Bob", 30, "Manager"], ["Carol", 28, "Designer"]],
        headers=["Name", "Age", "Role"],
        name="Employees"
    )
    assert result["success"], f"create_table failed: {result.get('error')}"
    table_id = result["table_id"]
    print(f"   ✅ Created table {table_id}")

    # Test list_tables
    result = await list_tables()
    assert result["success"], f"list_tables failed: {result.get('error')}"
    assert result["count"] == 1, f"Expected 1 table, got {result['count']}"
    print(f"   ✅ Listed {result['count']} table(s)")

    # Test clone_table
    result = await clone_table(table_id, "Employees Copy")
    assert result["success"], f"clone_table failed: {result.get('error')}"
    cloned_id = result["table_id"]
    print(f"   ✅ Cloned table to {cloned_id}")

    return table_id, cloned_id

async def test_manipulation_tools(table_id):
    """Test data manipulation tools"""
    print("🧪 Testing Data Manipulation...")

    from datatable_tools.manipulation_tools import append_row, add_column, set_range_values

    # Test append_row
    result = await append_row(
        table_id=table_id,
        row_data=["David", 35, "Director"],
        fill_strategy="none"
    )
    assert result["success"], f"append_row failed: {result.get('error')}"
    print(f"   ✅ Appended row, new shape: {result['new_shape']}")

    # Test add_column
    result = await add_column(
        table_id=table_id,
        column_name="Salary",
        default_value=50000
    )
    assert result["success"], f"add_column failed: {result.get('error')}"
    print(f"   ✅ Added column 'Salary'")

    # Test set_range_values
    result = await set_range_values(
        table_id=table_id,
        row_indices=[0, 1],
        column_names=["Salary"],
        values=[[75000], [85000]]
    )
    assert result["success"], f"set_range_values failed: {result.get('error')}"
    print(f"   ✅ Updated salary values")

async def test_query_tools(table_id):
    """Test data query and access tools"""
    print("🧪 Testing Data Query & Access...")

    from datatable_tools.query_tools import get_table_data, filter_rows, sort_table

    # Test get_table_data
    result = await get_table_data(
        table_id=table_id,
        output_format="records"
    )
    assert result["success"], f"get_table_data failed: {result.get('error')}"
    print(f"   ✅ Retrieved table data: {result['shape']} shape")

    # Test filter_rows
    result = await filter_rows(
        table_id=table_id,
        conditions=[{"column": "Age", "operator": "gt", "value": 27}],
        logic="AND"
    )
    assert result["success"], f"filter_rows failed: {result.get('error')}"
    print(f"   ✅ Filtered {result['filtered_rows']} rows out of {result['original_rows']}")

    # Test sort_table
    result = await sort_table(
        table_id=table_id,
        sort_columns=["Age"],
        ascending=[False],
        in_place=True
    )
    assert result["success"], f"sort_table failed: {result.get('error')}"
    print(f"   ✅ Sorted table by Age (descending)")

async def test_export_tools(table_id):
    """Test export and persistence tools"""
    print("🧪 Testing Export & Persistence...")

    from datatable_tools.export_tools import export_table
    import os
    import tempfile

    # Test export_table (CSV)
    result = await export_table(
        table_id=table_id,
        export_format="csv",
        return_content=True
    )
    assert result["success"], f"export_table (CSV) failed: {result.get('error')}"
    assert "content" in result, "CSV content not returned"
    print(f"   ✅ Exported as CSV content")

    # Test export_table (JSON)
    result = await export_table(
        table_id=table_id,
        export_format="json",
        return_content=True
    )
    assert result["success"], f"export_table (JSON) failed: {result.get('error')}"
    print(f"   ✅ Exported as JSON content")

async def test_advanced_tools(table_id, cloned_id):
    """Test advanced operations tools"""
    print("🧪 Testing Advanced Operations...")

    from datatable_tools.advanced_tools import aggregate_data, map_values
    from datatable_tools.manipulation_tools import add_column

    # Add department column for aggregation test
    await add_column(table_id, "Department", "Engineering")
    await add_column(cloned_id, "Department", "Sales")

    # Test aggregate_data
    result = await aggregate_data(
        table_id=table_id,
        group_by=["Department"],
        aggregations={"Age": ["mean", "max"], "Salary": "mean"}
    )
    assert result["success"], f"aggregate_data failed: {result.get('error')}"
    print(f"   ✅ Aggregated data: {result['aggregated_rows']} groups")

    # Test map_values
    result = await map_values(
        table_id=table_id,
        column_mappings={"Role": {"Engineer": "Software Engineer", "Manager": "Team Manager"}},
        create_new_columns=True
    )
    assert result["success"], f"map_values failed: {result.get('error')}"
    print(f"   ✅ Mapped values in {result['total_columns_processed']} column(s)")

async def test_session_tools():
    """Test session management tools"""
    print("🧪 Testing Session Management...")

    from datatable_tools.session_tools import get_session_stats, cleanup_tables

    # Test get_session_stats
    result = await get_session_stats()
    assert result["success"], f"get_session_stats failed: {result.get('error')}"
    print(f"   ✅ Session stats: {result['session_stats']['total_tables']} tables, {result['session_stats']['total_memory_mb']} MB")

    # Test cleanup_tables (force cleanup)
    result = await cleanup_tables(force_cleanup=True)
    assert result["success"], f"cleanup_tables failed: {result.get('error')}"
    print(f"   ✅ Cleaned up {result['cleaned_count']} tables")

async def run_all_tests():
    """Run all tests in sequence"""
    print("🚀 Starting DataTable MCP Server Tests")
    print("=" * 50)

    try:
        # Test each category
        table_id, cloned_id = await test_lifecycle_tools()
        await test_manipulation_tools(table_id)
        await test_query_tools(table_id)
        await test_export_tools(table_id)
        await test_advanced_tools(table_id, cloned_id)
        await test_session_tools()

        print("\n" + "=" * 50)
        print("🎉 All tests passed! DataTable MCP Server is working correctly.")
        return True

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Run tests
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)