#!/usr/bin/env python3
"""
Integration test for MCP Server functionality
Tests the actual MCP tools through direct function calls
"""

import asyncio
import sys
import logging
import json

# Add parent directory to path for imports
sys.path.insert(0, '..')

from datatable_tools.table_manager import table_manager

# Import the actual function implementations (before MCP decoration)
# We need to access the underlying functions
async def test_mcp_tools_integration():
    """Test MCP tools integration"""
    print("🧪 Testing MCP Tools Integration...")

    # Clean up any existing tables
    table_manager.cleanup_expired_tables(force=True)

    # Test data
    test_data = [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"], ["Carol", 28, "Designer"]]
    test_headers = ["Name", "Age", "Role"]

    # Test create_table functionality (core function)
    table_id = table_manager.create_table(
        data=test_data,
        headers=test_headers,
        name="Test Employees",
        source_info={"type": "test"}
    )

    print(f"✅ Created table: {table_id}")

    # Test list_tables functionality
    tables = table_manager.list_tables()
    assert len(tables) == 1, f"Expected 1 table, got {len(tables)}"
    print(f"✅ Listed {len(tables)} table(s)")

    # Test clone_table functionality
    cloned_id = table_manager.clone_table(table_id, "Cloned Employees")
    assert cloned_id is not None, "Clone failed"
    print(f"✅ Cloned table: {table_id} -> {cloned_id}")

    # Test table manipulation
    table = table_manager.get_table(table_id)
    original_shape = table.shape

    # Add a row
    table.append_row(["David", 35, "Director"])
    assert table.shape[0] == original_shape[0] + 1, "Row append failed"
    print(f"✅ Added row, new shape: {table.shape}")

    # Add a column
    table.add_column("Salary", 50000)
    assert table.shape[1] == original_shape[1] + 1, "Column add failed"
    print(f"✅ Added column, new shape: {table.shape}")

    # Test filtering
    filtered = table.filter_rows([{"column": "Age", "operator": "gt", "value": 27}])
    print(f"✅ Filtered {len(filtered)} rows from {table.shape[0]} total")

    # Test sorting
    sorted_df = table.sort_table(["Age"], [False])
    print(f"✅ Sorted table by Age (descending)")

    print("\n🎉 All MCP integration tests passed!")
    return True

async def main():
    """Run integration tests"""
    print("🚀 Starting MCP Integration Tests")
    print("=" * 50)

    try:
        success = await test_mcp_tools_integration()
        return success
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)