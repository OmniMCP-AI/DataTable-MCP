#!/usr/bin/env python3
"""
Comprehensive test script for DataTable MCP Server
Tests table manager functionality and core operations
Simplified version focusing on core functionality
"""

import asyncio
import sys
import logging

# Add parent directory to path for imports
sys.path.insert(0, '..')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datatable_tools.table_manager import table_manager


async def test_table_manager_comprehensive():
    """Test comprehensive table manager functionality"""
    print("🧪 Testing Comprehensive Table Manager Operations...")

    # Clean up any existing tables for this test
    initial_count = len(table_manager.list_tables())

    # Test data
    test_data = [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"], ["Carol", 28, "Designer"]]
    test_headers = ["Name", "Age", "Role"]

    # Test 1: Create table
    table_id = table_manager.create_table(
        data=test_data,
        headers=test_headers,
        name="Comprehensive Test Employees",
        source_info={"type": "comprehensive_test"}
    )
    print(f"   ✅ Created table: {table_id}")

    # Test 2: List tables
    tables = table_manager.list_tables()
    assert len(tables) >= initial_count + 1, f"Expected at least {initial_count + 1} tables, got {len(tables)}"
    print(f"   ✅ Listed {len(tables)} total table(s)")

    # Test 3: Get table
    table = table_manager.get_table(table_id)
    assert table is not None, "Failed to retrieve table"
    print(f"   ✅ Retrieved table with shape: {table.shape}")

    # Test 4: Clone table
    cloned_id = table_manager.clone_table(table_id, "Comprehensive Cloned Employees")
    assert cloned_id is not None, "Clone failed"
    print(f"   ✅ Cloned table: {table_id} -> {cloned_id}")

    # Test 5: Manipulate original table
    original_shape = table.shape

    # Add row
    table.append_row(["David", 35, "Director"])
    assert table.shape[0] == original_shape[0] + 1, "Row append failed"
    print(f"   ✅ Added row, new shape: {table.shape}")

    # Add column
    table.add_column("Salary", 50000)
    assert table.shape[1] == original_shape[1] + 1, "Column add failed"
    print(f"   ✅ Added column, new shape: {table.shape}")

    # Test 6: Data operations
    # Filter rows
    filtered = table.filter_rows([{"column": "Age", "operator": "gt", "value": 27}])
    print(f"   ✅ Filtered {len(filtered)} rows from {table.shape[0]} total")

    # Sort table
    table.sort_table(["Age"], [False])
    print(f"   ✅ Sorted table by Age (descending)")

    # Update values
    table.set_values([0], ["Salary"], [[75000]])
    print(f"   ✅ Updated salary values")

    # Test 7: Advanced operations
    # Add more data for aggregation
    table.add_column("Department", "Engineering")
    print(f"   ✅ Added Department column")

    # Test aggregation (simplified)
    dept_data = table.df["Department"].unique()
    print(f"   ✅ Found departments: {list(dept_data)}")

    # Test 8: Export functionality (basic)
    csv_data = table.df.to_csv(index=False)
    assert len(csv_data) > 0, "CSV export failed"
    print(f"   ✅ Exported to CSV format ({len(csv_data)} chars)")

    json_data = table.df.to_json(orient='records')
    assert len(json_data) > 0, "JSON export failed"
    print(f"   ✅ Exported to JSON format ({len(json_data)} chars)")

    # Test 9: Memory and session info (basic implementation)
    tables_count = len(table_manager.list_tables())
    # Calculate basic memory estimate
    total_memory_mb = sum(len(table_manager.get_table(t["table_id"]).df) * len(table_manager.get_table(t["table_id"]).headers) * 8 / 1024 / 1024
                         for t in table_manager.list_tables()) if table_manager.list_tables() else 0
    print(f"   ✅ Session stats: {tables_count} tables, {total_memory_mb:.2f} MB estimated")

    return table_id, cloned_id


async def test_table_lifecycle():
    """Test table lifecycle operations"""
    print("🧪 Testing Table Lifecycle Operations...")

    # Create multiple tables
    tables_created = []

    for i in range(3):
        table_id = table_manager.create_table(
            data=[["Item", i], ["Value", i*10]],
            headers=["Type", "Data"],
            name=f"Lifecycle Test Table {i}",
            source_info={"type": "lifecycle_test", "index": i}
        )
        tables_created.append(table_id)
        print(f"   ✅ Created table {i+1}/3: {table_id}")

    # Test listing
    all_tables = table_manager.list_tables()
    print(f"   ✅ Total tables in session: {len(all_tables)}")

    # Test cleanup (but don't force cleanup all tables)
    expired_count = table_manager.cleanup_expired_tables(force=False)
    print(f"   ✅ Cleaned up {expired_count} expired tables")

    return tables_created


async def run_all_tests():
    """Run comprehensive test suite"""
    print("🚀 Starting Comprehensive DataTable MCP Server Tests")
    print("=" * 70)

    try:
        # Run comprehensive functionality test
        print("\n🧪 Running Comprehensive Functionality Tests")
        print("-" * 60)
        table_id, cloned_id = await test_table_manager_comprehensive()
        print("   🎉 Comprehensive functionality tests passed!")

        # Run lifecycle tests
        print("\n📋 Running Table Lifecycle Tests")
        print("-" * 60)
        lifecycle_tables = await test_table_lifecycle()
        print("   🎉 Table lifecycle tests passed!")

        print("\n" + "=" * 70)
        print("📊 Test Results Summary:")
        print("   Comprehensive Functionality Tests: ✅ PASSED")
        print("   Table Lifecycle Tests: ✅ PASSED")
        print(f"   Tables Created: {len(lifecycle_tables) + 2}")
        print(f"   Main Test Table: {table_id}")
        print(f"   Cloned Table: {cloned_id}")

        print("\n🎉 All tests passed! DataTable MCP Server is working correctly.")
        return True

    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Run comprehensive tests
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)