#!/usr/bin/env python3
"""
Test the core table manager functionality directly
without MCP server decorators
"""

import asyncio
import sys
import logging
import unittest
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, '..')

from datatable_tools.table_manager import table_manager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestTableManager(unittest.TestCase):
    """Test table manager core functionality"""

    def setUp(self):
        """Clean up any existing tables before each test"""
        table_manager.cleanup_expired_tables(force=True)

    def test_create_table(self):
        """Test table creation"""
        data = [["Alice", 25, "Engineer"], ["Bob", 30, "Manager"], ["Carol", 28, "Designer"]]
        headers = ["Name", "Age", "Role"]
        name = "Employees"

        table_id = table_manager.create_table(
            data=data,
            headers=headers,
            name=name,
            source_info={"type": "manual_creation"}
        )

        self.assertIsNotNone(table_id)
        self.assertTrue(table_id.startswith("dt_"))

        # Get table and verify
        table = table_manager.get_table(table_id)
        self.assertIsNotNone(table)
        self.assertEqual(table.metadata.name, name)
        self.assertEqual(table.shape, [3, 3])
        self.assertEqual(table.headers, headers)

        print(f"✅ Created table {table_id}")

    def test_list_tables(self):
        """Test listing tables"""
        # Create a table first
        table_id = table_manager.create_table(
            data=[["test", 1]],
            headers=["col1", "col2"],
            name="Test Table"
        )

        tables = table_manager.list_tables()
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]["table_id"], table_id)
        self.assertEqual(tables[0]["name"], "Test Table")

        print(f"✅ Listed {len(tables)} table(s)")

    def test_clone_table(self):
        """Test table cloning"""
        # Create original table
        data = [["Alice", 25], ["Bob", 30]]
        headers = ["Name", "Age"]
        original_id = table_manager.create_table(
            data=data,
            headers=headers,
            name="Original"
        )

        # Clone it
        cloned_id = table_manager.clone_table(original_id, "Cloned Table")
        self.assertIsNotNone(cloned_id)
        self.assertNotEqual(original_id, cloned_id)

        # Verify clone
        cloned_table = table_manager.get_table(cloned_id)
        original_table = table_manager.get_table(original_id)

        self.assertEqual(cloned_table.shape, original_table.shape)
        self.assertEqual(cloned_table.headers, original_table.headers)
        self.assertEqual(cloned_table.metadata.name, "Cloned Table")

        print(f"✅ Cloned table {original_id} to {cloned_id}")

    def test_table_manipulation(self):
        """Test table data manipulation"""
        # Create a table
        table_id = table_manager.create_table(
            data=[["Alice", 25], ["Bob", 30]],
            headers=["Name", "Age"],
            name="Test Table"
        )

        table = table_manager.get_table(table_id)

        # Test append row
        table.append_row(["Carol", 28])
        self.assertEqual(table.shape[0], 3)

        # Test add column
        table.add_column("Role", "Unknown")
        self.assertEqual(table.shape[1], 3)
        self.assertIn("Role", table.headers)

        # Test set values
        table.set_values([0, 1], ["Role"], [["Engineer"], ["Manager"]])

        print("✅ Table manipulation successful")

    def test_table_query(self):
        """Test table query operations"""
        # Create table with test data
        data = [
            ["Alice", 25, "Engineer"],
            ["Bob", 30, "Manager"],
            ["Carol", 28, "Designer"],
            ["David", 35, "Director"]
        ]
        headers = ["Name", "Age", "Role"]

        table_id = table_manager.create_table(data=data, headers=headers)
        table = table_manager.get_table(table_id)

        # Test filtering
        filtered = table.filter_rows([{"column": "Age", "operator": "gt", "value": 27}])
        self.assertEqual(len(filtered), 3)  # Bob, Carol, David

        # Test sorting
        sorted_df = table.sort_table(["Age"], [False])
        self.assertEqual(sorted_df.iloc[0]["Name"], "David")  # Oldest first

        print("✅ Table query operations successful")

    def test_cleanup(self):
        """Test table cleanup"""
        # Create some tables
        table_id1 = table_manager.create_table([["test1"]], ["col1"])
        table_id2 = table_manager.create_table([["test2"]], ["col1"])

        # Verify they exist
        self.assertEqual(len(table_manager.list_tables()), 2)

        # Force cleanup
        cleaned = table_manager.cleanup_expired_tables(force=True)
        self.assertEqual(cleaned, 2)
        self.assertEqual(len(table_manager.list_tables()), 0)

        print("✅ Cleanup successful")

def run_tests():
    """Run all table manager tests"""
    print("🧪 Testing Table Manager Core Functionality")
    print("=" * 50)

    unittest.main(argv=[''], exit=False, verbosity=2)

    print("🎉 All table manager tests completed!")

if __name__ == "__main__":
    run_tests()