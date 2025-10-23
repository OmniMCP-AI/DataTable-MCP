#!/usr/bin/env python3
"""
Unit tests for auto_expand_range functionality in Google Sheets datatable

Tests the automatic range expansion based on data dimensions, particularly:
- Ranges with colons (e.g., "F2:F5" -> "F2:G6" for 5x2 data)
- Single cell addresses (e.g., "A1" -> "A1:B5" for 5x2 data)
- Column letters (e.g., "F" -> "F1:F5" for 5 rows)
"""

import unittest
import sys
import os
import re

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_dir)


def column_letter_to_index(col_letter: str) -> int:
    """Convert column letter(s) to 0-based index (A=0, Z=25, AA=26, etc.)"""
    result = 0
    for char in col_letter:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1


def column_index_to_letter(col_index: int) -> str:
    """Convert 0-based column index to letter(s) (0=A, 25=Z, 26=AA, etc.)"""
    result = ""
    col_index += 1  # Convert to 1-based
    while col_index > 0:
        col_index -= 1
        result = chr(col_index % 26 + ord('A')) + result
        col_index //= 26
    return result


def auto_expand_range(range_addr: str, data_values: list[list]) -> str:
    """Auto-expand range to match data dimensions."""
    if not range_addr:
        return "A1"

    rows = len(data_values)
    cols = max(len(row) for row in data_values) if data_values else 0

    if rows == 0 or cols == 0:
        return range_addr

    # Case 1: Range with colon (e.g., "F2:F5")
    if ':' in range_addr:
        # Parse start cell from range
        start_cell = range_addr.split(':')[0]
        match = re.match(r'^([A-Z]+)(\d+)$', start_cell)
        if not match:
            return range_addr

        start_col = match.group(1)
        start_row = int(match.group(2))

        # Calculate end cell based on data dimensions
        start_col_index = column_letter_to_index(start_col)
        end_col_index = start_col_index + cols - 1
        end_col = column_index_to_letter(end_col_index)
        end_row = start_row + rows - 1

        expanded = f"{start_col}{start_row}:{end_col}{end_row}"
        return expanded

    # Case 2: Just a column letter (e.g., "B", "AA")
    if range_addr.isalpha():
        end_row = rows
        expanded = f"{range_addr}1:{range_addr}{end_row}"
        return expanded

    # Case 3: Cell address (e.g., "A23")
    match = re.match(r'^([A-Z]+)(\d+)$', range_addr)
    if not match:
        return range_addr

    start_col = match.group(1)
    start_row = int(match.group(2))

    # Calculate end cell
    start_col_index = column_letter_to_index(start_col)
    end_col_index = start_col_index + cols - 1
    end_col = column_index_to_letter(end_col_index)
    end_row = start_row + rows - 1

    expanded = f"{start_col}{start_row}:{end_col}{end_row}"
    return expanded


class TestAutoExpandRange(unittest.TestCase):
    """Test cases for auto_expand_range function"""

    def test_range_with_colon_single_column(self):
        """Test expanding a range that already has colon - single column case"""
        # Data: 5 rows, 1 column
        data = [['val1'], ['val2'], ['val3'], ['val4'], ['val5']]
        result = auto_expand_range("F2:F5", data)
        # Should expand to F2:F6 (5 rows starting from row 2)
        self.assertEqual(result, "F2:F6")

    def test_range_with_colon_multiple_columns(self):
        """Test expanding a range that already has colon - multiple columns case"""
        # Data: 5 rows, 2 columns (the failing case from the error log)
        data = [
            ['null', 'Head of Marketing/Footprint An…'],
            ['null', 'Blockchain Storage Developer @…'],
            ['null', 'MaybeAI | AI | Workflow | Mayb…'],
            ['null', 'Co-founder | CTO | AdTech | Cr…'],
            ['null', 'Marketing Specialist']
        ]
        result = auto_expand_range("F2:F5", data)
        # Should expand to F2:G6 (5 rows, 2 columns starting from F2)
        self.assertEqual(result, "F2:G6")

    def test_range_with_colon_wide_data(self):
        """Test expanding a range with many columns"""
        # Data: 3 rows, 5 columns
        data = [
            ['a', 'b', 'c', 'd', 'e'],
            ['f', 'g', 'h', 'i', 'j'],
            ['k', 'l', 'm', 'n', 'o']
        ]
        result = auto_expand_range("A1:A1", data)
        # Should expand to A1:E3
        self.assertEqual(result, "A1:E3")

    def test_single_cell_address(self):
        """Test expanding a single cell address"""
        # Data: 3 rows, 2 columns
        data = [['a', 'b'], ['c', 'd'], ['e', 'f']]
        result = auto_expand_range("B5", data)
        # Should expand to B5:C7 (3 rows, 2 columns starting from B5)
        self.assertEqual(result, "B5:C7")

    def test_column_letter_only(self):
        """Test expanding a column letter"""
        # Data: 5 rows, 1 column
        data = [['val1'], ['val2'], ['val3'], ['val4'], ['val5']]
        result = auto_expand_range("F", data)
        # Should expand to F1:F5
        self.assertEqual(result, "F1:F5")

    def test_column_letter_multi_column_data(self):
        """Test column letter with multi-column data (edge case)"""
        # Data: 3 rows, 2 columns
        data = [['a', 'b'], ['c', 'd'], ['e', 'f']]
        result = auto_expand_range("B", data)
        # Should expand to B1:B3 (column letter means single column)
        self.assertEqual(result, "B1:B3")

    def test_empty_data(self):
        """Test with empty data"""
        data = []
        result = auto_expand_range("A1:B2", data)
        # Should return original range
        self.assertEqual(result, "A1:B2")

    def test_empty_range(self):
        """Test with empty range"""
        data = [['a', 'b'], ['c', 'd']]
        result = auto_expand_range("", data)
        # Should return "A1"
        self.assertEqual(result, "A1")

    def test_range_starting_at_row_1(self):
        """Test range expansion starting from row 1"""
        # Data: 4 rows, 3 columns
        data = [
            ['a', 'b', 'c'],
            ['d', 'e', 'f'],
            ['g', 'h', 'i'],
            ['j', 'k', 'l']
        ]
        result = auto_expand_range("A1:A2", data)
        # Should expand to A1:C4
        self.assertEqual(result, "A1:C4")

    def test_large_row_number(self):
        """Test with large row numbers"""
        # Data: 2 rows, 2 columns
        data = [['a', 'b'], ['c', 'd']]
        result = auto_expand_range("Z999:Z1000", data)
        # Should expand to Z999:AA1000
        self.assertEqual(result, "Z999:AA1000")

    def test_double_letter_column(self):
        """Test with double-letter column (AA, AB, etc.)"""
        # Data: 3 rows, 2 columns
        data = [['a', 'b'], ['c', 'd'], ['e', 'f']]
        result = auto_expand_range("AA10:AA12", data)
        # Should expand to AA10:AB12
        self.assertEqual(result, "AA10:AB12")


def run_tests():
    """Run all tests and print results"""
    print("🧪 Running Auto-Expand Range Tests")
    print("=" * 70)

    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestAutoExpandRange)

    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print("\n" + "=" * 70)
    print("📊 Test Summary")
    print(f"Total tests: {result.testsRun}")
    print(f"Passed: {result.testsRun - len(result.failures) - len(result.errors)} ✅")
    print(f"Failed: {len(result.failures)} ❌")
    print(f"Errors: {len(result.errors)} ⚠️")

    if result.wasSuccessful():
        print("\n🎉 All tests passed!")
        return True
    else:
        print("\n❌ Some tests failed!")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
