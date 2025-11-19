"""Unit tests for formula_adapter module

Tests the formula adaptation logic for copying formulas with proper handling
of absolute ($) and relative cell references.
"""

import sys
import os
import pytest
import importlib.util

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import formula_adapter module directly without triggering __init__.py
spec = importlib.util.spec_from_file_location(
    "formula_adapter",
    os.path.join(os.path.dirname(__file__), '..', 'datatable_tools', 'formula_adapter.py')
)
formula_adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(formula_adapter)

# Now we can use the functions
adapt_formula = formula_adapter.adapt_formula
adapt_cell_reference = formula_adapter.adapt_cell_reference
adapt_range_reference = formula_adapter.adapt_range_reference
column_letter_to_index = formula_adapter.column_letter_to_index
column_index_to_letter = formula_adapter.column_index_to_letter


class TestColumnConversion:
    """Test column letter/index conversion utilities"""

    def test_column_letter_to_index(self):
        assert column_letter_to_index('A') == 0
        assert column_letter_to_index('B') == 1
        assert column_letter_to_index('Z') == 25
        assert column_letter_to_index('AA') == 26
        assert column_letter_to_index('AB') == 27
        assert column_letter_to_index('ZZ') == 701

    def test_column_index_to_letter(self):
        assert column_index_to_letter(0) == 'A'
        assert column_index_to_letter(1) == 'B'
        assert column_index_to_letter(25) == 'Z'
        assert column_index_to_letter(26) == 'AA'
        assert column_index_to_letter(27) == 'AB'
        assert column_index_to_letter(701) == 'ZZ'


class TestAdaptCellReference:
    """Test single cell reference adaptation"""

    def test_relative_reference(self):
        """Both row and column change"""
        assert adapt_cell_reference("A1", row_offset=1, col_offset=0) == "A2"
        assert adapt_cell_reference("A1", row_offset=0, col_offset=1) == "B1"
        assert adapt_cell_reference("A1", row_offset=1, col_offset=1) == "B2"
        assert adapt_cell_reference("B5", row_offset=2, col_offset=3) == "E7"

    def test_absolute_column(self):
        """Column fixed, row changes"""
        assert adapt_cell_reference("$A1", row_offset=1, col_offset=0) == "$A2"
        assert adapt_cell_reference("$A1", row_offset=0, col_offset=1) == "$A1"
        assert adapt_cell_reference("$A5", row_offset=1, col_offset=1) == "$A6"

    def test_absolute_row(self):
        """Row fixed, column changes"""
        assert adapt_cell_reference("A$1", row_offset=1, col_offset=0) == "A$1"
        assert adapt_cell_reference("A$1", row_offset=0, col_offset=1) == "B$1"
        assert adapt_cell_reference("B$1", row_offset=1, col_offset=1) == "C$1"

    def test_absolute_cell(self):
        """Both row and column fixed"""
        assert adapt_cell_reference("$A$1", row_offset=1, col_offset=1) == "$A$1"
        assert adapt_cell_reference("$B$5", row_offset=10, col_offset=10) == "$B$5"

    def test_negative_offsets(self):
        """Test moving up/left"""
        assert adapt_cell_reference("C5", row_offset=-2, col_offset=-1) == "B3"
        assert adapt_cell_reference("A1", row_offset=-1, col_offset=-1) == "A1"  # Can't go below A1

    def test_multicharacter_columns(self):
        """Test columns like AA, AB, etc."""
        assert adapt_cell_reference("AA10", row_offset=0, col_offset=1) == "AB10"
        assert adapt_cell_reference("$AA10", row_offset=0, col_offset=1) == "$AA10"
        assert adapt_cell_reference("AA$10", row_offset=1, col_offset=1) == "AB$10"


class TestAdaptRangeReference:
    """Test range reference adaptation"""

    def test_cell_range(self):
        """Test simple cell ranges"""
        assert adapt_range_reference("A1:B10", row_offset=1, col_offset=0) == "A2:B11"
        assert adapt_range_reference("A1:B10", row_offset=0, col_offset=1) == "B1:C10"
        assert adapt_range_reference("A1:B10", row_offset=1, col_offset=1) == "B2:C11"

    def test_absolute_ranges(self):
        """Test ranges with absolute references"""
        assert adapt_range_reference("$A$1:$B$10", row_offset=1, col_offset=1) == "$A$1:$B$10"
        assert adapt_range_reference("$A1:$B10", row_offset=1, col_offset=1) == "$A2:$B11"
        assert adapt_range_reference("A$1:B$10", row_offset=1, col_offset=1) == "B$1:C$10"

    def test_column_ranges(self):
        """Test column-only ranges (e.g., A:A)"""
        assert adapt_range_reference("A:A", row_offset=0, col_offset=1) == "B:B"
        assert adapt_range_reference("$A:$A", row_offset=0, col_offset=1) == "$A:$A"
        assert adapt_range_reference("A:B", row_offset=0, col_offset=1) == "B:C"

    def test_row_ranges(self):
        """Test row-only ranges (e.g., 1:1)"""
        assert adapt_range_reference("1:1", row_offset=1, col_offset=0) == "2:2"
        assert adapt_range_reference("$1:$1", row_offset=1, col_offset=0) == "$1:$1"
        assert adapt_range_reference("1:10", row_offset=2, col_offset=0) == "3:12"

    def test_sheet_qualified_ranges(self):
        """Test ranges with sheet references"""
        assert adapt_range_reference("'Sheet1'!A1:B10", row_offset=1, col_offset=0) == "'Sheet1'!A2:B11"
        assert adapt_range_reference("Sheet1!A1", row_offset=1, col_offset=1) == "Sheet1!B2"
        assert adapt_range_reference("'Other Sheet'!$A$1:$B$10", row_offset=1, col_offset=1) == "'Other Sheet'!$A$1:$B$10"


class TestAdaptFormula:
    """Test complete formula adaptation"""

    def test_simple_sum(self):
        """Test basic SUM formula"""
        assert adapt_formula("=SUM(A1:A10)", row_offset=1, col_offset=0) == "=SUM(A2:A11)"
        assert adapt_formula("=SUM(A1:A10)", row_offset=0, col_offset=1) == "=SUM(B1:B10)"  # Fixed: B1:B10 not B11

    def test_formula_from_requirement(self):
        """Test the example from the requirement document"""
        # Original formula in B5:
        # =SUMIFS('1.库存台账'!$J:$J,'1.库存台账'!$F:$F,$A5,'1.库存台账'!$A:$A,B$1)
        #
        # When copied from B5 to B6 (row_offset=1, col_offset=0):
        # - $J:$J stays (absolute column range)
        # - $F:$F stays (absolute column range)
        # - $A5 -> $A6 (absolute column, relative row)
        # - $A:$A stays (absolute column range)
        # - B$1 stays (relative column, absolute row)

        original = "=SUMIFS('1.库存台账'!$J:$J,'1.库存台账'!$F:$F,$A5,'1.库存台账'!$A:$A,B$1)"
        expected = "=SUMIFS('1.库存台账'!$J:$J,'1.库存台账'!$F:$F,$A6,'1.库存台账'!$A:$A,B$1)"
        result = adapt_formula(original, row_offset=1, col_offset=0)
        assert result == expected

        # When copied from B5 to C5 (row_offset=0, col_offset=1):
        # - $J:$J stays (absolute column range)
        # - $F:$F stays (absolute column range)
        # - $A5 stays (absolute column, same row)
        # - $A:$A stays (absolute column range)
        # - B$1 -> C$1 (relative column, absolute row)

        expected_col = "=SUMIFS('1.库存台账'!$J:$J,'1.库存台账'!$F:$F,$A5,'1.库存台账'!$A:$A,C$1)"
        result_col = adapt_formula(original, row_offset=0, col_offset=1)
        assert result_col == expected_col

    def test_multiple_references(self):
        """Test formula with multiple cell references"""
        formula = "=IF(A1>10,SUM($B$1:$B$10),C1)"
        expected = "=IF(B3>10,SUM($B$1:$B$10),D3)"
        assert adapt_formula(formula, row_offset=2, col_offset=1) == expected

    def test_cross_sheet_references(self):
        """Test formulas with multiple sheet references"""
        formula = "='Sheet1'!A1+'Sheet2'!B2"
        expected = "='Sheet1'!A2+'Sheet2'!B3"  # Fixed: keep the = sign
        assert adapt_formula(formula, row_offset=1, col_offset=0) == expected

    def test_named_ranges_unchanged(self):
        """Named ranges should not be adapted"""
        formula = "=SUM(Revenue)"
        assert adapt_formula(formula, row_offset=1, col_offset=0) == "=SUM(Revenue)"

        formula = "=VLOOKUP(A1,DataTable,2,FALSE)"
        expected = "=VLOOKUP(B2,DataTable,2,FALSE)"
        assert adapt_formula(formula, row_offset=1, col_offset=1) == expected

    def test_formula_without_equals(self):
        """Test formula without leading ="""
        formula = "SUM(A1:A10)"
        expected = "SUM(A2:A11)"
        assert adapt_formula(formula, row_offset=1, col_offset=0) == expected

    def test_complex_nested_formula(self):
        """Test complex nested formula"""
        formula = "=IF(AND(A1>0,B1<100),SUM($C$1:$C$10)*A1,AVERAGE(D1:D10))"
        expected = "=IF(AND(B2>0,C2<100),SUM($C$1:$C$10)*B2,AVERAGE(E2:E11))"
        assert adapt_formula(formula, row_offset=1, col_offset=1) == expected

    def test_empty_formula(self):
        """Test empty or None formula"""
        assert adapt_formula("", row_offset=1, col_offset=0) == ""
        assert adapt_formula(None, row_offset=1, col_offset=0) is None

    def test_formula_with_text_strings(self):
        """Test that text strings in formulas are not affected"""
        # Text strings should not be confused with cell references
        formula = '=CONCATENATE("A1", A1, "B2")'
        expected = '=CONCATENATE("A1", B2, "B2")'
        assert adapt_formula(formula, row_offset=1, col_offset=1) == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
