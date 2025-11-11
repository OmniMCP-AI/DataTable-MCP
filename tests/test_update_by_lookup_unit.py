#!/usr/bin/env python3
"""
Unit tests for update_by_lookup method (no server required)

Tests the core logic of GoogleSheetDataTable.update_by_lookup()
using mock Google Sheets service.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datatable_tools.third_party.google_sheets.datatable import GoogleSheetDataTable


class TestUpdateByLookup:
    """Unit tests for update_by_lookup functionality"""

    @pytest.fixture
    def mock_service(self):
        """Create a mock Google Sheets service"""
        service = MagicMock()
        return service

    @pytest.fixture
    def google_sheet(self):
        """Create a GoogleSheetDataTable instance"""
        return GoogleSheetDataTable()

    @pytest.mark.asyncio
    async def test_basic_lookup_update(self, google_sheet, mock_service):
        """Test basic lookup and update of subset of columns"""
        # Mock load_data_table to return existing data
        existing_data = [
            {"username": "@user1", "display_name": "User One", "status": "active", "score": "100"},
            {"username": "@user2", "display_name": "User Two", "status": "inactive", "score": "50"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            with patch.object(google_sheet, 'update_range') as mock_update:
                mock_update.return_value = {
                    'success': True,
                    'message': 'Updated successfully',
                    'updated_cells': 2
                }

                # Perform update by lookup
                update_data = [
                    {"username": "@user1", "status": "updated"},
                    {"username": "@user2", "status": "updated"}
                ]

                result = await google_sheet.update_by_lookup(
                    mock_service,
                    "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                    update_data,
                    on="username"
                )

                # Verify update_range was called
                assert mock_update.called
                call_args = mock_update.call_args[0]

                # Check the data passed to update_range
                write_data = call_args[2]  # Third argument is data
                headers = write_data[0]
                rows = write_data[1:]

                # Headers should remain the same
                assert headers == ["username", "display_name", "status", "score"]

                # Row data should be updated
                assert rows[0] == ["@user1", "User One", "updated", "100"]  # status updated
                assert rows[1] == ["@user2", "User Two", "updated", "50"]  # status updated

                # Verify display_name and score were preserved
                assert result['success'] == True

    @pytest.mark.asyncio
    async def test_case_insensitive_matching(self, google_sheet, mock_service):
        """Test case-insensitive lookup matching"""
        existing_data = [
            {"username": "@User1", "status": "active"},
            {"username": "@USER2", "status": "inactive"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            with patch.object(google_sheet, 'update_range') as mock_update:
                mock_update.return_value = {'success': True, 'updated_cells': 2}

                # Update with lowercase usernames
                update_data = [
                    {"username": "@user1", "status": "updated"},  # lowercase
                    {"username": "@user2", "status": "updated"},  # lowercase
                ]

                result = await google_sheet.update_by_lookup(
                    mock_service,
                    "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                    update_data,
                    on="username"
                )

                # Verify matches were found despite case differences
                call_args = mock_update.call_args[0]
                write_data = call_args[2]
                rows = write_data[1:]

                assert rows[0][1] == "updated"  # First row status updated
                assert rows[1][1] == "updated"  # Second row status updated

    @pytest.mark.asyncio
    async def test_create_new_columns(self, google_sheet, mock_service):
        """Test adding new columns when create_new_columns=True"""
        existing_data = [
            {"username": "@user1", "status": "active"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            with patch.object(google_sheet, 'update_range') as mock_update:
                mock_update.return_value = {'success': True}

                # Update with new columns
                update_data = [
                    {"username": "@user1", "status": "updated", "new_col1": "val1", "new_col2": "val2"},
                ]

                result = await google_sheet.update_by_lookup(
                    mock_service,
                    "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                    update_data,
                    on="username",
                    create_new_columns=True
                )

                # Verify new columns were added
                call_args = mock_update.call_args[0]
                write_data = call_args[2]
                headers = write_data[0]

                assert "new_col1" in headers
                assert "new_col2" in headers
                # New columns should be at the end (order may vary)
                assert set(headers[-2:]) == {"new_col1", "new_col2"}

    @pytest.mark.asyncio
    async def test_ignore_new_columns_by_default(self, google_sheet, mock_service):
        """Test that new columns are ignored when create_new_columns=False"""
        existing_data = [
            {"username": "@user1", "status": "active"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            with patch.object(google_sheet, 'update_range') as mock_update:
                mock_update.return_value = {'success': True}

                # Update with new columns
                update_data = [
                    {"username": "@user1", "status": "updated", "new_col": "value"},
                ]

                result = await google_sheet.update_by_lookup(
                    mock_service,
                    "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                    update_data,
                    on="username",
                    create_new_columns=False  # Default
                )

                # Verify new columns were NOT added
                call_args = mock_update.call_args[0]
                write_data = call_args[2]
                headers = write_data[0]

                assert "new_col" not in headers
                assert headers == ["username", "status"]

    @pytest.mark.asyncio
    async def test_override_empty_values(self, google_sheet, mock_service):
        """Test override=True clears cells with empty values"""
        existing_data = [
            {"username": "@user1", "status": "active", "score": "100"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            with patch.object(google_sheet, 'update_range') as mock_update:
                mock_update.return_value = {'success': True}

                # Update with empty value
                update_data = [
                    {"username": "@user1", "status": "", "score": "150"},  # Empty status
                ]

                result = await google_sheet.update_by_lookup(
                    mock_service,
                    "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                    update_data,
                    on="username",
                    override=True
                )

                # Verify empty value cleared the cell
                call_args = mock_update.call_args[0]
                write_data = call_args[2]
                rows = write_data[1:]

                assert rows[0] == ["@user1", "", "150"]  # status cleared, score updated

    @pytest.mark.asyncio
    async def test_preserve_empty_values_by_default(self, google_sheet, mock_service):
        """Test override=False preserves existing values when update has empty"""
        existing_data = [
            {"username": "@user1", "status": "active", "score": "100"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            with patch.object(google_sheet, 'update_range') as mock_update:
                mock_update.return_value = {'success': True}

                # Update with empty value
                update_data = [
                    {"username": "@user1", "status": "", "score": "150"},  # Empty status
                ]

                result = await google_sheet.update_by_lookup(
                    mock_service,
                    "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                    update_data,
                    on="username",
                    override=False  # Default
                )

                # Verify empty value preserved existing
                call_args = mock_update.call_args[0]
                write_data = call_args[2]
                rows = write_data[1:]

                assert rows[0] == ["@user1", "active", "150"]  # status preserved, score updated

    @pytest.mark.asyncio
    async def test_error_lookup_column_missing_in_sheet(self, google_sheet, mock_service):
        """Test error when lookup column doesn't exist in sheet"""
        existing_data = [
            {"username": "@user1", "status": "active"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            update_data = [
                {"username": "@user1", "status": "updated", "email": "user1@example.com"},
            ]

            result = await google_sheet.update_by_lookup(
                mock_service,
                "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                update_data,
                on="email"  # Exists in update data but not in sheet
            )

            assert result.success == False or result['success'] == False
            error_msg = result.error if hasattr(result, 'error') else result.get('error', '')
            assert "not found in sheet" in error_msg

    @pytest.mark.asyncio
    async def test_error_lookup_column_missing_in_data(self, google_sheet, mock_service):
        """Test error when lookup column doesn't exist in update data"""
        update_data = [
            {"status": "updated"},  # Missing 'username' column
        ]

        result = await google_sheet.update_by_lookup(
            mock_service,
            "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
            update_data,
            on="username"
        )

        assert result.success == False or result['success'] == False
        error_msg = result.error if hasattr(result, 'error') else result.get('error', '')
        assert "not found in all rows" in error_msg

    @pytest.mark.asyncio
    async def test_unmatched_rows_ignored(self, google_sheet, mock_service):
        """Test that unmatched rows in update data are silently ignored"""
        existing_data = [
            {"username": "@user1", "status": "active"},
        ]

        load_response = {
            'success': True,
            'data': existing_data,
            'source_info': {
                'spreadsheet_id': 'test123',
                'worksheet': 'Sheet1',
                'worksheet_url': 'https://docs.google.com/spreadsheets/d/test123/edit#gid=0'
            }
        }

        with patch.object(google_sheet, 'load_data_table', return_value=load_response):
            with patch.object(google_sheet, 'update_range') as mock_update:
                mock_update.return_value = {'success': True}

                # Update with some matching and some non-matching
                update_data = [
                    {"username": "@user1", "status": "updated"},  # Exists
                    {"username": "@nonexistent", "status": "new"},  # Doesn't exist
                ]

                result = await google_sheet.update_by_lookup(
                    mock_service,
                    "https://docs.google.com/spreadsheets/d/test123/edit#gid=0",
                    update_data,
                    on="username"
                )

                # Should succeed, only @user1 updated
                assert result['success'] == True
                assert "1 rows matched, 1 unmatched" in result['message']


def test_sync_wrapper():
    """Wrapper to run async tests"""
    # This will be picked up by pytest
    pass


if __name__ == "__main__":
    # Run with pytest
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
