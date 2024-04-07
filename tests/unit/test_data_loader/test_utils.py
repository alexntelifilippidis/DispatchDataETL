from unittest.mock import AsyncMock, patch

import pytest
from data_loader.utils import move_file, read_all_files


@pytest.mark.asyncio
@patch("shutil.move")
async def test_move_file(mock_move):
    # Define source and destination paths
    source_path = "/path/to/source/file.txt"
    destination_path = "/path/to/destination/"

    # Call the move_file function
    await move_file(source_path, destination_path)

    # Assert that shutil.move was called with the correct arguments
    mock_move.assert_called_once_with(source_path, destination_path)
