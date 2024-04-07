import asyncio
from unittest.mock import AsyncMock, call, patch

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


@pytest.mark.asyncio
async def test_read_all_files():
    # Sample data
    file_paths = ["/path/to/file1", "/path/to/file2"]
    destination_dir = "/destination/dir"
    data1 = ["data1"]
    data2 = ["data2"]

    # Mocking AbstractDataReader and its read_data method
    mock_reader = AsyncMock()
    mock_reader.read_data.side_effect = [data1, data2]

    # Calling the function with mocked reader
    result = await read_all_files(mock_reader, file_paths, destination_dir)

    # Asserting the result
    expected_result = [data1, data2]
    assert result == expected_result

    # Asserting the calls to read_data
    expected_calls = [
        call(file_path=file_paths[0], destination_dir=destination_dir),
        call(file_path=file_paths[1], destination_dir=destination_dir),
    ]
    mock_reader.read_data.assert_has_calls(expected_calls)
