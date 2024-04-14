import asyncio
from unittest.mock import AsyncMock, call, patch

import pytest
from etl_process.utils import check_all_files, move_file, read_all_files


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
        call(file_path=file_paths[0], destination_dir=destination_dir, dry_run=False),
        call(file_path=file_paths[1], destination_dir=destination_dir, dry_run=False),
    ]
    mock_reader.read_data.assert_has_calls(expected_calls)


@pytest.mark.asyncio
async def test_check_all_files():
    reader = AsyncMock()
    reader.check_data.return_value = ["file1.dat"]
    data = [(1, 2, "file1.dat"), (4, 5, "file2.dat")]
    corrupted_files = []
    file_path = "/path/to/file"
    destination_dir = "/path/to/destination"
    reader.check_data.side_effect = ["", "", "file1.dat", ""]

    # Test with clean data
    clean_data = await check_all_files(reader, data, corrupted_files, file_path, destination_dir)
    assert clean_data == data

    # Test with corrupted data]
    with patch("etl_process.utils.move_file") as move_file_mock:
        clean_data = await check_all_files(reader, data, corrupted_files, file_path, destination_dir)
    assert clean_data == [(4, 5, "file2.dat")]
    assert corrupted_files == ["file1.dat"]
    # Test file movement
    move_file_mock.assert_called_once_with("/path/to/file\\file1.dat", "/path/to/destination")
