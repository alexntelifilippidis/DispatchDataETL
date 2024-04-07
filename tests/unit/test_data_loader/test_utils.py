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


@pytest.mark.asyncio
async def test_read_all_files():
    from config import dat_dir

    # Define mock data for file paths and reader result
    file_paths = [dat_dir + "/20231020152446.dat", dat_dir + "/20231020152556.dat"]
    reader_result = ["data1", "data2"]

    # Mock the reader's read_data method
    async def mock_read_data(file_path, **kwargs):
        return f"data from {file_path}"

    with patch("asyncio.gather") as mock_gather:
        # Set up mock behavior for asyncio.gather
        mock_gather.return_value = reader_result

        # Call the read_all_files function
        result = await read_all_files(reader=AsyncMock(), file_paths=file_paths)

        # Assert that asyncio.gather is called with the correct arguments
        mock_gather.assert_called_once_with(
            *[AsyncMock().read_data(path) for path in file_paths]
        )

        # Assert the result
        assert result == reader_result
