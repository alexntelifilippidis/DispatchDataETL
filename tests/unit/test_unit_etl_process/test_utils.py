import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from etl_process.utils import check_all_files, deduplicate_data, fetch_and_combine_data, move_file, read_all_files


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
        # Test file movement
        move_file_mock.assert_called_once_with("/path/to/file/file1.dat", "/path/to/destination")
    assert clean_data == [(4, 5, "file2.dat")]
    assert corrupted_files == ["file1.dat"]


@pytest.mark.asyncio
async def test_fetch_and_combine_data():
    # Mocking aiofiles.open to return a MagicMock
    mock_file = AsyncMock()

    # Mock aiofiles.open to return the mock file object
    mock_open = MagicMock(return_value=mock_file)

    with patch("etl_process.utils.aiofiles.open", mock_open):
        # Mocking the reader
        mock_reader = AsyncMock()
        mock_reader.read_data = AsyncMock(
            side_effect=[
                [
                    {
                        "id": 1,
                        "voucher": "ABC123",
                        "measure_datetime": "2024-04-17",
                        "Length": 10,
                        "Height": 5,
                        "Width": 3,
                        "Weight": 50,
                        "source": 1,
                    }
                ],
                [
                    {
                        "id": 2,
                        "voucher": "DEF456",
                        "measure_datetime": "2024-04-16",
                        "Length": 8,
                        "Height": 4,
                        "Width": 2,
                        "Weight": 40,
                        "source": 2,
                    }
                ],
                [
                    {
                        "id": 3,
                        "voucher": "GHI789",
                        "measure_datetime": "2024-04-15",
                        "Length": 12,
                        "Height": 6,
                        "Width": 4,
                        "Weight": 60,
                        "source": 3,
                    }
                ],
            ]
        )

        # Mocking the conf module
        mock_conf = AsyncMock()
        mock_conf.table_name_source_csv = "csv_table"
        mock_conf.table_name_source_dat = "dat_table"
        mock_conf.table_name_source = "mysql_table"

        # Calling the function
        result = await fetch_and_combine_data(mock_reader, mock_conf, dry_run=True)

        # Assertions
        assert len(result) == 3
        assert result[0]["id"] == 1
        assert result[0]["voucher"] == "ABC123"
        assert result[0]["source"] == 1

        assert result[1]["id"] == 2
        assert result[1]["voucher"] == "DEF456"
        assert result[1]["source"] == 2

        assert result[2]["id"] == 3
        assert result[2]["voucher"] == "GHI789"
        assert result[2]["source"] == 3

        # Ensure that read_data was called with the correct parameters
        mock_reader.read_data.assert_any_call(
            table_name="csv_table",
            columns="id,voucher,measure_datetime,Length,Height,Width,Weight,2 as source",
            where_clause="id>0",
            dry_run=True,
        )
        mock_reader.read_data.assert_any_call(
            table_name="dat_table",
            columns="id,b as voucher,measure_datetime,wt as weight,CAST(SUBSTRING_INDEX(d, 'X', 1) AS FLOAT) AS Lenght,CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(d, 'X', 2), ' ', -1) AS FLOAT) AS Height,CAST(SUBSTRING_INDEX(d, 'X', -1) AS FLOAT) AS Width,1 as source",
            where_clause="id>0",
            dry_run=True,
        )
        mock_reader.read_data.assert_any_call(
            table_name="mysql_table",
            columns="Barcode as voucher,IssueDate as measure_datetime,WeightKg as weight,LengthCm AS Lenght,HeightCm AS Height,WidthCm AS Width,3 as source",
            dry_run=True,
        )


@pytest.mark.asyncio
async def test_deduplicate_data():
    # Mock data
    data = [
        {"id": 1, "voucher": "ABC123", "source": 1, "measure_datetime": datetime(2024, 4, 17)},
        {"id": 2, "voucher": "ABC123", "source": 2, "measure_datetime": datetime(2024, 4, 18)},
        {"id": 2, "voucher": "ABC123", "source": 2, "measure_datetime": datetime(2024, 4, 19)},
        {"id": 3, "voucher": "DEF456", "source": 1, "measure_datetime": datetime(2024, 4, 15)},
        {"id": 4, "voucher": "DEF456", "source": 2, "measure_datetime": datetime(2024, 4, 16)},
        {"id": 5, "voucher": "DEF456", "source": 3, "measure_datetime": datetime(2024, 4, 14)},
    ]

    expected_result = [("ABC123", 2, datetime(2024, 4, 19)), ("DEF456", 3, datetime(2024, 4, 14))]

    # Call the function under test
    result = await deduplicate_data(data)

    # Assert the result
    assert result == expected_result
