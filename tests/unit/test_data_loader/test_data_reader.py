import os
from unittest.mock import MagicMock, patch

import pytest
from data_loader.data_reader import CSVDataReader, DATDataReader, MySQLDataReader
from data_loader.utils import move_file


@pytest.mark.asyncio
async def test_read_data(expected_csv_data):
    from config import csv_destination_dir, csv_dir

    file_name = "01062023_155202_01_01_19112930098.csv"
    # Use an existing path for the CSV file
    csv_file_path = csv_dir + "/" + file_name

    # Specify the destination directory
    destination_dir = csv_destination_dir

    # Create an instance of CSVDataReader
    csv_reader = CSVDataReader()

    # Read data from the CSV file
    data = await csv_reader.read_data(file_path=csv_file_path, destination_dir=destination_dir)

    expected_data = expected_csv_data

    # Assert that data is correct
    assert data == expected_data
    # Assert that data is correctly read
    assert len(data) > 0  # Assuming the file has at least one row of data
    # Assert that the file is moved to the destination directory
    assert os.path.exists(os.path.join(destination_dir, os.path.basename(csv_file_path)))

    # Move the file back to the original directory
    await move_file(source_path=destination_dir + "/" + file_name, destination_dir=csv_dir)


@pytest.mark.asyncio
async def test_read_data_dat(expected_dat_data):
    from config import dat_destination_dir, dat_dir

    file_name = "20231020152446.dat"
    # Use an existing path for the DAT file
    dat_file_path = dat_dir + "/" + file_name

    # Specify the destination directory
    destination_dir = dat_destination_dir

    # Create an instance of DATDataReader
    dat_reader = DATDataReader()

    # Read data from the DAT file
    data = await dat_reader.read_data(file_path=dat_file_path, destination_dir=destination_dir)

    expected_data = expected_dat_data

    # Assert that data is correct
    assert data == expected_data
    # Assert that data is correctly read
    assert len(data) > 0  # Assuming the file has at least one row of data
    # Assert that the file is moved to the destination directory
    assert os.path.exists(os.path.join(destination_dir, os.path.basename(dat_file_path)))

    # Move the file back to the original directory for checking the data
    await move_file(source_path=destination_dir + "/" + file_name, destination_dir=dat_dir)
