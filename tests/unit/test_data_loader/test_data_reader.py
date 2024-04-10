import datetime
import os
import subprocess
from decimal import Decimal

import pytest
from data_loader.data_reader import CSVDataReader, DATDataReader, MySQLDataReader
from data_loader.utils import move_file


@pytest.mark.asyncio
async def test_read_data_csv(expected_csv_data):
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
    print("***dest_path*** : ", os.path.join(destination_dir, os.path.basename(csv_file_path)))
    output = subprocess.check_output("ls -l", shell=True, stderr=subprocess.STDOUT)
    print(output.decode("utf-8"))  # Decode bytes to string and print
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


@pytest.mark.asyncio
async def test_transform_data_csv(expected_dat_data):
    data = [
        [
            [
                "01",
                "01",
                "19112930098",
                "67254036AY",
                "700025218874",
                "1685623517595_700025218874_01-06-2023-15-45-17.jpeg",
                "1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg",
                "2023-06-01-15-45-17",
                "42",
                "13,5",
                "24",
                "15,28",
                "13,6",
            ],
            [
                "01",
                "01",
                "19112930098",
                "67254036AY",
                "700025218874",
                "1685623531229_700025218874_01-06-2023-15-45-31.jpeg",
                "1685623531229_700025218874_01-06-2023-15-45-31_C.jpeg",
                "2023-06-01-15-45-31",
                "41,5",
                "13,5",
                "24",
                "15,28",
                "13,4",
            ],
        ]
    ]
    csv_reader = CSVDataReader()

    # Read data from the CSV file
    result = await csv_reader.transform_data(data=data)
    expected_result = [
        (
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025218874",
            "1685623517595_700025218874_01-06-2023-15-45-17.jpeg",
            "1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg",
            datetime.datetime(2023, 6, 1, 15, 45, 17),
            42,
            13.5,
            24,
            15.28,
            13.6,
        ),
        (
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025218874",
            "1685623531229_700025218874_01-06-2023-15-45-31.jpeg",
            "1685623531229_700025218874_01-06-2023-15-45-31_C.jpeg",
            datetime.datetime(2023, 6, 1, 15, 45, 31),
            41.5,
            13.5,
            24,
            15.28,
            13.4,
        ),
    ]

    assert result == expected_result


@pytest.mark.asyncio
async def test_transform_data_dat(expected_dat_data):

    data = [
        [
            [
                "Sequence=8747",
                "PLACE=PATRA",
                "STATION=    1",
                "DATETIME=20231020122340",
                "WU=kg",
                "WS=0000",
                "WT=  18.660",
                "DU=cm",
                "DS=0000",
                "D= 85.0X 53.0X 20.0",
                "VOLUME= 90.10",
                "B=700028655189",
                "CHK=45",
            ],
            [
                "Sequence=8748",
                "PLACE=PATRA",
                "STATION=    1",
                "DATETIME=20231020122406",
                "WU=kg",
                "WS=0000",
                "WT=   5.220",
                "DU=cm",
                "DS=0000",
                "D= 33.5X 23.5X 28.5",
                "VOLUME= 22.44",
                "B=700028665730",
                "CHK=35",
            ],
        ]
    ]
    dat_reader = DATDataReader()

    # Read data from the CSV file
    result = await dat_reader.transform_data(data=data)
    expected_result = [
        (
            8747,
            "PATRA",
            "1",
            datetime.datetime(2023, 10, 20, 12, 23, 40),
            "kg",
            "0000",
            18.66,
            "cm",
            "0000",
            "85.0X 53.0X 20.0",
            90.1,
            "700028655189",
            "45",
        ),
        (
            8748,
            "PATRA",
            "1",
            datetime.datetime(2023, 10, 20, 12, 24, 6),
            "kg",
            "0000",
            5.22,
            "cm",
            "0000",
            "33.5X 23.5X 28.5",
            22.44,
            "700028665730",
            "35",
        ),
    ]

    assert result == expected_result
