import asyncio
import os

from config import csv_destination_dir, csv_dir, dat_destination_dir, dat_dir
from data_loader.data_reader import CSVDataReader, DATDataReader
from data_loader.utils import read_all_files


async def main() -> None:
    """Main function to demonstrate usage."""

    csv_file_reader = CSVDataReader()
    dat_file_reader = DATDataReader()

    # Get list of all files in the directory
    all_csv_files = [os.path.join(csv_dir, file) for file in os.listdir(csv_dir)]
    all_dat_files = [os.path.join(dat_dir, file) for file in os.listdir(dat_dir)]

    # Filter only CSV files
    csv_files = [file for file in all_csv_files if file.endswith(".csv")]

    # Filter only DAT files
    dat_files = [file for file in all_dat_files if file.endswith(".dat")]

    # Gather file reading tasks
    csv_task = read_all_files(
        reader=csv_file_reader,
        file_paths=csv_files,
        destination_dir=csv_destination_dir,
    )
    dat_task = read_all_files(
        reader=dat_file_reader,
        file_paths=dat_files,
        destination_dir=dat_destination_dir,
    )

    # Execute tasks concurrently
    csv_data = await csv_task
    dat_data = await dat_task

    print("CSV data:", csv_data)
    print("DAT data:", dat_data)


if __name__ == "__main__":
    asyncio.run(main())
