import asyncio
import os

import config as conf
from data_loader.data_reader import CSVDataReader, DATDataReader, MySQLDataReader
from data_loader.utils import read_all_files


async def main() -> None:
    """Main function to demonstrate usage."""

    csv_file_reader = CSVDataReader()
    dat_file_reader = DATDataReader()
    mysql_data_reader = MySQLDataReader(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
    )

    # Get list of all files in the directory
    all_csv_files = [os.path.join(conf.csv_dir, file) for file in os.listdir(conf.csv_dir)]
    all_dat_files = [os.path.join(conf.dat_dir, file) for file in os.listdir(conf.dat_dir)]

    # Filter only CSV files
    csv_files = [file for file in all_csv_files if file.endswith(".csv")]

    # Filter only DAT files
    dat_files = [file for file in all_dat_files if file.endswith(".dat")]

    # Gather file reading tasks
    csv_task = read_all_files(
        reader=csv_file_reader,
        file_paths=csv_files,
        destination_dir=conf.csv_destination_dir,
    )
    dat_task = read_all_files(
        reader=dat_file_reader,
        file_paths=dat_files,
        destination_dir=conf.dat_destination_dir,
    )
    mysql_task = mysql_data_reader.read_data(table_name=conf.table_name_source)

    # Execute tasks concurrently
    csv_data = await csv_task
    dat_data = await dat_task
    mysql_data = await mysql_task

    print("CSV data:", csv_data)
    print("DAT data:", dat_data)
    print("MySQL data:", mysql_data)


if __name__ == "__main__":
    asyncio.run(main())
