import asyncio
import os

import config as conf
from etl_process.data_loader import MySQLDataLoader
from etl_process.data_reader import CSVDataReader, DATDataReader, MySQLDataReader
from etl_process.utils import check_all_files, deduplicate_data, fetch_and_combine_data, logger, my_logger, read_all_files


async def main(dry_run: bool) -> None:
    """
    The main process function that orchestrates the ETL process.

    :param dry_run: Flag indicating whether it's a dry run or not.
    :type dry_run: bool
    """
    # =====================
    #     Bronze Layer
    # =====================
    logger.info("Starting main process")

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
    mysql_data_loader = MySQLDataLoader(
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
        dry_run=dry_run,
    )
    dat_task = read_all_files(
        reader=dat_file_reader,
        file_paths=dat_files,
        destination_dir=conf.dat_destination_dir,
        dry_run=dry_run,
    )

    logger.info("Starting read data process")

    # Gather file reading data
    csv_data, dat_data = await asyncio.gather(csv_task, dat_task)

    await my_logger.log_with_time_elapsed("Finish read data process")
    logger.info("Starting data transforming process")

    # Transform data
    csv_data_transformed, corrupted_csv_files = await csv_file_reader.transform_data(csv_data)
    dat_data_transformed, corrupted_dat_files = await dat_file_reader.transform_data(dat_data)

    await my_logger.log_with_time_elapsed("Finish data transforming process")

    logger.info("Starting data checking process")

    # Check data
    # Gather file reading tasks
    csv_check_task = check_all_files(
        reader=csv_file_reader,
        data=csv_data_transformed,
        corrupted_files=corrupted_csv_files,
        file_path=conf.csv_destination_dir,
        destination_dir=conf.corrupted_csv_destination_dir,
        dry_run=dry_run,
    )
    dat_check_task = check_all_files(
        reader=dat_file_reader,
        data=dat_data_transformed,
        corrupted_files=corrupted_dat_files,
        file_path=conf.dat_destination_dir,
        destination_dir=conf.corrupted_dat_destination_dir,
        dry_run=dry_run,
    )
    # Gather checked data
    csv_data_checked, dat_data_checked = await asyncio.gather(csv_check_task, dat_check_task)

    await my_logger.log_with_time_elapsed("Finish data checking process")
    logger.info("Starting data ingestion to Source DBs")

    # Load data to source tables
    loop = asyncio.get_event_loop()

    await mysql_data_loader.load_data_to_db(
        data=dat_data_checked,
        table_name=conf.table_name_source_dat,
        creation_columns=conf.creation_column_dat,
        chunk_size=conf.chunk_size,
        loop=loop,
        dry_run=dry_run,
    )

    await mysql_data_loader.load_data_to_db(
        data=csv_data_checked,
        table_name=conf.table_name_source_csv,
        creation_columns=conf.creation_column_csv,
        chunk_size=conf.chunk_size,
        loop=loop,
        dry_run=dry_run,
    )

    await my_logger.log_with_time_elapsed("Finish data ingestion to Source DBs")

    # =====================
    #     Silver Layer
    # =====================
    logger.info("Starting fetch and deduplicate source data from DBs")

    # Gather data from bronze tables
    silver_data = await fetch_and_combine_data(reader=mysql_data_reader, conf=conf, dry_run=dry_run)
    # Deduplicate silver data
    silver_deduplicated_data = await deduplicate_data(silver_data)

    await my_logger.log_with_time_elapsed("Finish data fetching and deduplicating from DBs")

    # Load data to silver tables
    await mysql_data_loader.load_data_to_db(
        data=silver_deduplicated_data,
        table_name=conf.silver_table,
        creation_columns=conf.creation_column_silver,
        chunk_size=conf.chunk_size,
        loop=loop,
        deduplication_columns=["voucher"],
        deduplication_method=True,
        dry_run=dry_run,
    )

    await my_logger.log_with_time_elapsed("Finish data ingestion to Silver DBs")


if __name__ == "__main__":
    asyncio.run(main(dry_run=conf.dry_run))
