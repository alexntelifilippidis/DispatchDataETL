import asyncio
import os
import shutil
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union

import aiofiles
from etl_process.abstract_data_loader import AbstractDataReader
from etl_process.logger import MyLogger
from isort.place import module

my_logger = MyLogger("DDELogger")
logger = my_logger.get_logger()


async def move_file(source_path: str, destination_dir: str) -> None:
    """
    Move a file asynchronously from a source path to a destination path.

    :param source_path: The path of the file to move.
    :type source_path: str
    :param destination_dir: The path where the file should be moved to.
    :type destination_dir: str
    """
    shutil.move(source_path, destination_dir)


async def read_all_files(
    reader: AbstractDataReader, file_paths: List[str], destination_dir: str, dry_run: bool = False
) -> list[Any]:
    """
    Read data from all files asynchronously.

    :param reader: An instance of a class that implements the AbstractDataReader interface.
    :type reader: AbstractDataReader
    :param file_paths: A list of file paths to read.
    :type file_paths: list[str]
    :param destination_dir: The destination dir path
    :type destination_dir: str
    :param dry_run: Flag indicating whether it's a dry run or not.
    :type dry_run: bool
    :return: A list containing the results of reading data from all files.
    """
    if dry_run:
        logger.info("Performing dry run. No file will be moved to destination dir")
    tasks = [reader.read_data(file_path=file_path, destination_dir=destination_dir, dry_run=dry_run) for file_path in file_paths]
    results = await asyncio.gather(*tasks)
    return results


async def check_all_files(
    reader: AbstractDataReader,
    data: List[Tuple],
    corrupted_files: List,
    file_path: str,
    destination_dir: str,
    dry_run: bool = False,
) -> List[tuple]:
    """
    Check the format and content of the provided data tuples.
    Remove all data from the corrupted files.
    Move the corrupted files to another folder.

    :param reader: An instance of a class that implements the AbstractDataReader interface.
    :type reader: AbstractDataReader
    :param data: A list of tuples, each containing data in the specified format.
    :type data: List[Tuple]
    :param corrupted_files: A list with the corrupted files come from reader.transform_data.
    :type corrupted_files: List
    :param file_path: The path to the DAT file to read.
    :type file_path: str
    :param destination_dir: The path to move the DAT file to after reading.
    :type destination_dir: str
    :param dry_run: Flag indicating whether it's a dry run or not.
    :type dry_run: bool
    :return: A list of clean data.
    """
    tasks = [reader.check_data(data_tuple) for data_tuple in data]
    results = await asyncio.gather(*tasks)
    corrupted_files += list(set([filename for filename in results if filename]))

    clean_data = []
    for data_tuple in data:
        if data_tuple[-1] not in corrupted_files:
            clean_data.append(data_tuple)

    if corrupted_files:
        if not dry_run:
            for file in corrupted_files:
                await move_file(os.path.join(file_path, file), destination_dir)
            logger.warning(
                f"""Corrupted files moved to {destination_dir}
                                                Corrupted Files: {corrupted_files}"""
            )
        else:
            logger.info("Performing dry run. No file will be moved to destination dir after check")
            logger.warning(
                f"""Corrupted files not moved to {destination_dir}
                                                Corrupted Files: {corrupted_files}"""
            )

    return clean_data


async def fetch_and_combine_data(
    reader: AbstractDataReader, conf: "module", dry_run: bool = False
) -> List[Dict[str, Union[int, float, str]]]:
    """Fetches data from MySQL databases and combines the results into one list of dictionaries.

    :param reader: An instance of a class that implements the AbstractDataReader interface.
    :type reader: AbstractDataReader
    :param conf: The configuration module.
    :type conf: module
    :param dry_run: Flag indicating whether to perform a dry run (default is False).
    :type dry_run: bool
    :return: Combined data from all three sources.
    :rtype: List[Dict[str, Union[int, float, str]]]
    """
    # Read maximum IDs from file
    max_dat_id: int = 0
    max_csv_id: int = 0
    async with aiofiles.open("max_id.txt", "r") as f:
        async for line in f:
            if line.startswith("Maximum Dat ID:"):
                max_dat_id = int(line.split(":")[1])
            elif line.startswith("Maximum CSV ID:"):
                max_csv_id = int(line.split(":")[1])

    # Fetch data from MySQL databases
    source_csv_data_task = reader.read_data(
        table_name=conf.table_name_source_csv,
        columns="id,voucher,measure_datetime,Length,Height,Width,Weight,2 as source",
        where_clause=f"id>{max_csv_id}",
        dry_run=dry_run,
    )
    source_dat_data_task = reader.read_data(
        table_name=conf.table_name_source_dat,
        columns="id,b as voucher,measure_datetime,wt as weight,CAST(SUBSTRING_INDEX(d, 'X', 1) AS FLOAT) AS Lenght,"
        "CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(d, 'X', 2), ' ', -1) AS FLOAT) AS Height,"
        "CAST(SUBSTRING_INDEX(d, 'X', -1) AS FLOAT) AS Width,1 as source",
        where_clause=f"id>{max_dat_id}",
        dry_run=dry_run,
    )
    mysql_task = reader.read_data(
        table_name=conf.table_name_source,
        columns="Barcode as voucher,IssueDate as measure_datetime,WeightKg as weight,LengthCm AS Lenght,"
        "HeightCm AS Height,WidthCm AS Width,3 as source",
        dry_run=dry_run,
    )
    csv_data_source, dat_data_source, mysql_data = await asyncio.gather(source_csv_data_task, source_dat_data_task, mysql_task)

    # Combine the results into one list of dictionaries
    combined_data: List[Dict[str, Union[int, float, str]]] = []
    combined_data.extend(csv_data_source)
    combined_data.extend(dat_data_source)
    combined_data.extend(mysql_data)

    # Update maximum IDs in the file
    max_dat_id_new = max(row["id"] for row in dat_data_source) if dat_data_source else max_dat_id
    max_csv_id_new = max(row["id"] for row in csv_data_source) if csv_data_source else max_csv_id
    async with aiofiles.open("max_id.txt", "w") as f:
        await f.write(f"Maximum Dat ID: {max_dat_id_new}\n")
        await f.write(f"Maximum CSV ID: {max_csv_id_new}\n")

    return combined_data


async def deduplicate_data(data: List[Dict[str, Any]]) -> List[Tuple]:
    """Deduplicates a list of dictionaries based on 'voucher', keeping entries with the biggest 'source'
    and then the biggest 'measure_datetime'.

    :param data: List of dictionaries containing the data to be deduplicated.
    :type data: List[Dict[str, Any]]
    :return: Deduplicated list of tuples.
    :rtype: List[Tuple]
    """
    # Step 1: Group the entries by their 'voucher' key
    grouped_data: Dict = {}
    for entry in data:
        voucher = entry.get("voucher")
        if voucher not in grouped_data:
            grouped_data[voucher] = []
        grouped_data[voucher].append(entry)

    # Step 2-5: Deduplicate entries within each group
    deduplicated_data = []
    for voucher, entries in grouped_data.items():
        entries.sort(key=lambda x: (x.get("source", 0), x.get("measure_datetime", datetime.min)), reverse=True)
        deduplicated_data.append(tuple(v for k, v in entries[0].items() if k != "id"))

    return deduplicated_data
