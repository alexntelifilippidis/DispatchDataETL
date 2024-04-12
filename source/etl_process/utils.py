import asyncio
import os
import shutil
from typing import Any, List, Tuple

from etl_process.abstract_data_loader import AbstractDataReader
from etl_process.logger import MyLogger

my_logger = MyLogger("DDELogger")
logger = my_logger.get_logger()


async def move_file(source_path: str, destination_dir: str) -> None:
    """
    Move a file asynchronously from a source path to a destination path.

    Parameters:
        source_path (str): The path of the file to move.
        destination_dir (str): The path where the file should be moved to.
    """
    shutil.move(source_path, destination_dir)


async def read_all_files(
    reader: AbstractDataReader, file_paths: List[str], destination_dir: str, dry_run: bool = False
) -> list[Any]:
    """
    Read data from all files asynchronously.

    Parameters:
        reader (AbstractDataReader): An instance of a class that implements the AbstractDataReader interface.
        file_paths (list[str]): A list of file paths to read.
        destination_dir: (str): The destination dir path
        dry_run (bool): Flag indicating whether it's a dry run or not.

    Returns:
        list[Any]: A list containing the results of reading data from all files.

    """
    if dry_run:
        logger.info("Performing dry run. No file will be moved to destination dir")
    tasks = [reader.read_data(file_path=file_path, destination_dir=destination_dir, dry_run=dry_run) for file_path in file_paths]
    results = await asyncio.gather(*tasks)
    return results


async def check_files(
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

    Parameters:
        reader (AbstractDataReader): An instance of a class that implements the AbstractDataReader interface.
        data (List[Tuple]): A list of tuples, each containing data in the specified format.
        corrupted_files (List): A list with the corrupted files come from reader.transform_data.
        file_path (str): The path to the DAT file to read.
        destination_dir (str): The path to move the DAT file to after reading.
        dry_run (bool): Flag indicating whether it's a dry run or not.

    Returns:
        List[str]: A list of clean data.
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
