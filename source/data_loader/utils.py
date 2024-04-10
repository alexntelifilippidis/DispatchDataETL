import asyncio
import shutil
from typing import Any, List

from data_loader.abstract_data_loader import AbstractDataReader
from data_loader.logger import MyLogger

my_logger = MyLogger("my_logger")
logger = my_logger.get_logger()


async def move_file(source_path: str, destination_dir: str) -> None:
    """
    Move a file asynchronously from a source path to a destination path.

    Parameters:
        source_path (str): The path of the file to move.
        destination_dir (str): The path where the file should be moved to.
    """
    shutil.move(source_path, destination_dir)
    logger.info(f"move {source_path} to {destination_dir}")


async def read_all_files(reader: AbstractDataReader, file_paths: List[str], destination_dir: str) -> list[Any]:
    """
    Read data from all files asynchronously.

    Parameters:
        reader (AbstractDataReader): An instance of a class that implements the AbstractDataReader interface.
        file_paths (list[str]): A list of file paths to read.
        **kwargs: Additional keyword arguments to be passed to the read_data method of the reader.

    Returns:
        list[Any]: A list containing the results of reading data from all files.
    """
    tasks = [reader.read_data(file_path=file_path, destination_dir=destination_dir) for file_path in file_paths]
    return await asyncio.gather(*tasks)
