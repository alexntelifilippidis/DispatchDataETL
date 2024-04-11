import csv
import datetime
import os
from datetime import datetime as dt
from typing import Any, Dict, List, Tuple, Union

import aiomysql
from data_loader.abstract_data_loader import AbstractDataReader
from data_loader.utils import logger, move_file


class CSVDataReader(AbstractDataReader):
    """Class for reading data from CSV files asynchronously."""

    async def read_data(self, file_path: str, destination_dir: str, dry_run: bool = False) -> list[list[str]]:
        """
        Read data from a CSV file asynchronously and move it to a destination path.

        Parameters:
            file_path (str): The path to the CSV file to read.
            destination_dir (str): The path to move the CSV file to after reading.
            dry_run (bool): Flag indicating whether it's a dry run or not.

        Returns:
            List[Dict[str, Any]]: The data read from the CSV file.
        """
        data = []
        with open(file_path, newline="") as file:
            spamreader = csv.reader(file, delimiter=" ")
            for row in spamreader:
                data.append(row[0].split(";") + [f"{os.path.split(file_path)[-1]}"])
        # Move the file after reading
        if not dry_run:
            await move_file(file_path, destination_dir)
        logger.debug(f"Processed {os.path.split(file_path)[-1]} file")
        return data

    async def transform_data(self, data: List[List[dict]], dry_run: bool = False) -> list[tuple[dict | Any, ...]]:
        """
        Parse the provided data and convert values to appropriate types.

        Parameters:
            data (List[List[dict]]): A list containing sublists of dictionaries with string values.
            dry_run (bool): Flag indicating whether it's a dry run or not.

        Returns:
            List[Tuple[datetime, Union[int, float, str]]]: A list of tuples where values are converted to appropriate types.
        """
        formatted_data = []
        for item in data:
            for sublist in item:
                try:
                    # Convert date string to datetime object
                    date_str = sublist[7].replace("-", " ").replace("_", " ").replace("-", " ").replace(":", " ")
                    date_time = datetime.datetime.strptime(date_str, "%Y %m %d %H %M %S")

                    # Convert other elements to int or float
                    converted_elements = [int(item) if item.isdigit() else float(item.replace(",", ".")) for item in sublist[8:-1]]

                    # Create tuple and append to formatted_data
                    formatted_data.append(
                        (sublist[0], sublist[1], sublist[2], sublist[3], sublist[4], sublist[5], sublist[6], date_time)
                        + tuple(converted_elements + [sublist[-1]])
                    )
                except ValueError as ve:
                    logger.error(
                        f"""ValueError occurred when trying to modify csv data
                                                            File: {sublist[-1]}
                                                            RowOfData: {item}
                                                            CodeError: {ve}"""
                    )
                    raise
        return formatted_data


class DATDataReader(AbstractDataReader):
    """Class for reading data from DAT files asynchronously."""

    async def read_data(self, file_path: str, destination_dir: str, dry_run: bool = False) -> List[List[str]]:
        """
        Read data from a DAT file asynchronously and move it to a destination path.

        Parameters:
            file_path (str): The path to the DAT file to read.
            destination_dir (str): The path to move the DAT file to after reading.
            dry_run (bool): Flag indicating whether it's a dry run or not.

        Returns:
            List[List[str]]: A list of lists, where each inner list represents a row of data read from the DAT file.
        """
        data = []
        with open(file_path, "r") as file:
            for line in file:
                # Custom logic to parse DAT file lines and add filename
                data.append(line.strip().split(",") + [f"filename={os.path.split(file_path)[-1]}"])
        # Move the file after reading
        if not dry_run:
            await move_file(file_path, destination_dir)
        logger.debug(f"Processed {os.path.split(file_path)[-1]} file")
        return data

    async def transform_data(self, data: List[List[str]], dry_run: bool = False) -> list[tuple[dict | Any, ...]]:
        """
        Transform the provided data into a list of dictionaries.

        Parameters:
            data (List[List[str]]): A list containing sublists of strings with key-value pairs.
            dry_run (bool): Flag indicating whether it's a dry run or not.

        Returns:
            List[List[dict]]: A list where each sublist contains dictionaries with key-value pairs.
        """
        transformed_data = []
        for sublist in data:
            for item in sublist:
                item_dict = {}
                for pair in item:
                    key, value = pair.split("=")
                    item_dict[key.strip()] = value.strip()
                try:
                    # Convert specific values to int, float, or datetime
                    item_dict["Sequence"] = int(item_dict["Sequence"])  # type: ignore
                    item_dict["WT"] = float(item_dict["WT"])  # type: ignore
                    item_dict["VOLUME"] = float(item_dict["VOLUME"])  # type: ignore
                    item_dict["DATETIME"] = dt.strptime(item_dict["DATETIME"], "%Y%m%d%H%M%S")  # type: ignore
                    transformed_data.append(tuple(item_dict.values()))
                except ValueError as ve:
                    logger.error(
                        f"""ValueError occurred when trying to modify dat data
                                        File: {item_dict["filename"]}
                                        RowOfData: {item}
                                        CodeError: {ve}"""
                    )
                    raise
                except KeyError as ke:
                    logger.error(
                        f"""KeyError occurred when trying to modify dat data
                                        File: {item_dict["filename"]}
                                        RowOfData: {item}
                                        CodeError: {ke}"""
                    )
                    raise
        return transformed_data  # type: ignore


class MySQLDataReader(AbstractDataReader):
    """Class for reading data from MySQL database asynchronously."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        db: str,
        pool_size: int = 5,
    ):
        """
        Initialize MySQLDataReader.

        Args:
            host (str): MySQL host address.
            port (int): MySQL port number.
            user (str): MySQL username.
            password (str): MySQL password.
            db (str): MySQL database name.
            pool_size (int): Connection pool size (default is 5).
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.pool_size = pool_size

    async def read_data(self, table_name: str, dry_run: bool = False) -> List[Tuple]:
        """
        Read data from MySQL database asynchronously.

        Args:
            table_name (str): Name of the table in the database.
            dry_run (bool): Flag indicating whether it's a dry run or not.

        Returns:
            List[Tuple]: List of tuples containing the data read from the database.
        """
        if dry_run:
            logger.info("Performing dry run. No data will be fetched from the database.")
            return []  # Return empty list indicating no data fetched in dry run mode

        async with aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            maxsize=self.pool_size,
        ) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"SELECT * FROM {table_name}")
                    result = await cursor.fetchall()
                    return result

    async def transform_data(self, data: List[Dict[str, Any]]) -> Any:
        """

        :param kwargs:
        :return:
        """
        pass
