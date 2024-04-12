import csv
import datetime
import os
from datetime import datetime as dt
from typing import Any, Dict, List, Tuple, Union

import aiomysql
from etl_process.abstract_data_loader import AbstractDataReader
from etl_process.utils import logger, move_file


class CSVDataReader(AbstractDataReader):
    """Class for reading data from CSV files asynchronously."""

    async def read_data(self, file_path: str, destination_dir: str, dry_run: bool = False) -> list[list[str]]:
        """
        Read data from a CSV file asynchronously and move it to a destination path.

        :param file_path: The path to the CSV file to read.
        :type file_path: str
        :param destination_dir: The path to move the CSV file to after reading.
        :type destination_dir: str
        :param dry_run: Flag indicating whether it's a dry run or not.
        :type dry_run: bool, optional
        :return: The data read from the CSV file.
        :rtype: list[list[str]]
        """
        data = []
        with open(file_path, newline="") as file:
            spamreader = csv.reader(file, delimiter=" ")
            for row in spamreader:
                data.append(row[0].split(";") + [f"{os.path.split(file_path)[-1]}"])
        if not dry_run:
            await move_file(file_path, destination_dir)
        logger.debug(f"Processed {os.path.split(file_path)[-1]} file")
        return data

    async def transform_data(self, data: List[List[dict]], dry_run: bool = False) -> tuple[list[tuple[Any]], list[Any]]:
        """
        Parse the provided data and convert values to appropriate types.

        :param data: A list containing sublists of dictionaries with string values.
        :type data: List[List[dict]]
        :param dry_run: Flag indicating whether it's a dry run or not.
        :type dry_run: bool, optional
        :return: A list of tuples where values are converted to appropriate types.
        :rtype: tuple[list[tuple[Any]], list[Any]]
        """
        formatted_data = []
        corrupted_files = []
        for item in data:
            for sublist in item:
                try:
                    date_str = sublist[7].replace("-", " ").replace("_", " ").replace("-", " ").replace(":", " ")
                    date_time = datetime.datetime.strptime(date_str, "%Y %m %d %H %M %S")
                    converted_elements = [int(item) if item.isdigit() else float(item.replace(",", ".")) for item in sublist[8:-1]]
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
                    corrupted_files.append(sublist[-1])
                except AttributeError as ae:
                    logger.error(
                        f"""AttributeError occurred when trying to modify csv data
                                              File: {sublist[-1]}
                                              RowOfData: {item}
                                              CodeError: {ae}"""
                    )
                    corrupted_files.append(sublist[-1])

        return formatted_data, list(set(corrupted_files))

    async def check_data(self, line: Tuple) -> str:
        """
        Check the format and content of the provided CSV line asynchronously.

        :param line: A tuple containing the CSV data in the specified format.
        :type line: Tuple
        :return: The filename if the data is invalid, otherwise an empty string.
        :rtype: str
        """
        expected_length = 14
        expected_types = [
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            datetime.datetime,
            Union[float, int],
            Union[float, int],
            Union[float, int],
            Union[float, int],
            Union[float, int],
            str,
        ]
        if len(line) != expected_length:
            return line[-1]
        for value, expected_type in zip(line, expected_types):
            if not isinstance(value, expected_type):  # type: ignore
                return line[-1]

        return ""


class DATDataReader(AbstractDataReader):
    """Class for reading data from DAT files asynchronously."""

    async def read_data(self, file_path: str, destination_dir: str, dry_run: bool = False) -> List[List[str]]:
        """
        Read data from a DAT file asynchronously and move it to a destination path.

        :param file_path: The path to the DAT file to read.
        :type file_path: str
        :param destination_dir: The path to move the DAT file to after reading.
        :type destination_dir: str
        :param dry_run: Flag indicating whether it's a dry run or not.
        :type dry_run: bool, optional
        :return: A list of lists, where each inner list represents a row of data read from the DAT file.
        :rtype: List[List[str]]
        """
        data = []
        with open(file_path, "r") as file:
            for line in file:
                data.append(line.strip().split(",") + [f"filename={os.path.split(file_path)[-1]}"])
        if not dry_run:
            await move_file(file_path, destination_dir)
        logger.debug(f"Processed {os.path.split(file_path)[-1]} file")
        return data

    async def transform_data(self, data: List[List[str]], dry_run: bool = False) -> tuple[list[tuple[str | Any, ...]], list[str]]:
        """
        Transform the provided data into a list of dictionaries.

        :param data: A list containing sublists of strings with key-value pairs.
        :type data: List[List[str]]
        :param dry_run: Flag indicating whether it's a dry run or not.
        :type dry_run: bool, optional
        :return: A list where each sublist contains dictionaries with key-value pairs.
        :rtype: tuple[list[tuple[str | Any, ...]], list[str]]
        """
        transformed_data = []
        corrupted_files = []
        for sublist in data:
            for item in sublist:
                item_dict = {}
                for pair in item:
                    key, value = pair.split("=")
                    item_dict[key.strip()] = value.strip()
                try:
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
                    corrupted_files.append(sublist[-1])
                except KeyError as ke:
                    logger.error(
                        f"""KeyError occurred when trying to modify dat data
                                        File: {

item_dict["filename"]}
                                        RowOfData: {item}
                                        CodeError: {ke}"""
                    )
                    corrupted_files.append(sublist[-1])
        return transformed_data, list(set(corrupted_files))

    async def check_data(self, data: Tuple) -> str:
        """
        Check the format and content of the provided data tuple.

        :param data: A tuple containing the data in the specified format.
        :type data: Tuple
        :return: The filename if the data is invalid, otherwise an empty string.
        :rtype: str
        """
        expected_length = 14
        expected_types = [
            int,
            str,
            str,
            datetime.datetime,
            str,
            str,
            float,
            str,
            str,
            str,
            float,
            str,
            str,
            str,
        ]
        if len(data) != expected_length:
            return data[-1]
        for value, expected_type in zip(data, expected_types):
            if not isinstance(value, expected_type):
                return data[-1]
        return ""


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

        :param host: MySQL host address.
        :type host: str
        :param port: MySQL port number.
        :type port: int
        :param user: MySQL username.
        :type user: str
        :param password: MySQL password.
        :type password: str
        :param db: MySQL database name.
        :type db: str
        :param pool_size: Connection pool size (default is 5).
        :type pool_size: int, optional
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

        :param table_name: Name of the table in the database.
        :type table_name: str
        :param dry_run: Flag indicating whether it's a dry run or not.
        :type dry_run: bool, optional
        :return: List of tuples containing the data read from the database.
        :rtype: List[Tuple]
        """
        if dry_run:
            logger.info("Performing dry run. No data will be fetched from the database.")
            return []
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

    async def transform_data(self) -> Any:
        """ """
        pass

    async def check_data(self) -> Any:
        """ """
        pass
