import csv
from typing import Any, Dict, List, Tuple

import aiomysql
from data_loader.abstract_data_loader import AbstractDataReader
from data_loader.utils import move_file


class CSVDataReader(AbstractDataReader):
    """Class for reading data from CSV files asynchronously."""

    async def read_data(self, file_path: str, destination_dir: str) -> List[Dict[str, Any]]:
        """
        Read data from a CSV file asynchronously and move it to a destination path.

        Parameters:
            file_path (str): The path to the CSV file to read.
            destination_dir (str): The path to move the CSV file to after reading.

        Returns:
            List[Dict[str, Any]]: The data read from the CSV file.
        """
        data = []
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        # Move the file after reading
        await move_file(file_path, destination_dir)
        return data


class DATDataReader(AbstractDataReader):
    """Class for reading data from DAT files asynchronously."""

    async def read_data(self, file_path: str, destination_dir: str) -> List[List[str]]:
        """
        Read data from a DAT file asynchronously and move it to a destination path.

        Parameters:
            file_path (str): The path to the DAT file to read.
            destination_dir (str): The path to move the DAT file to after reading.

        Returns:
            List[List[str]]: A list of lists, where each inner list represents a row of data read from the DAT file.
        """
        data = []
        with open(file_path, "r") as file:
            for line in file:
                # Custom logic to parse DAT file lines
                data.append(line.strip().split(","))
        # Move the file after reading
        await move_file(file_path, destination_dir)

        return data


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

    async def read_data(self, table_name: str) -> List[Tuple]:
        """
        Read data from MySQL database asynchronously.

        Args:
            table_name (str): Name of the table in the database.

        Returns:
            List[Tuple]: List of tuples containing the data read from the database.
        """
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
