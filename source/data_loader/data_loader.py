from typing import List, Tuple

import aiomysql
from data_loader.abstract_data_loader import AbstractDataReader


class MySQLDataLoader(AbstractDataReader):
    """Class for loading data into MySQL database asynchronously."""

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
        Initialize MySQLDataLoader.

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

    async def load_data_to_db(self, data: List[Tuple[str, str, str]], table_name: str) -> None:
        """
        Load data into MySQL database asynchronously.

        Args:
            data (List[Tuple[str, str, str]]): List of tuples containing data to be loaded into the database.
            table_name (str): Name of the table in the database.
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
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        f"CREATE TABLE IF NOT EXISTS {table_name} ("
                        "id INT AUTO_INCREMENT PRIMARY KEY, "
                        "column1 VARCHAR(255), "
                        "column2 VARCHAR(255), "
                        "column3 VARCHAR(255))"
                    )
                    await cursor.executemany(
                        f"INSERT INTO {table_name} (column1, column2, column3) " "VALUES (%s, %s, %s)",
                        data,
                    )
                    await conn.commit()
