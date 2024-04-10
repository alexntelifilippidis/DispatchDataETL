import asyncio
from abc import ABC
from typing import Any, List, Tuple

import aiomysql
from data_loader.abstract_data_loader import AbstractDataLoader
from data_loader.utils import logger


class MySQLDataLoader(AbstractDataLoader, ABC):
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

    async def load_data_to_db(
        self,
        data: list[tuple[dict | Any, ...]],
        table_name: str,
        creation_columns: str,
        chunk_size: int,
        loop: Any,
        dry_run: bool = False,
    ) -> None:
        """
        Insert data into MySQL database asynchronously.

        Args:
            data (List[Tuple[str, str, str]]): List of tuples containing data to be inserted into the database.
            table_name (str): Name of the table in the database.
            chunk_size (int): How many lines will ingest to table
            creation_columns (str): all the columns that I need to add for create the table:
            loop (Any):
            dry_run (bool): Flag indicating whether it's a dry run or not.
        """
        if dry_run:
            logger.info("Performing dry run. No data will be inserted into the database.")
            # Generate and log the SQL queries without executing them
            chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
            for chunk in chunks:
                # Split the string into lines
                lines = creation_columns.strip().split("\n")

                # Extract column names from each line
                columns = [line.split()[0] for line in lines]
                # remove autoincrement id column
                updated_columns = [item for item in columns if item != "id"]
                # Generate placeholders for values in the query
                value_placeholders = ", ".join(["%s"] * len(updated_columns))

                # Generate the INSERT query dynamically
                query = f"INSERT IGNORE INTO {table_name} ({', '.join(updated_columns)}) VALUES ({value_placeholders})"

                # Extract values from the chunk
                values = [tuple(row) for row in chunk]  # Convert each row to tuple

                # Log the SQL query without executing it
                logger.debug(f"SQL Query: {query}")
                logger.debug(f"Values: {values}")
        else:
            # Proceed with the normal data insertion operation
            chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
            pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db,
                maxsize=self.pool_size,
                loop=loop,
                autocommit=True,
            )
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({creation_columns})")
                    await conn.commit()
                    for chunk in chunks:
                        # Split the string into lines
                        lines = creation_columns.strip().split("\n")

                        # Extract column names from each line
                        columns = [line.split()[0] for line in lines]
                        # remove autoincrement id column
                        updated_columns = [item for item in columns if item != "id"]
                        # Generate placeholders for values in the query
                        value_placeholders = ", ".join(["%s"] * len(updated_columns))

                        # Generate the INSERT query dynamically
                        query = f"INSERT IGNORE INTO {table_name} ({', '.join(updated_columns)}) VALUES ({value_placeholders})"

                        # Extract values from the chunk
                        values = [tuple(row) for row in chunk]  # Convert each row to tuple

                        # Execute the query with the chunk of data
                        await cur.executemany(query, values)
                        await conn.commit()

            conn.close()
            logger.info(f"Inserted Data to table: {table_name}")
