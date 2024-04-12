from abc import ABC
from typing import Any

import aiomysql
from etl_process.abstract_data_loader import AbstractDataLoader
from etl_process.utils import logger


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

        :param data: List of tuples containing data to be inserted into the database.
        :type data: List[Tuple[Union[dict, Any], ...]]
        :param table_name: Name of the table in the database.
        :type table_name: str
        :param creation_columns: all the columns that I need to add for create the table
        :type creation_columns: str
        :param chunk_size: How many lines will ingest to table
        :type chunk_size: int
        :param loop: asyncio event loop
        :type loop: Any
        :param dry_run: Flag indicating whether it's a dry run or not.
        :type dry_run: bool, optional
        :raises TypeError: If there is a type error occurred during data insertion.
        """
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
                    lines = creation_columns.strip().split("\n")
                    columns = [line.split()[0] for line in lines]
                    updated_columns = [item for item in columns if item != "id"]
                    value_placeholders = ", ".join(["%s"] * len(updated_columns))
                    values = [tuple(row) for row in chunk]

                    if dry_run:
                        logger.info("Performing dry run. Data will be inserted into the database but we do rollback.")
                        query = (
                            f"START TRANSACTION; INSERT IGNORE INTO {table_name} ({', '.join(updated_columns)}) "
                            f"VALUES ({value_placeholders}); ROLLBACK;"
                        )
                        logger.debug(f"SQL Query: {query}")
                        logger.debug(f"Values: {values}")
                    else:
                        query = f"INSERT IGNORE INTO {table_name} ({', '.join(updated_columns)}) VALUES ({value_placeholders})"
                    try:
                        await cur.executemany(query, values)
                        await conn.commit()
                    except TypeError as te:
                        logger.error(
                            f"""TypeError occurred when trying to insert data to DB
                                            File: {set([item[-1] for item in values])}
                                            Table: {table_name}
                                            Query: {query}
                                            RowOfData: {values}
                                            CodeError: {te}"""
                        )
        conn.close()
        logger.info(f"Inserted Data to table: {table_name}")
