import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from data_loader.data_loader import MySQLDataLoader


@pytest.mark.asyncio
def test_insert_chunk():
    # Define sample data
    chunk = [("value1", 10, 3.5), ("value2", 20, 4.5)]
    table_name = "your_table"
    creation_columns = """
             column1 VARCHAR(255),
             column2 INT, 
             column3 FLOAT
             """

    # Mock cursor object
    cursor_mock = AsyncMock()

    # Instantiate MySQLDataLoader
    loader = MySQLDataLoader(host="your_host", port=3333, user="your_user", password="your_password", db="your_db")

    # Call the _insert_chunk method
    asyncio.run(loader._insert_chunk(cursor_mock, table_name, chunk, creation_columns))

    # Assert that executemany method is called with the correct query and values
    expected_query = "INSERT IGNORE INTO your_table (column1, column2, column3) VALUES (%s, %s, %s)"
    expected_values = [("value1", 10, 3.5), ("value2", 20, 4.5)]
    cursor_mock.executemany.assert_called_once_with(expected_query, expected_values)
