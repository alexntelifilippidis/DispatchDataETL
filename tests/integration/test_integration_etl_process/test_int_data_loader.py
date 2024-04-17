import asyncio
import datetime
from decimal import Decimal

import aiomysql
import pytest
from conftest import query_and_delete_data
from etl_process.data_loader import MySQLDataLoader


@pytest.mark.asyncio
async def test_data_loader_mysql_csv():
    import config as conf

    data = [
        [
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025218874",
            "1685623517595_700025218874_01-06-2023-15-45-17.jpeg",
            "1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg",
            "2023-06-01-15-45-17",
            "42",
            "13,5",
            "24",
            "15,28",
            "13,6",
            "test.csv",
        ],
        [
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025218874",
            "1685623531229_700025218874_01-06-2023-15-45-31.jpeg",
            "1685623531229_700025218874_01-06-2023-15-45-31_C.jpeg",
            "2023-06-01-15-45-31",
            "41,5",
            "13,5",
            "24",
            "15,28",
            "13,4",
            "test.csv",
        ],
    ]
    loop = asyncio.get_event_loop()
    mysql_data_reader = MySQLDataLoader(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
    )
    await mysql_data_reader.load_data_to_db(
        data=data, table_name="source_csv", creation_columns=conf.creation_column_csv, chunk_size=conf.chunk_size, loop=loop
    )
    actual_data = await query_and_delete_data(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
        table_name="source_csv",
    )

    actual_data_without_id = [t[:-1] for t in actual_data]
    assert actual_data_without_id == [
        (
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025218874",
            "1685623517595_700025218874_01-06-2023-15-45-17.jpeg",
            "1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg",
            datetime.datetime(2023, 6, 1, 15, 45, 17),
            Decimal("42.00"),
            Decimal("13.00"),
            Decimal("24.00"),
            Decimal("15.00"),
            Decimal("13.00"),
            "test.csv",
        ),
        (
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025218874",
            "1685623531229_700025218874_01-06-2023-15-45-31.jpeg",
            "1685623531229_700025218874_01-06-2023-15-45-31_C.jpeg",
            datetime.datetime(2023, 6, 1, 15, 45, 31),
            Decimal("41.00"),
            Decimal("13.00"),
            Decimal("24.00"),
            Decimal("15.00"),
            Decimal("13.00"),
            "test.csv",
        ),
    ]


@pytest.mark.asyncio
async def test_data_loader_mysql_dat():
    import config as conf

    data = [
        (
            8747,
            "PATRA",
            "1",
            datetime.datetime(2023, 10, 20, 12, 23, 40),
            "kg",
            "0000",
            18.66,
            "cm",
            "0000",
            "85.0X 53.0X 20.0",
            90.1,
            "700028655189",
            "45",
            "test.dat",
        ),
        (
            8748,
            "PATRA",
            "1",
            datetime.datetime(2023, 10, 20, 12, 24, 6),
            "kg",
            "0000",
            5.22,
            "cm",
            "0000",
            "33.5X 23.5X 28.5",
            22.44,
            "700028665730",
            "35",
            "test.dat",
        ),
    ]
    loop = asyncio.get_event_loop()
    mysql_data_reader = MySQLDataLoader(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
    )
    await mysql_data_reader.load_data_to_db(
        data=data, table_name="source_dat", creation_columns=conf.creation_column_dat, chunk_size=conf.chunk_size, loop=loop
    )
    actual_data = await query_and_delete_data(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
        table_name="source_dat",
    )

    actual_data_without_id = [t[:-1] for t in actual_data]
    assert actual_data_without_id == [
        (
            8747,
            "PATRA",
            "1",
            datetime.datetime(2023, 10, 20, 12, 23, 40),
            "kg",
            "0000",
            Decimal("18.66"),
            "cm",
            "0000",
            "85.0X 53.0X 20.0",
            Decimal("90.10"),
            "700028655189",
            "45",
            "test.dat",
        ),
        (
            8748,
            "PATRA",
            "1",
            datetime.datetime(2023, 10, 20, 12, 24, 6),
            "kg",
            "0000",
            Decimal("5.22"),
            "cm",
            "0000",
            "33.5X 23.5X 28.5",
            Decimal("22.44"),
            "700028665730",
            "35",
            "test.dat",
        ),
    ]


@pytest.mark.asyncio
async def test_data_loader_mysql_silver():
    import config as conf

    data = [
        ("ABC123", datetime.datetime(2024, 4, 8, 12, 0), Decimal("2.50"), Decimal("10.50"), Decimal("8.00"), Decimal("5.50"), 3),
        ("DEF456", datetime.datetime(2024, 4, 9, 13, 0), Decimal("3.20"), Decimal("12.00"), Decimal("9.00"), Decimal("6.00"), 3),
    ]
    loop = asyncio.get_event_loop()
    mysql_data_loader = MySQLDataLoader(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
    )

    await mysql_data_loader.load_data_to_db(
        data=data,
        table_name=conf.silver_table,
        creation_columns=conf.creation_column_silver,
        chunk_size=conf.chunk_size,
        loop=loop,
        deduplication_columns=["voucher"],
        deduplication_method=True,
    )
    actual_data = await query_and_delete_data(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
        table_name=conf.silver_table,
    )

    assert actual_data == (
        ("ABC123", datetime.datetime(2024, 4, 8, 12, 0), Decimal("2.50"), Decimal("10.50"), Decimal("8.00"), Decimal("5.50"), 3),
        ("DEF456", datetime.datetime(2024, 4, 9, 13, 0), Decimal("3.20"), Decimal("12.00"), Decimal("9.00"), Decimal("6.00"), 3),
    )


@pytest.mark.asyncio
async def test_data_loader_mysql_silver_deduplicate():
    import config as conf

    data = [
        ("ABC123", datetime.datetime(2024, 4, 8, 12, 0), Decimal("2.50"), Decimal("10.50"), Decimal("8.00"), Decimal("5.50"), 3),
        ("ABC123", datetime.datetime(2025, 4, 9, 13, 0), Decimal("3.20"), Decimal("12.00"), Decimal("9.00"), Decimal("6.00"), 3),
        ("DEF456", datetime.datetime(2024, 4, 9, 13, 0), Decimal("3.20"), Decimal("12.00"), Decimal("9.00"), Decimal("6.00"), 3),
    ]
    loop = asyncio.get_event_loop()
    mysql_data_loader = MySQLDataLoader(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
    )

    await mysql_data_loader.load_data_to_db(
        data=data,
        table_name=conf.silver_table,
        creation_columns=conf.creation_column_silver,
        chunk_size=conf.chunk_size,
        loop=loop,
        deduplication_columns=["voucher"],
        deduplication_method=True,
    )
    actual_data = await query_and_delete_data(
        host=conf.host,
        port=conf.port,  # Your MySQL port
        user=conf.user,
        password=conf.password,
        db=conf.db,
        pool_size=conf.pool_size,
        table_name=conf.silver_table,
    )

    assert actual_data == (
        ("ABC123", datetime.datetime(2025, 4, 9, 13, 0), Decimal("3.20"), Decimal("12.00"), Decimal("9.00"), Decimal("6.00"), 3),
        ("DEF456", datetime.datetime(2024, 4, 9, 13, 0), Decimal("3.20"), Decimal("12.00"), Decimal("9.00"), Decimal("6.00"), 3),
    )
