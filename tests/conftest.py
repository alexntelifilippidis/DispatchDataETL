import ast
import os

import aiomysql
import pytest

pytest_plugins = ("pytest_asyncio",)


# Define a pytest fixture to set up the environment variable
@pytest.fixture(autouse=True)
def set_test_env():
    os.environ["ENV"] = "test"  # Set the environment variable to 'test' for the duration of tests
    yield  # Execute the tests
    del os.environ["ENV"]  # Remove the environment variable after tests finish


@pytest.fixture(autouse=True)
def navigate_to_project_root():
    """
    Fixture to navigate to the project root directory if tests are being run from a subdirectory.
    """
    current_dir = os.getcwd()
    if os.path.basename(current_dir) == "tests":
        os.chdir("..")  # Move up one directory to the project root
    yield
    os.chdir(current_dir)  # Return to the original directory after the test


async def query_and_delete_data(host, port, user, password, db, pool_size, table_name):
    data = None
    async with aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db,
        maxsize=pool_size,
    ) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Query the database to retrieve the data
                await cursor.execute(f"SELECT * FROM {table_name}")
                data = await cursor.fetchall()

                # Delete the data from the database
                await cursor.execute(f"DELETE FROM {table_name}")
                await conn.commit()

    return data


@pytest.fixture
def expected_csv_data():
    # Define the expected data
    return [
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
            "01062023_155202_01_01_19112930098.csv",
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
            "01062023_155202_01_01_19112930098.csv",
        ],
        [
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025220631",
            "1685623554748_700025220631_01-06-2023-15-45-54.jpeg",
            "1685623554748_700025220631_01-06-2023-15-45-54_C.jpeg",
            "2023-06-01-15-45-54",
            "42,5",
            "11,5",
            "39",
            "1,38",
            "19,1",
            "01062023_155202_01_01_19112930098.csv",
        ],
        [
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025197311",
            "1685623563112_700025197311_01-06-2023-15-46-03.jpeg",
            "1685623563112_700025197311_01-06-2023-15-46-03_C.jpeg",
            "2023-06-01-15-46-03",
            "23,5",
            "8,5",
            "18,5",
            "1,06",
            "3,7",
            "01062023_155202_01_01_19112930098.csv",
        ],
        [
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025206049",
            "1685623572567_700025206049_01-06-2023-15-46-12.jpeg",
            "1685623572567_700025206049_01-06-2023-15-46-12_C.jpeg",
            "2023-06-01-15-46-12",
            "32",
            "18,5",
            "25",
            "1,38",
            "14,8",
            "01062023_155202_01_01_19112930098.csv",
        ],
        [
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025215681",
            "1685623592477_700025215681_01-06-2023-15-46-32.jpeg",
            "1685623592477_700025215681_01-06-2023-15-46-32_C.jpeg",
            "2023-06-01-15-46-32",
            "34,5",
            "16",
            "24",
            "0,76",
            "13,2",
            "01062023_155202_01_01_19112930098.csv",
        ],
        [
            "01",
            "01",
            "19112930098",
            "67254036AY",
            "700025214669",
            "1685623598481_700025214669_01-06-2023-15-46-38.jpeg",
            "1685623598481_700025214669_01-06-2023-15-46-38_C.jpeg",
            "2023-06-01-15-46-38",
            "42,5",
            "13",
            "38",
            "0,92",
            "21,0",
            "01062023_155202_01_01_19112930098.csv",
        ],
    ]


@pytest.fixture
def expected_dat_data():
    # Define the expected data
    return [
        [
            "Sequence=8747",
            "PLACE=PATRA",
            "STATION=    1",
            "DATETIME=20231020122340",
            "WU=kg",
            "WS=0000",
            "WT=  18.660",
            "DU=cm",
            "DS=0000",
            "D= 85.0X 53.0X 20.0",
            "VOLUME= 90.10",
            "B=700028655189",
            "CHK=45",
            "filename=20231020152446.dat",
        ],
        [
            "Sequence=8748",
            "PLACE=PATRA",
            "STATION=    1",
            "DATETIME=20231020122406",
            "WU=kg",
            "WS=0000",
            "WT=   5.220",
            "DU=cm",
            "DS=0000",
            "D= 33.5X 23.5X 28.5",
            "VOLUME= 22.44",
            "B=700028665730",
            "CHK=35",
            "filename=20231020152446.dat",
        ],
    ]
