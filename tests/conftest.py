import os

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


@pytest.fixture
def expected_csv_data():
    # Define the expected data
    return [
        {
            "01;01;19112930098;67254036AY;700025218874;1685623517595_700025218874_01-06-2023-15-45-17"
            ".jpeg;1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg;2023-06-01-15-45-17;42;13": "01;01;19112930098;67254036AY;700025218874;1685623531229_700025218874_01-06-2023-15-45"
            "-31.jpeg;1685623531229_700025218874_01-06-2023-15-45-31_C.jpeg;2023-06-01-15-45-31;41",
            "5;24;15": "5;13",
            "28;13": "5;24;15",
            "6": "28;13",
            None: ["4"],
        },
        {
            "01;01;19112930098;67254036AY;700025218874;1685623517595_700025218874_01-06-2023-15-45-17"
            ".jpeg;1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg;2023-06-01-15-45-17;42;13": "01;01;19112930098;67254036AY;700025220631;1685623554748_700025220631_01-06-2023-15-45"
            "-54.jpeg;1685623554748_700025220631_01-06-2023-15-45-54_C.jpeg;2023-06-01-15-45-54;42",
            "5;24;15": "5;11",
            "28;13": "5;39;1",
            "6": "38;19",
            None: ["1"],
        },
        {
            "01;01;19112930098;67254036AY;700025218874;1685623517595_700025218874_01-06-2023-15-45-17"
            ".jpeg;1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg;2023-06-01-15-45-17;42;13": "01;01;19112930098;67254036AY;700025197311;1685623563112_700025197311_01-06-2023-15-46"
            "-03.jpeg;1685623563112_700025197311_01-06-2023-15-46-03_C.jpeg;2023-06-01-15-46-03;23",
            "5;24;15": "5;8",
            "28;13": "5;18",
            "6": "5;1",
            None: ["06;3", "7"],
        },
        {
            "01;01;19112930098;67254036AY;700025218874;1685623517595_700025218874_01-06-2023-15-45-17"
            ".jpeg;1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg;2023-06-01-15-45-17;42;13": "01;01;19112930098;67254036AY;700025206049;1685623572567_700025206049_01-06-2023-15-46"
            "-12.jpeg;1685623572567_700025206049_01-06-2023-15-46-12_C.jpeg;2023-06-01-15-46-12;32"
            ";18",
            "5;24;15": "5;25;1",
            "28;13": "38;14",
            "6": "8",
        },
        {
            "01;01;19112930098;67254036AY;700025218874;1685623517595_700025218874_01-06-2023-15-45-17"
            ".jpeg;1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg;2023-06-01-15-45-17;42;13": "01;01;19112930098;67254036AY;700025215681;1685623592477_700025215681_01-06-2023-15-46"
            "-32.jpeg;1685623592477_700025215681_01-06-2023-15-46-32_C.jpeg;2023-06-01-15-46-32;34",
            "5;24;15": "5;16;24;0",
            "28;13": "76;13",
            "6": "2",
        },
        {
            "01;01;19112930098;67254036AY;700025218874;1685623517595_700025218874_01-06-2023-15-45-17"
            ".jpeg;1685623517595_700025218874_01-06-2023-15-45-17_C.jpeg;2023-06-01-15-45-17;42;13": "01;01;19112930098;67254036AY;700025214669;1685623598481_700025214669_01-06-2023-15-46"
            "-38.jpeg;1685623598481_700025214669_01-06-2023-15-46-38_C.jpeg;2023-06-01-15-46-38;42",
            "5;24;15": "5;13;38;0",
            "28;13": "92;21",
            "6": "0",
        },
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
        ],
    ]
