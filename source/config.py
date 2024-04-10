# Assuming there's an environment variable named ENV that stores the environment information

import os

# Get the environment variable ENV
try:
    env = os.environ.get("ENV")
except KeyError:
    # Handle the case where 'ENV' is not set
    env = "production"

if env == "test":
    # If the environment is set to "test"
    print("Environment is set to Test")
    # Add your test-specific configurations or actions here
    dat_dir = "tests/test_data/test_data_dat"
    dat_destination_dir = "tests/test_data/test_data_dat_destination"
    csv_dir = "tests/test_data/test_data_csv"
    csv_destination_dir = "tests/test_data/test_data_csv_destination"
    chunk_size = 50
    host = "localhost"
    port = 3306
    user = "root"
    password = "test_password"
    db = "dbo"
    pool_size = 10
    table_name_source = "Packages"
else:
    # If the environment is not set to "test"
    print("Environment is set to Production")
    # Add your non-test configurations or actions here
    dat_dir = "../tests/test_data/test_data_dat"
    dat_destination_dir = "../tests/test_data/test_data_dat_destination"
    csv_dir = "../tests/test_data/test_data_csv"
    csv_destination_dir = "../tests/test_data/test_data_csv_destination"
    chunk_size = 100
    host = "localhost"
    port = 3306
    user = "root"
    password = "test_password"
    db = "dbo"
    pool_size = 10
    table_name_source = "Packages"

# the lines need to be above each other to work source/data_loader/data_loader.py:80
creation_column_csv = """  
    data_01 VARCHAR(255),
    data_02 VARCHAR(255),
    data_03 VARCHAR(255),
    data_04 VARCHAR(255),
    voucher VARCHAR(255),
    filename_01 VARCHAR(255),
    filename_02 VARCHAR(255),
    measure_datetime DATETIME,
    Length DECIMAL(10,2),
    Height DECIMAL(10,2),
    Width DECIMAL(10,2),
    Weight DECIMAL(10,2),
    data_13 DECIMAL(10,2),
    id INT AUTO_INCREMENT PRIMARY KEY
"""
creation_column_dat = """
    sequence INT,
    place VARCHAR(255),
    station VARCHAR(255),
    measure_datetime DATETIME,
    wu VARCHAR(255),
    ws VARCHAR(255),
    wt DECIMAL(10, 2),
    du VARCHAR(255),
    ds VARCHAR(255),
    d VARCHAR(255),
    volume DECIMAL(10, 2),
    b VARCHAR(255),
    chk VARCHAR(255),
    id INT AUTO_INCREMENT PRIMARY KEY
"""
