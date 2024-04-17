"""
Module-level docstring for configuration and environment setup.

This module handles the configuration settings and environment setup for the ETL process.
It checks the environment variable 'ENV' to determine the environment type and sets
configurations accordingly. If 'ENV' is set to 'test', it configures the settings for
the test environment, otherwise, it configures settings for the production environment.

Test Environment Configuration:
- Directories for data files are set to test directories.
- Database connection settings are configured for the test environment.

Production Environment Configuration:
- Directories for data files are set to production directories.
- Database connection settings are configured for the production environment.

Attributes:
    dry_run (bool): Flag indicating whether it's a dry run or not.
    dat_dir (str): Directory for DAT files.
    dat_destination_dir (str): Destination directory for DAT files.
    corrupted_dat_destination_dir (str): Directory for corrupted DAT files.
    csv_dir (str): Directory for CSV files.
    csv_destination_dir (str): Destination directory for CSV files.
    corrupted_csv_destination_dir (str): Directory for corrupted CSV files.
    chunk_size (int): Size of data chunks for processing.
    host (str): Database host.
    port (int): Database port.
    user (str): Database user.
    password (str): Database password.
    db (str): Database name.
    pool_size (int): Size of the database connection pool.
    table_name_source (str): Name of the source table in the database.
    creation_column_csv (str): SQL column definitions for CSV files.
    creation_column_dat (str): SQL column definitions for DAT files.
"""

# Assuming there's an environment variable named ENV that stores the environment information

import os

from etl_process.utils import logger

# Get the environment variable ENV
try:
    env = os.environ.get("ENV")
except KeyError:
    # Handle the case where 'ENV' is not set
    env = "production"

if env == "test":
    # If the environment is set to "test"
    logger.info("Environment is set to Test")
    # Add your test-specific configurations or actions here
    dry_run = False
    dat_dir = "tests/test_data/test_data_dat"
    dat_destination_dir = "tests/test_data/test_data_dat_destination"
    corrupted_dat_destination_dir = "tests/test_data/test_data_dat_corrupted_destination"
    csv_dir = "tests/test_data/test_data_csv"
    csv_destination_dir = "tests/test_data/test_data_csv_destination"
    corrupted_csv_destination_dir = "tests/test_data/test_data_csv_corrupted_destination"
    chunk_size = 50
    host = "localhost"
    port = 3306
    user = "root"
    password = "test_password"
    db = "dbo"
    pool_size = 10
    table_name_source = "Packages"
    table_name_source_dat = "source_dat"
    table_name_source_csv = "source_csv"
    silver_table = "silver_dat_csv_sql"
else:
    # If the environment is not set to "test" or "dry-run"
    logger.info("Environment is set to Production")
    # Add your non-test configurations or actions here
    dry_run = False
    dat_dir = "../tests/test_data/test_data_dat"
    dat_destination_dir = "../tests/test_data/test_data_dat_destination"
    corrupted_dat_destination_dir = "../tests/test_data/test_data_dat_corrupted_destination"
    csv_dir = "../tests/test_data/test_data_csv"
    csv_destination_dir = "../tests/test_data/test_data_csv_destination"
    corrupted_csv_destination_dir = "../tests/test_data/test_data_csv_corrupted_destination"
    chunk_size = 100
    host = "localhost"
    port = 3306
    user = "root"
    password = "test_password"
    db = "dbo"
    pool_size = 10
    table_name_source = "Packages"
    table_name_source_dat = "source_dat"
    table_name_source_csv = "source_csv"
    silver_table = "silver_dat_csv_sql"

# the lines need to be above each other to work source/etl_process/etl_process.py:80
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
    source_filename VARCHAR(255),
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
    source_filename VARCHAR(255),
    id INT AUTO_INCREMENT PRIMARY KEY
"""

creation_column_silver = """
    voucher VARCHAR(255) PRIMARY KEY,
    measure_datetime DATETIME,
    Length DECIMAL(10,2),
    Height DECIMAL(10,2),
    Width DECIMAL(10,2),
    Weight DECIMAL(10,2),
    source INT
"""
