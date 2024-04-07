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
    print("Environment is set to test")
    # Add your test-specific configurations or actions here
    dat_dir = "tests/test_data/test_data_dat"
    dat_destination_dir = "tests/test_data/test_data_dat_destination"
    csv_dir = "tests/test_data/test_data_csv"
    csv_destination_dir = "tests/test_data/test_data_csv_destination"
    chunk_size = 50
else:
    # If the environment is not set to "test"
    print("Environment is set to production")
    # Add your non-test configurations or actions here
    dat_dir = "../tests/test_data/test_data_dat"
    dat_destination_dir = "../tests/test_data/test_data_dat_destination"
    csv_dir = "../tests/test_data/test_data_csv"
    csv_destination_dir = "../tests/test_data/test_data_csv_destination"
    chunk_size = 100
