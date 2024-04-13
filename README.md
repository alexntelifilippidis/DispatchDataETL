# DispatchDataETL

1. [Code Documantation](#code-documentation)
1. [Setup](#setup)
2. [Configuration](#configuration)
3. [Testing](#testing)
    1. [Unit Testing](#unit-testing)
    2. [Integration Testing](#integration-testing)
    3. [Dry run](#dry-run)
4. [CI/CD](#cicd)
    1. [CI](#ci)
5. [Code Structure](#code-structure)


## Code Documantation
Documentation for the code exists here: [GitHubPage](https://alexntelifilippidis.github.io/DispatchDataETL/)

## Setup

Follow these steps to set up the project environment:

### Prerequisites

Make sure you have Python 3.12 and [pre-commit](https://pre-commit.com/#intro) installed on your system. You can
download it from [Python's official website](https://www.python.org/downloads/).
And

## Configuration

The application uses a configuration file to specify various settings and parameters. The configuration file should be
named `config.py` and should be placed in the root directory of the project.

### Configuration Options

- `dry_run`: Flag indicating whether it's a dry run or not.
- `dat_dir`: Path to the directory containing DAT files.
- `dat_destination_dir`: Path to the directory where processed DAT files should be stored.
- `csv_dir`: Path to the directory containing CSV files.
- `csv_destination_dir`: Path to the directory where processed CSV files should be stored.
- `chunk_size`: Size of data chunks for processing.
- `host`: Database host.
- `port`: Database port.
- `user`: Database user.
- `password`: Database password.
- `db`: Database name.
- `pool_size`: Size of the database connection pool.
- `table_name_source`: Name of the source table in the database.
- `creation_column_csv`: The columns for the source csv table
- `creation_column_dat`: The columns for the source dat table

### Installation

1. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/alexntelifilippidis/DispatchDataETL.git
    ```

2. Navigate to the project directory:

    ```bash
    cd project_name
    ```


3. Install project dependencies using Makefile:

    ```bash
    make install-dev
    ```

   or

    ```bash
    make install-prod
    ```

   Depending on whether you want to install development or production dependencies.


4. Activate Project
   ```bash
    make activate-env
    ```

## Testing

### Unit Testing

Tested with python 3.12.2

```bash
  make activate-env
```

- Run all tests with:

```bash
  make unit
 ```

### Integration Testing

Part of the automated tests. If you need to test manually, it is as easy as executing

```bash
  make integration-tests
```

or specifically

```bash
  make integration-environment
  pytest -v -s tests/integration --no-header -vv || (make teardown && exit 1)
  make integration-teardown
```

The integration tests need a running environment consisting of:

- A running operational mysql docker container

To run both unit and integration together run:

```bash
  make test
```

### Dry run

A "dry run" is a practice of testing a process or operation without actually executing it, providing a way to verify
functionality and identify potential issues before making any real changes or updates. It's commonly used in software
development, system administration, and other fields to ensure safety and reliability.

Change in the conf file the parameter dry_run=True and run main

## CI/CD

The CI/CD pipeline is configured in the [GitHub actions](.github/workflows) files.

### CI

The CI pipeline is configured in the [GitHub actions](.github/workflows/ci.yml) file.

- It is triggered on every push to the main branch and in every pull request(pr).
- It deploys sphinx doc to GitHub pages ([how to do it](https://redandgreen.co.uk/sphinx-to-github-pages-via-github-actions/python-code/))
- It runs unit testing and the integration testing.
- It also runs the pre-commit hooks to ensure that the code is formatted correctly and that the tests pass before
  pushing to the main branch.
- Fails if any of the tests fail.

## Code Structure

```
.
├── .github                     # Directory for GitHub actions
│   └── workflows               # Directory for GitHub actions workflows
├── source                      # Directory where all your source code exists
│   ├── docs                    # Directory that created from sphinx and manages the html doc that exists in github
│   ├── etl_process             # Directory for all etl classes
│   ├── config.py               # Config file for the source code
│   └── main.py                 # The running file           
├── tests                       # Directory for all tests
│   ├── sql_scripts             # Directory init sql script for test db
│   ├── test_data               # Directory with test data for the tests
│   ├── unit                    # Directory for unit tests
│   └── integration             # Directory for integration tests
├── .gitignore                  # File for ignoring files in git
├── .pre-commit-config.yaml     # File for running pre-commit hooks before puss to git
├── docker-compose.yaml         # File for set up Testing environment 
├── Makefile                    # File for any Python packages 
├── pyproject.toml              # File of python project configurations
├── Pipfile                     # File for any Python packages prod and dev
└── setup.cfg                   # File for set up

```