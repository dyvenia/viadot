# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added `validate` function to `viadot/utils.py`
- Fixed `Databricks.create_table_from_pandas()` failing to overwrite a table in some cases even with `replace="True"`
- Enabled Databricks Connect in the image. To enable, [follow this guide](./README.md#executing-spark-jobs)
- Added `Databricks` source
- Added `ExchangeRates` source
- Added `from_df()` method to `AzureDataLake` source
- Added `SAPRFC` source
- Added `S3` source
- Added `RedshiftSpectrum` source
- Added `upload()` and `download()` methods to `S3` source
- Added `Genesys` source
- Fixed a bug in `Databricks.create_table_from_pandas()`. The function that converts column names to snake_case was not used in every case. (#672)
- Added `howto_migrate_sources_tasks_and_flows.md` document explaining viadot 1 -> 2 migration process
- `RedshiftSpectrum.from_df()` now automatically creates a folder for the table if not specified in `to_path`
- Fixed a bug in `Databricks.create_table_from_pandas()`. The function now automatically casts DataFrame types. (#681)
- Added `close_connection()` to `SAPRFC`
- Added `Trino` source
- Added `MinIO` source
- Added `gen_split()` method to `SAPRFCV2` class to allow looping over a data frame with generator - improves performance

### Changed

- Added `SQLServerToDF` task
- Added `SQLServerToDuckDB` flow which downloads data from SQLServer table, loads it to parquet file and then uploads it do DuckDB
- Added complete proxy set up in `SAPRFC` example (`viadot/examples/sap_rfc`)
- Added Databricks/Spark setup to the image. See README for setup & usage instructions
- Added rollback feature to `Databricks` source
- Changed all Prefect logging instances in the `sources` directory to native Python logging
- Changed `rm()`, `from_df()`, `to_df()` methods in `S3` Source
- Changed `get_request()` to `handle_api_request()` in `utils.py`
- Changed `SAPRFCV2` in `to_df()`for loop with generator
- Updated `Dockerfile` to remove obsolete `adoptopenjdk` and replace it with `temurin`

### Removed

- Removed the `env` param from `Databricks` source, as user can now store multiple configs for the same source using different config keys
- Removed Prefect dependency from the library (Python library, Docker base image)
- Removed `catch_extra_separators()` from `SAPRFCV2` class

## [0.4.3] - 2022-04-28

### Added

- Added `adls_file_name` in `SupermetricsToADLS` and `SharepointToADLS` flows
- Added `BigQueryToADLS` flow class which anables extract data from BigQuery
- Added `Salesforce` source
- Added `SalesforceUpsert` task
- Added `SalesforceBulkUpsert` task
- Added C4C secret handling to `CloudForCustomersReportToADLS` flow (`c4c_credentials_secret` parameter)

### Fixed

- Fixed `get_flow_last_run_date()` incorrectly parsing the date
- Fixed C4C secret handling (tasks now correctly read the secret as the credentials, rather than assuming the secret is a container for credentials for all environments and trying to access specific key inside it). In other words, tasks now assume the secret holds credentials, rather than a dict of the form `{env: credentials, env2: credentials2}`
- Fixed `utils.gen_bulk_insert_query_from_df()` failing with > 1000 rows due to INSERT clause limit by chunking the data into multiple INSERTs
- Fixed `get_flow_last_run_date()` incorrectly parsing the date
- Fixed `MultipleFlows` when one flow is passed and when last flow fails.
- Fixed issue with async usage in `Genesys.genesys_generate_exports()` (#669).

## [0.4.2] - 2022-04-08

### Added

- Added `AzureDataLakeRemove` task

### Changed

- Changed name of task file from `prefect` to `prefect_date_range`

### Fixed

- Fixed out of range issue in `prefect_date_range`

## [0.4.1] - 2022-04-07

### Changed

- bumped version

## [0.4.0] - 2022-04-07

### Added

- Added `custom_mail_state_handler` function that sends mail notification using custom smtp server.
- Added new function `df_clean_column` that cleans data frame columns from special characters
- Added `df_clean_column` util task that removes special characters from a pandas DataFrame
- Added `MultipleFlows` flow class which enables running multiple flows in a given order.
- Added `GetFlowNewDateRange` task to change date range based on Prefect flows
- Added `check_col_order` parameter in `ADLSToAzureSQL`
- Added new source `ASElite`
- Added KeyVault support in `CloudForCustomers` tasks
- Added `SQLServer` source
- Added `DuckDBToDF` task
- Added `DuckDBTransform` flow
- Added `SQLServerCreateTable` task
- Added `credentials` param to `BCPTask`
- Added `get_sql_dtypes_from_df` and `update_dict` util tasks
- Added `DuckDBToSQLServer` flow
- Added `if_exists="append"` option to `DuckDB.create_table_from_parquet()`
- Added `get_flow_last_run_date` util function
- Added `df_to_dataset` task util for writing DataFrames to data lakes using `pyarrow`
- Added retries to Cloud for Customers tasks
- Added `chunksize` parameter to `C4CToDF` task to allow pulling data in chunks
- Added `chunksize` parameter to `BCPTask` task to allow more control over the load process
- Added support for SQL Server's custom `datetimeoffset` type
- Added `AzureSQLToDF` task
- Added `AzureDataLakeRemove` task
- Added `AzureSQLUpsert` task

### Changed

- Changed the base class of `AzureSQL` to `SQLServer`
- `df_to_parquet()` task now creates directories if needed
- Added several more separators to check for automatically in `SAPRFC.to_df()`
- Upgraded `duckdb` version to 0.3.2

### Fixed

- Fixed bug with `CheckColumnOrder` task
- Fixed OpenSSL config for old SQL Servers still using TLS < 1.2
- `BCPTask` now correctly handles custom SQL Server port
- Fixed `SAPRFC.to_df()` ignoring user-specified separator
- Fixed temporary CSV generated by the `DuckDBToSQLServer` flow not being cleaned up
- Fixed some mappings in `get_sql_dtypes_from_df()` and optimized performance
- Fixed `BCPTask` - the case when the file path contained a space
- Fixed credential evaluation logic (`credentials` is now evaluated before `config_key`)
- Fixed "$top" and "$skip" values being ignored by `C4CToDF` task if provided in the `params` parameter
- Fixed `SQL.to_df()` incorrectly handling queries that begin with whitespace

### Removed

- Removed `autopick_sep` parameter from `SAPRFC` functions. The separator is now always picked automatically if not provided.
- Removed `dtypes_to_json` task to task_utils.py

## [0.3.2] - 2022-02-17

### Fixed

- fixed an issue with schema info within `CheckColumnOrder` class.

## [0.3.1] - 2022-02-17

### Changed

-`ADLSToAzureSQL` - added `remove_tab` parameter to remove unnecessary tab separators from data.

### Fixed

- fixed an issue with return df within `CheckColumnOrder` class.

## [0.3.0] - 2022-02-16

### Added

- new source `SAPRFC` for connecting with SAP using the `pyRFC` library (requires pyrfc as well as the SAP NW RFC library that can be downloaded [here](https://support.sap.com/en/product/connectors/nwrfcsdk.html)
- new source `DuckDB` for connecting with the `DuckDB` database
- new task `SAPRFCToDF` for loading data from SAP to a pandas DataFrame
- new tasks, `DuckDBQuery` and `DuckDBCreateTableFromParquet`, for interacting with DuckDB
- new flow `SAPToDuckDB` for moving data from SAP to DuckDB
- Added `CheckColumnOrder` task
- C4C connection with url and report_url documentation -`SQLIteInsert` check if DataFrame is empty or object is not a DataFrame
- KeyVault support in `SharepointToDF` task
- KeyVault support in `CloudForCustomers` tasks

### Changed

- pinned Prefect version to 0.15.11
- `df_to_csv` now creates dirs if they don't exist
- `ADLSToAzureSQL` - when data in csv coulmns has unnecessary "\t" then removes them

### Fixed

- fixed an issue with duckdb calls seeing initial db snapshot instead of the updated state (#282)
- C4C connection with url and report_url optimization
- column mapper in C4C source

## [0.2.15] - 2022-01-12

### Added

- new option to `ADLSToAzureSQL` Flow - `if_exists="delete"`
- `SQL` source: `create_table()` already handles `if_exists`; now it handles a new option for `if_exists()`
- `C4CToDF` and `C4CReportToDF` tasks are provided as a class instead of function

### Fixed

- Appending issue within CloudForCustomers source
- An early return bug in `UKCarbonIntensity` in `to_df` method

## [0.2.14] - 2021-12-01

### Fixed

- authorization issue within `CloudForCustomers` source

## [0.2.13] - 2021-11-30

### Added

- Added support for file path to `CloudForCustomersReportToADLS` flow
- Added `flow_of_flows` list handling
- Added support for JSON files in `AzureDataLakeToDF`

### Fixed

- `Supermetrics` source: `to_df()` now correctly handles `if_empty` in case of empty results

### Changed

- `Sharepoint` and `CloudForCustomers` sources will now provide an informative `CredentialError` which is also raised early. This will make issues with input credenials immediately clear to the user.
- Removed set_key_value from `CloudForCustomersReportToADLS` flow

## [0.2.12] - 2021-11-25

### Added

- Added `Sharepoint` source
- Added `SharepointToDF` task
- Added `SharepointToADLS` flow
- Added `CloudForCustomers` source
- Added `c4c_report_to_df` task
- Added `def c4c_to_df` task
- Added `CloudForCustomersReportToADLS` flow
- Added `df_to_csv` task to task_utils.py
- Added `df_to_parquet` task to task_utils.py
- Added `dtypes_to_json` task to task_utils.py

## [0.2.11] - 2021-10-30

### Fixed

- `ADLSToAzureSQL` - fixed path to csv issue.
- `SupermetricsToADLS` - fixed local json path issue.

## [0.2.10] - 2021-10-29

### Release due to CI/CD error

## [0.2.9] - 2021-10-29

### Release due to CI/CD error

## [0.2.8] - 2021-10-29

### Changed

- CI/CD: `dev` image is now only published on push to the `dev` branch
- Docker:
  - updated registry links to use the new `ghcr.io` domain
  - `run.sh` now also accepts the `-t` option. When run in standard mode, it will only spin up the `viadot_jupyter_lab` service.
    When ran with `-t dev`, it will also spin up `viadot_testing` and `viadot_docs` containers.

### Fixed

- ADLSToAzureSQL - fixed path parameter issue.

## [0.2.7] - 2021-10-04

### Added

- Added `SQLiteQuery` task
- Added `CloudForCustomers` source
- Added `CloudForCustomersToDF` and `CloudForCustomersToCSV` tasks
- Added `CloudForCustomersToADLS` flow
- Added support for parquet in `CloudForCustomersToDF`
- Added style guidelines to the `README`
- Added local setup and commands to the `README`

### Changed

- Changed CI/CD algorithm
  - the `latest` Docker image is now only updated on release and is the same exact image as the latest release
  - the `dev` image is released only on pushes and PRs to the `dev` branch (so dev branch = dev image)
- Modified `ADLSToAzureSQL` - _read_sep_ and _write_sep_ parameters added to the flow.

### Fixed

- Fixed `ADLSToAzureSQL` breaking in `"append"` mode if the table didn't exist (#145).
- Fixed `ADLSToAzureSQL` breaking in promotion path for csv files.

## [0.2.6] - 2021-09-22

### Added

- Added flows library docs to the references page

### Changed

- Moved task library docs page to topbar
- Updated docs for task and flows

## [0.2.5] - 2021-09-20

### Added

- Added `start` and `end_date` parameters to `SupermetricsToADLS` flow
- Added a tutorial on how to pull data from `Supermetrics`

## [0.2.4] - 2021-09-06

### Added

- Added documentation (both docstrings and MKDocs docs) for multiple tasks
- Added `start_date` and `end_date` parameters to the `SupermetricsToAzureSQL` flow
- Added a temporary workaround `df_to_csv_task` task to the `SupermetricsToADLS` flow to handle mixed dtype columns not handled automatically by DataFrame's `to_parquet()` method

## [0.2.3] - 2021-08-19

### Changed

- Modified `RunGreatExpectationsValidation` task to use the built in support for evaluation parameters added in Prefect v0.15.3
- Modified `SupermetricsToADLS` and `ADLSGen1ToAzureSQLNew` flows to align with this [recipe](https://docs.prefect.io/orchestration/flow_config/storage.html#loading-additional-files-with-git-storage) for reading the expectation suite JSON
  The suite now has to be loaded before flow initialization in the flow's python file and passed as an argument to the flow's constructor.
- Modified `RunGreatExpectationsValidation`'s `expectations_path` parameter to point to the directory containing the expectation suites instead of the
  Great Expectations project directory, which was confusing. The project directory is now only used internally and not exposed to the user
- Changed the logging of docs URL for `RunGreatExpectationsValidation` task to use GE's recipe from [the docs](https://docs.greatexpectations.io/docs/guides/validation/advanced/how_to_implement_custom_notifications/)

### Added

- Added a test for `SupermetricsToADLS` flow
  -Added a test for `AzureDataLakeList` task
- Added PR template for new PRs
- Added a `write_to_json` util task to the `SupermetricsToADLS` flow. This task dumps the input expectations dict to the local filesystem as is required by Great Expectations.
  This allows the user to simply pass a dict with their expectations and not worry about the project structure required by Great Expectations
- Added `Shapely` and `imagehash` dependencies required for full `visions` functionality (installing `visions[all]` breaks the build)
- Added more parameters to control CSV parsing in the `ADLSGen1ToAzureSQLNew` flow
- Added `keep_output` parameter to the `RunGreatExpectationsValidation` task to control Great Expectations output to the filesystem
- Added `keep_validation_output` parameter and `cleanup_validation_clutter` task to the `SupermetricsToADLS` flow to control Great Expectations output to the filesystem

### Removed

- Removed `SupermetricsToAzureSQLv2` and `SupermetricsToAzureSQLv3` flows
- Removed `geopy` dependency

## [0.2.2] - 2021-07-27

### Added

- Added support for parquet in `AzureDataLakeToDF`
- Added proper logging to the `RunGreatExpectationsValidation` task
- Added the `viz` Prefect extra to requirements to allow flow visualizaion
- Added a few utility tasks in `task_utils`
- Added `geopy` dependency
- Tasks:
  - `AzureDataLakeList` - for listing files in an ADLS directory
- Flows:
  - `ADLSToAzureSQL` - promoting files to conformed, operations,
    creating an SQL table and inserting the data into it
  - `ADLSContainerToContainer` - copying files between ADLS containers

### Changed

- Renamed `ReadAzureKeyVaultSecret` and `RunAzureSQLDBQuery` tasks to match Prefect naming style
- Flows:
  - `SupermetricsToADLS` - changed csv to parquet file extension. File and schema info are loaded to the `RAW` container.

### Fixed

- Removed the broken version autobump from CI

## [0.2.1] - 2021-07-14

### Added

- Flows:
  - `SupermetricsToADLS` - supporting immutable ADLS setup

### Changed

- A default value for the `ds_user` parameter in `SupermetricsToAzureSQLv3` can now be
  specified in the `SUPERMETRICS_DEFAULT_USER` secret
- Updated multiple dependencies

### Fixed

- Fixed "Local run of `SupermetricsToAzureSQLv3` skips all tasks after `union_dfs_task`" (#59)
- Fixed the `release` GitHub action

## [0.2.0] - 2021-07-12

### Added

- Sources:

  - `AzureDataLake` (supports gen1 & gen2)
  - `SQLite`

- Tasks:

  - `DownloadGitHubFile`
  - `AzureDataLakeDownload`
  - `AzureDataLakeUpload`
  - `AzureDataLakeToDF`
  - `ReadAzureKeyVaultSecret`
  - `CreateAzureKeyVaultSecret`
  - `DeleteAzureKeyVaultSecret`
  - `SQLiteInsert`
  - `SQLiteSQLtoDF`
  - `AzureSQLCreateTable`
  - `RunAzureSQLDBQuery`
  - `BCPTask`
  - `RunGreatExpectationsValidation`
  - `SupermetricsToDF`

- Flows:

  - `SupermetricsToAzureSQLv1`
  - `SupermetricsToAzureSQLv2`
  - `SupermetricsToAzureSQLv3`
  - `AzureSQLTransform`
  - `Pipeline`
  - `ADLSGen1ToGen2`
  - `ADLSGen1ToAzureSQL`
  - `ADLSGen1ToAzureSQLNew`

- Examples:
  - Hello world flow
  - Supermetrics Google Ads extract

### Changed

- Tasks now use secrets for credential management (azure tasks use Azure Key Vault secrets)
- SQL source now has a default query timeout of 1 hour

### Fixed

- Fix `SQLite` tests
- Multiple stability improvements with retries and timeouts

## [0.1.12] - 2021-05-08

### Changed

- Moved from poetry to pip

### Fixed

- Fix `AzureBlobStorage`'s `to_storage()` method is missing the final upload blob part
