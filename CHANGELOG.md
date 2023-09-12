# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [Unreleased]
### Added

### Fixed

### Changed

- Changed, `Mindful` credentials passed by the `auth` parameter, instead of by the `header`.

## [0.4.19] - 2023-08-31
### Added
- Added `add_viadot_metadata_columns` function that will be used as a decorator for `to_df` class methods.
- Added `TransformAndCatalog` flow.
- Added `CloneRepo` task.
- Added `LumaIngest` task.

### Fixed
- Updated Dockerfile - Changed Linux (RPM/DEB/APK) installer packages.


## [0.4.18] - 2023-07-27
### Added
- Added `SQLServerToParquet` flow.
- Added `SAPBW` source class.
- Added `SAPBWToDF` task class.
- Added `SAPBWToADLS` flow class.
- Added a new `end_point` parameter in `genesys_api_connection` to make it more generic.
- Added `VidClubToADLS` flow class.

### Fixed
- Fixed a bug in `subject` (extra separator) and in `receivers` (long strings) parameters in `Outlook` connector. 
- Fixed issue with credentials handling in `VidClub` source class.
- Fixed issue with missing arguments in `VidClubToDF` task class.

### Changed
- Genesys API call method and the name changed from `genesys_generate_exports` to `genesys_api_connection`. 
- Added `GET` connection inside the method `genesys_api_connection`.
- Added new parameters in the `GenesysToCSV` task to be able to extract `web message` files.
- Changed looping structure for API calls in `VidClub` source class to use time intervals.
- Changed `VidClubToDF` task class to use total_load function from source.

### Removed
- Removed methods never used in production: `get_analitics_url_report`, `get_all_schedules_job`, `schedule_report`,
`to_df`, `delete_scheduled_report_job` and `generate_reporting_export`.


## [0.4.17] - 2023-06-15
### Fixed
- Fixed issue with `tzlocal` for O365 package


## [0.4.16] - 2023-06-15
### Added
- Added `VidClub` source class
- Added `VidClubToDF` task class
- Added `GetPendingSalesOrderData`, `GetSalesInvoiceData`, `GetSalesReturnDetailData` 
 `GetSalesOrderData` endpoints in `BusinessCore()` source.
- Added `url` parameter to `CustomerGauge` source, and `endpoint_url` parameter to `CustomerGaugeToDF` task 
and `CustomerGaugeToADLS` flow. This parameter enables to pass the endpoint URL by user.
- Added new parameter `outbox_list` at all leves in `Outlook` connector to tag mailbox folders.

### Fixed
- Fixed `to_parquet()` from `base.py` when there is no directory specified in path 

### Changed
- Changed loop when retrieving email in `Outlook` source file, to cover all possible folders and subfolders.


## [0.4.15] - 2023-05-11
### Added
- Added `BusinessCore` source class
- Added `BusinessCoreToParquet` task class
- Added `Eurostat` source, task and flow classes
- Added `verify` parameter to `handle_api_response()`.
- Added `to_parquet()` in `base.py`
- Added new source class `SAPRFCV2` in `sap_rfc.py` with new approximation.
- Added new parameter `rfc_replacement` to `sap_rfc_to_adls.py` to replace
an extra separator character within a string column to avoid conflicts.
- Added `rfc_unique_id` in `SAPRFCV2` to merge chunks on this column.
- Added `close_connection()` to `SAPRFC` and `SAPRFCV2`

### Fixed
- Removed `try-except` sentence and added a new logic to remove extra separators in `sap_rfc.py` 
source file, to vaoid a mismatch in columns lenght between iterative connections to SAP tables.
- When `SAP` tables are updated during `sap_rfc.py` scrip running, if there are chunks, the
columns in the next chunk are unrelated rows.
- Fixed `sap_rfc.py` source file to not breakdown by both, 
and extra separator in a row and adding new rows in SAP table between iterations.


## [0.4.14] - 2023-04-13
### Added
- Added `anonymize_df` task function to `task_utils.py` to anonymize data in the dataframe in selected columns.
- Added `Hubspot` source class
- Added `HubspotToDF` task class
- Added `HubspotToADLS` flow class
- Added `CustomerGauge` source class
- Added `CustomerGaugeToDF` task class
- Added `CustomerGaugeToADLS` flow class

## [0.4.13] - 2023-03-15
### Added
- Added `validate_date_filter` parameter to `Epicor` source, `EpicorOrdersToDF` task and `EpicorOrdersToDuckDB` flow.
This parameter enables user to decide whether or not filter should be validated.
- Added `Mediatool` source class
- Added `MediatoolToDF` task class
- Added `MediatoolToADLS` flow class
- Added option to disable `check_dtypes_sort` in `ADLSToAzureSQL` flow.
- Added `query` parameter to `BigQueryToADLS` flow and `BigqueryToDF` task to be able to enter custom SQL query.
- Added new end point `conversations/details/query` connection to `Genesys` task.
- Added new task `filter_userid` in `GenesysToADLS` flow to filter out by user Ids list, previously passed by the user.

### Changed
- Changed parameter name in `BigQueryToADLS` flow - from `credentials_secret` to `credentials_key`


## [0.4.12] - 2023-01-31
### Added
- Added `view_type_time_sleep` to the Genesys `queue_performance_detail_view`.
- Added `FileNotFoundError` to catch up failures in `MindfulToCSV` and when creating SQL tables.
- Added `check_dtypes_sort` task into `ADLSToAzureSQL` to check if dtypes is properly sorted.
- Added `timeout` parameter to all `Task`s where it can be added.
- Added `timeout` parameter to all `Flow`s where it can be added.
- Added `adls_bulk_upload` task function to `task_utils.py`
- Added `get_survey_list` into `Mindful` Source file.

### Changed
- Updated `genesys_to_adls.py` flow with the `adls_bulk_upload` task
- Updated `mindful_to_adls.py` flow with the `adls_bulk_upload` task
- Changed `MindfulToCSV` task to download surveys info.


## [0.4.11] - 2022-12-15
### Added
- Added into `Genesys` the new view type `AGENT`. 

### Changed
- Changed data extraction logic for `Outlook` data.

## [0.4.10] - 2022-11-16
### Added
- Added `credentials_loader` function in utils
- Added new columns to `Epicor` source - `RequiredDate` and `CopperWeight`
- Added timeout to `DuckDBQuery` and `SAPRFCToDF`
- Added support for SQL queries with comments to `DuckDB` source
- Added "WITH" to query keywords in `DuckDB` source
- Added `avro-python3` library to `requirements`

### Changed
- Changed `duckdb` version to `0.5.1`
- Added new column into Data Frames created with `Mindful`.
- Added region parameter as an entry argument in `MindfulToADLS`.

### Fixed
- Fixed incorrect `if_exists="delete"` handling in `DuckDB.create_table_from_parquet()`
- Fixed `test_duckdb_to_sql_server.py` tests - revert to a previous version
- Removed `test__check_if_schema_exists()` test


## [0.4.9] - 2022-09-27
### Added
- Added new column named `_viadot_downloaded_at_utc` in genesys files with the datetime when it is created.
- Added sftp source class `SftpConnector`
- Added sftp tasks `SftpToDF` and `SftpList` 
- Added sftp flows `SftpToAzureSQL` and `SftpToADLS`
- Added new source file `mindful` to connect with mindful API.
- Added new task file `mindful` to be called by the Mindful Flow.
- Added new flow file `mindful_to_adls` to upload data from Mindful API tp ADLS.
- Added `recursive` parameter to `AzureDataLakeList` task


## [0.4.8] - 2022-09-06
### Added
- Added `protobuf` library to requirements


## [0.4.7] - 2022-09-06
### Added
- Added new flow - `SQLServerTransform` and new task `SQLServerQuery` to run queries on SQLServer
- Added `duckdb_query` parameter to `DuckDBToSQLServer` flow to enable option to create table
using outputs of SQL queries 
- Added handling empty DF in `set_new_kv()` task
- Added `update_kv` and `filter_column` params to `SAPRFCToADLS` and `SAPToDuckDB` flows and added `set_new_kv()` task
in `task_utils`
- Added Genesys API source `Genesys`
- Added tasks `GenesysToCSV` and `GenesysToDF`
- Added flows `GenesysToADLS` and `GenesysReportToADLS`
- Added `query` parameter to  `PrefectLogs` flow

### Changed
- Updated requirements.txt
- Changed 'handle_api_response()' method by adding more requests method also added contex menager


## [0.4.6] - 2022-07-21
### Added
- Added `rfc_character_limit` parameter in `SAPRFCToDF` task, `SAPRFC` source, `SAPRFCToADLS` and `SAPToDuckDB` flows
- Added `on_bcp_error` and `bcp_error_log_path` parameters in `BCPTask`
- Added ability to process queries which result exceed SAP's character per low limit in `SAPRFC` source
- Added new flow `PrefectLogs` for extracting all logs from Prefect with details
- Added `PrefectLogs` flow

### Changed
- Changed `CheckColumnOrder` task and `ADLSToAzureSQL` flow to handle appending to non existing table
- Changed tasks order in `EpicorOrdersToDuckDB`, `SAPToDuckDB` and `SQLServerToDuckDB` - casting 
DF to string before adding metadata
- Changed `add_ingestion_metadata_task()` to not to add metadata column when input DataFrame is empty
- Changed `check_if_empty_file()` logic according to changes in `add_ingestion_metadata_task()`
- Changed accepted values of `if_empty` parameter in `DuckDBCreateTableFromParquet`
- Updated `.gitignore` to ignore files with `*.bak` extension and to ignore `credentials.json` in any directory
- Changed logger messages in `AzureDataLakeRemove` task

### Fixed
- Fixed handling empty response in `SAPRFC` source
- Fixed issue in `BCPTask` when log file couln't be opened.
- Fixed log being printed too early in `Salesforce` source, which would sometimes cause a `KeyError`
- `raise_on_error` now behaves correctly in `upsert()` when receiving incorrect return codes from Salesforce

### Removed
- Removed option to run multiple queries in `SAPRFCToADLS`


## [0.4.5] - 2022-06-23
### Added
- Added `error_log_file_path` parameter in `BCPTask` that enables setting name of errors logs file 
- Added `on_error` parameter in `BCPTask` that tells what to do if bcp error occurs. 
- Added error log file and `on_bcp_error` parameter in `ADLSToAzureSQL`
- Added handling POST requests in `handle_api_response()` add added it to `Epicor` source.
- Added `SalesforceToDF` task
- Added `SalesforceToADLS` flow
- Added `overwrite_adls` option to `BigQueryToADLS` and `SharepointToADLS`
- Added `cast_df_to_str` task in `utils.py` and added this to `EpicorToDuckDB`, `SAPToDuckDB`, `SQLServerToDuckDB`
- Added `if_empty` parameter in `DuckDBCreateTableFromParquet` task and in `EpicorToDuckDB`, `SAPToDuckDB`,
`SQLServerToDuckDB` flows to check if output Parquet is empty and handle it properly.
- Added `check_if_empty_file()` and `handle_if_empty_file()` in `utils.py`


## [0.4.4] - 2022-06-09
### Added
- Added new connector - Outlook. Created `Outlook` source, `OutlookToDF` task and `OutlookToADLS` flow.
- Added new connector - Epicor. Created `Epicor` source, `EpicorToDF` task and `EpicorToDuckDB` flow.
- Enabled Databricks Connect in the image. To enable, [follow this guide](./README.md#executing-spark-jobs)
- Added `MySQL` source and `MySqlToADLS` flow
- Added `SQLServerToDF` task
- Added `SQLServerToDuckDB` flow which downloads data from SQLServer table, loads it to parquet file and then uplads it do DuckDB
- Added complete proxy set up in `SAPRFC` example (`viadot/examples/sap_rfc`)

### Changed
- Changed default name for the Prefect secret holding the name of the Azure KV secret storing Sendgrid credentials


## [0.4.3] - 2022-04-28
### Added
- Added `func` parameter to `SAPRFC` 
- Added `SAPRFCToADLS` flow which downloads data from SAP Database to to a pandas DataFrame, exports df to csv and uploads it to Azure Data Lake.
- Added `adls_file_name` in  `SupermetricsToADLS` and `SharepointToADLS` flows
- Added `BigQueryToADLS` flow class which anables extract data from BigQuery.
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
- Added `custom_mail_state_handler` task that sends email notification using a custom SMTP server.
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
-`ADLSToAzureSQL` - added `remove_tab`  parameter to remove uncessery tab separators from data. 

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
- C4C connection with url and report_url documentation
-`SQLIteInsert` check if DataFrame is empty or object is not a DataFrame
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
- Added `c4c_report_to_df` taks
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
- Modified `ADLSToAzureSQL` - *read_sep* and *write_sep* parameters added to the flow.

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
