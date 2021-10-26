# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
### Changed
- CI/CD: `dev` image is now only published on push to the `dev` branch
- Docker: 
  - updated registry links to use the new `ghcr.io` domain
  - `run.sh` now also accepts the `-t` option. When run in standard mode, it will only spin up the `viadot_jupyter_lab` service.
  When ran with `-t dev`, it will also spin up `viadot_testing` and `viadot_docs` containers.

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
