# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


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
