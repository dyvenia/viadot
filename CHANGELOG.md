# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Added support for parquet in `AzureDataLakeToDF`
- Added proper logging to the `RunGreatExpectationsValidation` task
- Tests
- Flows:
  - `ADLSToAzureSQL` - promoting files to conformed, operations and creating SQL table
   

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
