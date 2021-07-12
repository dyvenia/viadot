# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


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
- tasks now use secrets for credential management (azure tasks use Azure Key Vault secrets)
- SQL source now has a default query timeout of 1 hour


### Fixed
- Fix `SQLite` tests
- multiple stability improvements with retries and timeouts


## [0.1.12] - 2021-05-08

### Changed
- moved from poetry to pip

### Fixed
- Fix `AzureBlobStorage`'s `to_storage()` method is missing the final upload blob part
