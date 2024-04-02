# Adding a source

## 1. Add a source

To add a source, create a new file in `viadot/sources`. The source must inherit from the `Source` base class and accept a `credentials` parameter.

## 2. Add a task

Within the task, you should handle the authentication to the source. For this, utilize either a Prefect secret or the Azure Key Vault secret. See existing tasks, eg. `AzureDataLakeDownload`, for reference. Note that we sometimes also provide a default value for the secret name which is stored as a Prefect secret itself. This is so that you can safely publish your flow code in the "infrastructure as code" spirit, without revealing the names of the actual keys used in your vault. You can instead only provide the name of the Prefect secret holding the actual name. These defaults can be configured in your local Prefect config (`.prefect/config.toml`) or in Prefect cloud. For example, let's say you have a secret set by another department in your organization called `my_service_principal`, storing the credentials of the service account used to authenticate to the data lake. Let's assume the name of this service account should be protected. With the implementation used eg. in `AzureDataLakeDownload`, you can create Prefect secret called eg. `my_service_account_1_name` and only refer to this secret in your flow, eg. in this task, by setting `sp_credentials_secret` to `my_service_account_1_name`.

## 3. Integrate into a flow

Now you can finally integrate the source into a full flow. See [Adding a flow](../tutorials/adding_flow.md)
