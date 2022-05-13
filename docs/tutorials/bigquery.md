# How to pull data from BigQuery 

With Viadot you can download data from BigQuery project and then upload it to Azure Data Lake.
For querying on database - dataset and table name is required.
The name of the project is taken from the config/credential json file so there is no need to enter its name.

Note that credentials used for authentication can be generated only for User Principal who has access to specific BigQuery project.


## Pull data from BigQuery and save output as a csv/parquet file on Azure Data Lake

To pull the data from BigQuery we create flow basing on `BigQueryToADLS`
:::viadot.flows.BigQueryToADLS

## Example usage

```
flow = BigQueryToADLS(
    "1-raw BigQuery flow",
    dataset_name = "name",
    table_name = "name",
    adls_dir_path="adls directory path",
    output_file_extension=".csv",
    adls_sp_credentials_secret="adls credentials secret",
    if_exists="replace",
    credentials_secret="credentials secret",
    vault_name="vault name",
)

flow.run()
```
