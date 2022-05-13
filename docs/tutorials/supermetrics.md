# How to pull Supermetrics data

With Viadot you have opportunity to pull data from Supermetrics API, save it in parquet format on the Azure Data Lake. You can also load this data do Azure SQL DB.

If you need more info about Supermetric API please visit https://supermetrics.com/docs/product-api-getting-started/

## Pull data from Supermetrics and save output as a parquet file on Azure Data Lake

To pull data from Supermetrics we will create flow basing on `SupermetricsToADLS`
:::viadot.flows.SupermetricsToADLS 

Data types are automaticly detected and mapped to meet Microsoft Azure SQL Database requirments. Schema json will be stored in the data lake (parquet_file_directory/schema) 

## Example usage

```
flow = SupermetricsToADLS(
    "1-raw Supermetrics flow",
    ds_id="AAA",
    ds_accounts=[
         "12345"
        ],
    ds_user="username@x.com",
    date_range_type="last_30_days",
    fields=[
        "column1",
        "column2",
    ],
    max_rows=1000000,
    filter="some_filter",
    adls_sp_credentials_secret="adls credentials secret",
    expectation_suite=expectation_suite,
    adls_dir_path="adls directory path",
    parallel=True,   
)

flow.run()
```


