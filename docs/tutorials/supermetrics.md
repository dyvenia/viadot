# How to pull Supermetrics data

With Viadot you have opportunity to pull data from Supermetrics API, save it in parquet format on the Azure Data Lake. You can also load this data do Azure SQL DB.

If you need more info about Supermetric API please visit https://supermetrics.com/docs/product-api-getting-started/

## Pull data from Supermetrics and save output as a parquet file on Azure Data Lake

To pull data from Supermetrics we will create flow basing on `SupermetricsToADLS`
:::viadot.flows.SupermetricsToADLS

Data types are automatically detected and mapped to meet Microsoft Azure SQL Database requirements. Schema json will be stored in the data lake (parquet_file_directory/schema)
