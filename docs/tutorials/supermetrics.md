# How to pull Supermetrics data

With Viadot you have opportunity to pull data from Supermetrics API, save it in parquet format on the Azure Data Lake. You can also load this data do Azure SQL DB.

If you need more info about Supermetric API please visit https://supermetrics.com/docs/product-api-getting-started/

## Pull data from Supermetrics and save output as a parquet file on Azure Data Lake

To pull data from Supermetrics we will create flow basing on `SupermetricsToADLS`


Data types are automaticly detected and mapped to meet Microsoft Azure SQL Database requirments. Schema json will be stored in the data lake (parquet_file_directory/schema) 


