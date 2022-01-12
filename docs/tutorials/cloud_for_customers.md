# How to pull CloudForCustomers data

With Viadot you can pull data from CloudForCustomers API, save it in csv and parquet format on the Azure Data Lake.  
You can connect directly with prepeard report URL adress or with table adding special parameters to fetch the data. 

## Pull data from Supermetrics and save output as a csv file on Azure Data Lake

To pull data from CloudForCustomers we will create flow basing on `CloudForCustomersReportToADLS`
:::viadot.flows.CloudForCustomersReportToADLS 