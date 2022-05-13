# How to pull excel file from Sharepoint 

With Viadot you can download Excel file from Sharepoint and then upload it to Azure Data Lake. You can set a URL to file on Sharepoint an specify parameters such as path to local Excel file, number of rows and sheet number to be extracted. 

## Pull data from Sharepoint and save output as a csv file on Azure Data Lake

To pull Excel file from Sharepint we create flow basing on `SharepointToADLS`
:::viadot.flows.SharepointToADLS 

## Example usage

```
flow = SharepointToADLS(
    name="1-raw Sharepoint flow",
    nrows_to_df=100000,
    path_to_file="path to local file",
    url_to_file="https://aaa.sharepoint.com/aaa/file.xlsx",
    output_file_extension=".parquet",
    adls_sp_credentials_secret="adls credentials secret",
    adls_dir_path="adls directory path",
)

flow.run()
```
