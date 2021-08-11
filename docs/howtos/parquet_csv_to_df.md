## Loading Data from Azure Data Lake file to Pandas DataFrame

For getting pandas DataFrame from parquet or CSV file stored in ADLS use AzureDataLakeToDF class.  
:::viadot.tasks.AzureDataLakeToDF


```python
from viadot.tasks.azure_data_lake import AzureDataLakeToDF
get_df = AzureDataLakeToDF()
adls_file_path = "direct_adls_path_to_your_file"
df = get_df.run(path=adls_file_path,  vault_name="your_vault")
```
