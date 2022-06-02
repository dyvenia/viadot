# How to use Aselite flow

Aselite is a prefect flow that extract data from SQL Server database and save collected data as .csv in Azure Data Lake in defined folder.


## Example uasage

```
flow = ASELiteToADLS(
   "1-raw ASELite mytable extract",
   query = query,
   sqldb_credentials_secret=[Your sqldb credentials],
   vault_name=[Your vault name],
   file_path = "mytable.csv",
   to_path ="raw/aselite/mytable.csv",
   sp_credentials_secret=[Your adls credentials],
   remove_special_characters=True,
   state_handlers=[slack_handler]
   ) 
flow.run()
```

::: viadot.flows.aselite_to_adls.ASELiteToADLS