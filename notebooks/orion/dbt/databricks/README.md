Testing DBT with Databricks Delta Lake.

### Prerequisites
- mount the data lake under `/mnt`, eg. mount the `raw` container under `/mnt/raw`
- create the `raw` schema with `CREATE SCHEMA IF NOT EXISTS raw LOCATION '/mnt/raw/dyvenia_sandbox/data_platform_test/raw'`
- write some data into the table with `df.write.saveAsTable("raw.c4c_test")`

### How to
- 
