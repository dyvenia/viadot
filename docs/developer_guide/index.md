# Developer guide

In this guide, we're going to be developing a new source and data orchestration job.

We'll start by creating a `PostgreSQL` source connector. Then, we'll create a Prefect flow that downloads a PostgreSQL table into a pandas `DataFrame` and uploads the data as a Parquet file to Azure Data Lake.
