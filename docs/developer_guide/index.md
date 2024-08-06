# User guide

In this user guide, we're going to be developing a **new** source and data orchestration job.

We're first going to create a `PostgreSQL` source connector. Then, we'll create a Prefect flow that downloads a PostgreSQL table into a pandas `DataFrame`, and then uploads the data to Azure Data Lake.

## Environment

See the `CONTRIBUTING.md`
