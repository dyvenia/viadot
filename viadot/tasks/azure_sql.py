from typing import Any, Dict, Literal

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import AzureSQL


class CreateTableFromBlob(Task):
    def __init__(self, sep="\t", *args, **kwargs):
        self.sep = sep
        super().__init__(name="blob_to_azure_sql", *args, **kwargs)

    def __call__(self):
        """Bulk insert a CSV into an Azure SQL table"""

    @defaults_from_attrs("sep")
    def run(
        self,
        blob_path: str,
        schema: str,
        table: str,
        dtypes: Dict[str, Any],
        sep: str = None,
        if_exists: Literal = ["fail", "replace", "append"],
    ):
        """
        Create a table from an Azure Blob object.
        Currently, only CSV files are supported.

        Parameters
        ----------
        blob_path : str
            Path to the blob, eg. 'container_name/path/to.csv'.
        schema : str
            Destination schema.
        table : str
            Destination table.
        dtypes : Dict[str, Any]
            Data types to force.
        sep: str
            The separator to use to read the CSV file.
        if_exists : Literal, optional
            What to do if the table already exists.
        """

        fqn = f"{schema}.{table}" if schema else table
        azure_sql = AzureSQL(config_key="AZURE_SQL")
        # create table
        if if_exists == "replace":
            azure_sql.create_table(
                schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
            )

            self.logger.info(f"Successfully created table {fqn}.")

            # insert data
        azure_sql.bulk_insert(
            schema=schema,
            table=table,
            source_path=blob_path,
            sep=sep,
            if_exists=if_exists,
        )
        self.logger.info(f"Successfully inserted data into {fqn}.")


class RunAzureSQLDBQuery(Task):
    """
    Task for running an Azure SQL Database query.

    Args:
    - query (str, required): The query to execute on the database.
    """

    def __init__(self, *args, **kwargs):

        super().__init__(name="run_azure_sql_db_query", *args, **kwargs)

    def __call__(self):
        """Run an Azure SQL Database query"""

    def run(self, query: str):
        """Run an Azure SQL Database query

        Parameters
        ----------
        query : str
            The query to execute on the database.
        """

        # run the query and fetch the results if it's a select
        azure_sql = AzureSQL(config_key="AZURE_SQL")
        result = azure_sql.run(query)

        self.logger.info(f"Successfully ran the query.")
        return result
