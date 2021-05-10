from typing import Any, Dict, Literal

import prefect
from prefect import Task

from ..sources import AzureSQL


class CreateTableFromBlob(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(name="blob_to_azure_sql", *args, **kwargs)

    def __call__(self):
        """Bulk insert a CSV into an Azure SQL table"""

    def run(
        self,
        blob_path: str,
        schema: str,
        table: str,
        dtypes: Dict[str, Any],
        if_exists: Literal = ["fail", "replace"],
    ):
        """Create a table from an Azure Blob object.
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
        if_exists : Literal, optional
            What to do if the table already exists, by default ["fail", "replace"]
        """
        logger = prefect.context.get("logger")

        # create table
        azure_sql = AzureSQL()
        azure_sql.create_table(
            schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
        )
        fqn = f"{schema}.{table}" if schema else table
        logger.info(f"Successfully created table {fqn}.")

        # insert data
        azure_sql.bulk_insert(
            schema=schema,
            table=table,
            source_path=blob_path,
            if_exists=if_exists,
        )
        logger.info(f"Successfully inserted data into {fqn}.")
