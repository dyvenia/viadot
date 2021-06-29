import json
from datetime import timedelta
from typing import Any, Dict, Literal

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import AzureSQL
from .azure_key_vault import ReadAzureKeyVaultSecret


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


class AzureSQLBulkInsert(Task):
    def __init__(
        self,
        from_path: str = None,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        sep="\t",
        if_exists: Literal["fail", "replace", "append"] = "fail",
        credentials_secret: str = "AZURE_SQL",
        *args,
        **kwargs,
    ):
        self.from_path = from_path
        self.schema = schema
        self.table = table
        self.dtypes = dtypes
        self.sep = sep
        self.if_exists = if_exists
        self.credentials_secret = credentials_secret
        super().__init__(name="azure_sql_bulk_insert", *args, **kwargs)

    def __call__(self):
        """Bulk insert CSV(s) into an Azure SQL table"""

    @defaults_from_attrs("sep", "if_exists", "credentials_secret")
    def run(
        self,
        from_path: str = None,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        sep: str = None,
        if_exists: Literal["fail", "replace", "append"] = None,
        credentials_secret: str = None,
    ):
        """
        Bulk insert data from Azure Data Lake into an Azure SQL Database table.
        This task also creates the table if it doesn't exist.
        Currently, only CSV files are supported.

        Parameters
        ----------
        from_path : str
            Path to the file(s) to be inserted.
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
        azure_sql = AzureSQL(config_key=credentials_secret)
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
            source_path=from_path,
            sep=sep,
            if_exists=if_exists,
        )
        self.logger.info(f"Successfully inserted data into {fqn}.")


class AzureSQLCreateTable(Task):
    def __init__(
        self,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace", "append"] = "fail",
        credentials_secret: str = "AZURE_SQL",
        vault_name: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.schema = schema
        self.table = table
        self.dtypes = dtypes
        self.if_exists = if_exists
        self.credentials_secret = credentials_secret
        self.vault_name = vault_name
        super().__init__(
            name="azure_sql_create_table",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Create a table in Azure SQL"""

    @defaults_from_attrs("if_exists", "credentials_secret")
    def run(
        self,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace", "append"] = None,
        credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ):
        """
        Create a table in Azure SQL Database.

        Parameters
        ----------
        schema : str
            Destination schema.
        table : str
            Destination table.
        dtypes : Dict[str, Any]
            Data types to force.
        if_exists : Literal, optional
            What to do if the table already exists.
        """

        azure_secret_task = ReadAzureKeyVaultSecret()
        credentials_str = azure_secret_task.run(
            secret=credentials_secret, vault_name=vault_name
        )
        credentials = json.loads(credentials_str)

        fqn = f"{schema}.{table}" if schema else table
        azure_sql = AzureSQL(credentials=credentials)

        if if_exists == "replace":
            azure_sql.create_table(
                schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
            )

            self.logger.info(f"Successfully created table {fqn}.")


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
