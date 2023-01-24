from asyncio.log import logger
import json
from datetime import timedelta
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.tasks import defaults_from_attrs

from ..exceptions import ValidationError
from ..sources import AzureSQL
from ..utils import (
    build_merge_query,
    gen_bulk_insert_query_from_df,
    get_sql_server_table_dtypes,
)
from .azure_key_vault import AzureKeyVaultSecret


def get_credentials(credentials_secret: str, vault_name: str = None):
    """
    Get Azure credentials.
    If the credential secret is not provided it will be taken from Prefect Secrets. If Prefect Secrets does not
        contain the credential, it will be taken from the local credential file.

    Args:
        credentials_secret (str): The name of the Azure Key Vault secret containing a dictionary
        with SQL db credentials (server, db_name, user and password).
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

    Returns: Credentials

    """
    if not credentials_secret:
        # attempt to read a default for the service principal secret name
        try:
            credentials_secret = PrefectSecret(
                "AZURE_DEFAULT_SQLDB_SERVICE_PRINCIPAL_SECRET"
            ).run()
        except ValueError:
            pass

    if credentials_secret:
        azure_secret_task = AzureKeyVaultSecret()
        credentials_str = azure_secret_task.run(
            secret=credentials_secret, vault_name=vault_name
        )
        credentials = json.loads(credentials_str)

        return credentials


class CreateTableFromBlob(Task):
    def __init__(self, sep="\t", timeout: int = 3600, *args, **kwargs):
        self.sep = sep
        super().__init__(name="blob_to_azure_sql", timeout=timeout, *args, **kwargs)

    @defaults_from_attrs("sep")
    def run(
        self,
        blob_path: str,
        schema: str,
        table: str,
        dtypes: Dict[str, Any],
        sep: str = None,
        if_exists: Literal = ["fail", "replace", "append", "delete"],
    ):
        """
        Create a table from an Azure Blob object.
        Currently, only CSV files are supported.

        Args:
        from_path (str): Path to the file to be inserted.
        schema (str): Destination schema.
        table (str): Destination table.
        dtypes (Dict[str, Any]): Data types to force.
        sep (str): The separator to use to read the CSV file.
        if_exists (Literal, optional): What to do if the table already exists.
        """

        fqn = f"{schema}.{table}" if schema else table
        azure_sql = AzureSQL(config_key="AZURE_SQL")

        if if_exists == "replace":
            azure_sql.create_table(
                schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
            )

            self.logger.info(f"Successfully created table {fqn}.")

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
        if_exists: Literal["fail", "replace", "append", "delete"] = "fail",
        credentials_secret: str = None,
        timeout: int = 3600,
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
        super().__init__(name="azure_sql_bulk_insert", timeout=timeout, *args, **kwargs)

    @defaults_from_attrs("sep", "if_exists", "credentials_secret")
    def run(
        self,
        from_path: str = None,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        sep: str = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = None,
        credentials_secret: str = None,
        vault_name: str = None,
    ):
        """
        Bulk insert data from Azure Data Lake into an Azure SQL Database table.
        This task also creates the table if it doesn't exist.
        Currently, only CSV files are supported.

        Args:
        from_path (str): Path to the file to be inserted.
        schema (str): Destination schema.
        table (str): Destination table.
        dtypes (Dict[str, Any]): Data types to force.
        sep (str): The separator to use to read the CSV file.
        if_exists (Literal, optional): What to do if the table already exists.
        credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
        with SQL db credentials (server, db_name, user, and password).
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        fqn = f"{schema}.{table}" if schema else table
        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        azure_sql = AzureSQL(credentials=credentials)

        if if_exists == "replace":
            azure_sql.create_table(
                schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
            )

            self.logger.info(f"Successfully created table {fqn}.")

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
        if_exists: Literal["fail", "replace", "skip", "delete"] = "fail",
        credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
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
            timeout=timeout,
            *args,
            **kwargs,
        )

    @defaults_from_attrs("if_exists")
    def run(
        self,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace", "skip", "delete"] = None,
        credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ):
        """
        Create a table in Azure SQL Database.

        Args:
            schema (str, optional): Destination schema.
            table (str, optional): Destination table.
            dtypes (Dict[str, Any], optional): Data types to force.
            if_exists (Literal, optional): What to do if the table already exists.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with SQL db credentials (server, db_name, user, and password).
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        azure_sql = AzureSQL(credentials=credentials)

        fqn = f"{schema}.{table}" if schema is not None else table
        created = azure_sql.create_table(
            schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
        )
        if created:
            self.logger.info(f"Successfully created table {fqn}.")
        else:
            self.logger.info(
                f"Table {fqn} has not been created as if_exists is set to {if_exists}."
            )


class AzureSQLDBQuery(Task):
    """
    Task for running an Azure SQL Database query.

    Args:
        query (str, required): The query to execute on the database.
        credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
        with SQL db credentials (server, db_name, user, and password).
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
    """

    def __init__(
        self,
        credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.credentials_secret = credentials_secret
        self.vault_name = vault_name

        super().__init__(name="azure_sql_db_query", timeout=timeout, *args, **kwargs)

    def run(
        self,
        query: str,
        credentials_secret: str = None,
        vault_name: str = None,
    ):
        """Run an Azure SQL Database query

        Args:
            query (str, required): The query to execute on the database.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with SQL db credentials (server, db_name, user, and password).
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        azure_sql = AzureSQL(credentials=credentials)

        # run the query and fetch the results if it's a select
        result = azure_sql.run(query)

        self.logger.info(f"Successfully ran the query.")
        return result


class AzureSQLToDF(Task):
    """
    Task for loading the result of an Azure SQL Database query into a pandas DataFrame.

    Args:
        query (str, required): The query to execute on the database.
        credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
        with SQL db credentials (server, db_name, user, and password).
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
    """

    def __init__(
        self,
        credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.credentials_secret = credentials_secret
        self.vault_name = vault_name

        super().__init__(name="azure_sql_to_df", timeout=timeout, *args, **kwargs)

    def run(
        self,
        query: str,
        credentials_secret: str = None,
        vault_name: str = None,
    ):
        """Load the result of an Azure SQL Database query into a pandas DataFrame.

        Args:
            query (str, required): The query to execute on the database.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with SQL db credentials (server, db_name, user, and password).
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        azure_sql = AzureSQL(credentials=credentials)

        df = azure_sql.to_df(query)
        nrows = df.shape[0]
        ncols = df.shape[1]

        self.logger.info(
            f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
        )
        return df


class CheckColumnOrder(Task):
    """
    Task for checking the order of columns in the loaded DF and in the SQL table into which the data from DF will be loaded.
    If order is different then DF columns are reordered according to the columns of the SQL table.
    """

    def __init__(
        self,
        table: str = None,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "replace",
        df: pd.DataFrame = None,
        credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.credentials_secret = credentials_secret
        self.vault_name = vault_name

        super().__init__(
            name="run_check_column_order", timeout=timeout, *args, **kwargs
        )

    def df_change_order(
        self, df: pd.DataFrame = None, sql_column_list: List[str] = None
    ):
        df_column_list = list(df.columns)
        if set(df_column_list) == set(sql_column_list):
            df_changed = df.loc[:, sql_column_list]
        else:
            raise ValidationError(
                "Detected discrepancies in number of columns or different column names between the CSV file and the SQL table!"
            )

        return df_changed

    def sanitize_columns(self, df: pd.DataFrame = None):
        """
        Function to remove spaces at the end of column name.
        Args:
            df(pd.DataFrame): Dataframe to transform. Defaults to None.
        """
        for col in df.columns:
            df = df.rename(columns={col: col.strip()})
        return df

    def run(
        self,
        table: str = None,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "replace",
        df: pd.DataFrame = None,
        credentials_secret: str = None,
        vault_name: str = None,
    ):
        """
        Run a checking column order

        Args:
            table (str, optional): SQL table name without schema. Defaults to None.
            schema (str, optional): SQL schema name. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            df (pd.DataFrame, optional): Data Frame. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with SQL db credentials (server, db_name, user, and password). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """
        if if_exists not in ["fail", "replace", "append", "delete"]:
            raise ValueError(
                f"Please select one of the allowed 'if_exists' values: 'fail', 'replace', 'append', 'delete'."
            )
        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        azure_sql = AzureSQL(credentials=credentials)
        df = self.sanitize_columns(df)
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
        result = azure_sql.run(query=query)
        table_exists = len(result) != 0
        if not table_exists:
            self.logger.warning("Target table doesn't exists.")
            return df
        if if_exists == "fail":
            raise ValueError(
                "The table already exists and 'if_exists' is set to 'fail'."
            )
        if if_exists in ["append", "delete"]:
            sql_column_list = [row[0] for row in result]
            df_column_list = list(df.columns)
            if sql_column_list != df_column_list:
                self.logger.warning(
                    "Detected column order difference between the CSV file and the table. Reordering..."
                )
                df = self.df_change_order(df=df, sql_column_list=sql_column_list)
            else:
                return df
        else:
            self.logger.info("The table will be replaced.")
            return df


class AzureSQLUpsert(Task):
    """Task for upserting data from a pandas DataFrame into AzureSQL.

    Args:
        schema (str, optional): The schema where the data should be upserted. Defaults to None.
        table (str, optional): The table where the data should be upserted. Defaults to None.
        on (str, optional): The field on which to merge (upsert). Defaults to None.
        credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
    """

    def __init__(
        self,
        schema: str = None,
        table: str = None,
        on: str = None,
        credentials_secret: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.schema = schema
        self.table = table
        self.on = on
        self.credentials_secret = credentials_secret
        super().__init__(name="azure_sql_upsert", timeout=timeout, *args, **kwargs)

    @defaults_from_attrs(
        "schema",
        "table",
        "on",
        "credentials_secret",
    )
    def run(
        self,
        df: pd.DataFrame,
        schema: str = None,
        table: str = None,
        on: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
    ):
        """Upsert data from a pandas DataFrame into AzureSQL using a temporary staging table.

        Args:
            df (pd.DataFrame): The DataFrame to upsert.
            schema (str, optional): The schema where the data should be upserted. Defaults to None.
            table (str, optional): The table where the data should be upserted. Defaults to None.
            on (str, optional): The field on which to merge (upsert). Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        if not table:
            raise ValueError("'table' was not provided.")

        if not on:
            raise ValueError("'on' was not provided.")

        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        azure_sql = AzureSQL(credentials=credentials)

        # Create a temporary staging table.
        # Hashtag marks a temp table in SQL server.
        stg_table = "#" + "stg_" + table
        dtypes = get_sql_server_table_dtypes(
            schema=schema, table=table, con=azure_sql.con
        )
        created = azure_sql.create_table(
            schema=schema, table=stg_table, dtypes=dtypes, if_exists="fail"
        )

        # Insert data into the temp table
        stg_table_fqn = f"{schema}.{stg_table}"
        insert_query = gen_bulk_insert_query_from_df(df, table_fqn=stg_table_fqn)
        inserted = azure_sql.run(insert_query)

        # Upsert into prod table
        merge_query = build_merge_query(
            stg_schema=schema,
            stg_table=stg_table,
            schema=schema,
            table=table,
            primary_key=on,
            con=azure_sql.con,
        )

        merged = azure_sql.run(merge_query)

        if merged:
            rows = df.shape[0]
            table_fqn = f"{schema}.{table}"
            self.logger.info(
                f"Successfully upserted {rows} rows of data into table '{table_fqn}'."
            )

        return True
