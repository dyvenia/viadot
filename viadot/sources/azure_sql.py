from typing import Any, Dict, List, Literal

from prefect.utilities import logging

from .base import SQL

logger = logging.get_logger(__name__)


class AzureSQL(SQL):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.credentials["driver"] = "ODBC Driver 17 for SQL Server"

    @property
    def schemas(self) -> List[str]:
        schemas_tuples = self.run("SELECT s.name as schema_name from sys.schemas s")
        return [schema_tuple[0] for schema_tuple in schemas_tuples]

    @property
    def tables(self) -> List[str]:
        tables_tuples = self.run("SELECT * FROM information_schema.tables")
        return [table for row in tables_tuples for table in row]

    def bulk_insert(
        self,
        table: str,
        schema: str = None,
        source_path: str = None,
        sep="\t",
        if_exists: Literal = "append",
    ):
        if schema is None:
            schema = self.DEFAULT_SCHEMA
        fqn = f"{schema}.{table}"
        insert_sql = f"""
            BULK INSERT {fqn} FROM '{source_path}'
            WITH (
                CHECK_CONSTRAINTS,
                DATA_SOURCE='{self.credentials['data_source']}',
                DATAFILETYPE='char',
                FIELDTERMINATOR='{sep}',
                ROWTERMINATOR='0x0a',
                FIRSTROW=2,
                KEEPIDENTITY,
                TABLOCK,
                CODEPAGE='65001'
            );
        """
        if if_exists == "replace":
            self.run(f"DELETE FROM {schema}.{table}")
        self.run(insert_sql)
        return True

    def create_external_database(
        self,
        external_database_name: str,
        storage_account_name: str,
        container_name: str,
        sas_token: str,
        master_key_password: str,
        credential_name: str = None,
    ):
        """Create an external database. Used to eg. execute BULK INSERT or OPENROWSET
        queries.

        Args:
            external_database_name (str): The name of the extrnal source (db) to be created.
            storage_account_name (str): The name of the Azure storage account.
            container_name (str): The name of the container which should become the "database".
            sas_token (str): The SAS token to be used as the credential. Note that the auth
            system in Azure is pretty broken and you might need to paste here your storage
            account's account key instead.
            master_key_password (str): The password for the database master key of your
            Azure SQL Database.
            credential_name (str): How to name the SAS credential. This is really an Azure
            internal thing and can be anything. By default '{external_database_name}_credential`.
        """

        # stupid Microsoft thing
        if sas_token.startswith("?"):
            sas_token = sas_token[1:]

        if credential_name is None:
            credential_name = f"{external_database_name}_credential"

        create_master_key_sql = (
            f"CREATE MASTER KEY ENCRYPTION BY PASSWORD = {master_key_password}"
        )

        create_external_db_credential_sql = f"""
        CREATE DATABASE SCOPED CREDENTIAL {credential_name}
        WITH IDENTITY = 'SHARED ACCESS SIGNATURE'
        SECRET = '{sas_token}';
        """

        create_external_db_sql = f"""
        CREATE EXTERNAL DATA SOURCE {external_database_name} WITH (
        LOCATION = f'https://{storage_account_name}.blob.core.windows.net/{container_name}',
        CREDENTIAL = {credential_name}
        );
        """

        self.run(create_master_key_sql)
        self.run(create_external_db_credential_sql)
        self.run(create_external_db_sql)

    def exists(self, table: str, schema: str = None) -> bool:
        """Check whether a table exists.

        Args:
            table (str): The table to be checked.
            schema (str, optional): The schema whethe the table is located. Defaults to 'dbo'.

        Returns:
            bool: Whether the table exists.
        """

        if not schema:
            schema = "dbo"

        list_table_info_query = f"""
            SELECT *
            FROM sys.tables t
            JOIN sys.schemas s
                ON t.schema_id = s.schema_id
            WHERE s.name = '{schema}' AND t.name = '{table}'
        """
        exists = bool(self.run(list_table_info_query))
        return exists
