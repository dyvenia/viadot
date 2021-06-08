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
                DATA_SOURCE = '{self.credentials['data_source']}',
                DATAFILETYPE='char',
                FIELDTERMINATOR='{sep}',
                ROWTERMINATOR='0x0a',
                FIRSTROW=2,
                KEEPIDENTITY,
                TABLOCK,
                CODEPAGE = '65001'
            );
        """
        if if_exists == "replace":
            self.run(f"DELETE FROM {schema}.{table}")
        self.run(insert_sql)
        return True

    def create_external_database(url):
        """TODO"""

        credential_name = "data_lake_credential"
        shared_access_token = ""

        # "CREATE MASTER KEY ENCRYPTION BY PASSWORD = <enter very strong password here>""

        create_external_db_credential_sql = f"""
        USE [acdb]
        CREATE DATABASE SCOPED CREDENTIAL {credential_name}
        WITH IDENTITY = 'SHARED ACCESS SIGNATURE'
        SECRET = '{shared_access_token}';
        """

        create_external_db_sql = f"""
        USE [acdb]
        CREATE EXTERNAL DATA SOURCE testing WITH (
        LOCATION = 'https://dyvenia1.blob.core.windows.net/tests',
        CREDENTIAL = {credential_name}
        );
        """

        pass
