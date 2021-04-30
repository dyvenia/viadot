from typing import Any, Dict, List, Literal

import pyodbc
from prefect.utilities import logging

from ..config import local_config
from .base import Source

logger = logging.get_logger(__name__)


class AzureSQL(Source):
    """
    Azure SQL Database connector (do not confuse with managed SQL Server
    that's also provided as a service by Azure).

    Parameters
    ----------
    server : str, optional
        Server address, eg. mydb.database.windows.net, by default None
    db : str, optional
        Name of the database, by default None
    user : str, optional
        Username, by default None
    pw : str, optional
        Password, by default None
    """

    def __init__(
        self,
        server: str = None,
        db: str = None,
        user: str = None,
        pw: str = None,
        *args,
        **kwargs,
    ):

        DEFAULT_CREDENTIALS = local_config.get("AZURE_SQL")
        credentials = kwargs.pop("credentials", DEFAULT_CREDENTIALS)
        super().__init__(*args, credentials=credentials, **kwargs)

        # Connection details.
        self.driver = "ODBC Driver 17 for SQL Server"
        server = server or self.credentials["server"]
        db = db or self.credentials["db_name"]
        user = user or self.credentials["user"]
        pw = pw or self.credentials["password"]

        self.conn_str = f"DRIVER={{{self.driver}}};SERVER={{{server}}};DATABASE={{{db}}};UID={{{user}}};PWD={{{pw}}}"
        self._con = None
        self.DEFAULT_SCHEMA = "sandbox"

    @property
    def con(self):
        """A singleton for connection"""
        if not self._con:
            self._con = pyodbc.connect(self.conn_str)
        return self._con

    def run(self, query: str):
        cursor = self.con.cursor()
        cursor.execute(query)
        if query.upper().startswith("SELECT"):
            return cursor.fetchall()
        self.con.commit()

    @property
    def schemas(self) -> List[str]:
        schemas_tuples = self.run("SELECT s.name as schema_name from sys.schemas s")
        return [schema_tuple[0] for schema_tuple in schemas_tuples]

    @property
    def tables(self) -> List[str]:
        tables_tuples = self.run("SELECT * FROM information_schema.tables")
        return [table for row in tables_tuples for table in row]

    def create_table(
        self,
        table: str,
        schema: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal = ["fail", "replace"],
    ):
        if schema is None:
            schema = self.DEFAULT_SCHEMA
        fqn = f"{schema}.{table}"
        indent = "  "
        dtypes_rows = [indent + col + " " + dtype for col, dtype in dtypes.items()]
        dtypes_formatted = ",\n".join(dtypes_rows)
        create_table_sql = f"CREATE TABLE {fqn}(\n{dtypes_formatted}\n)"
        if if_exists == "replace":
            try:
                self.run(f"DROP TABLE {schema}.{table}")
            except:
                pass
        self.run(create_table_sql)
        return True

    def bulk_insert(
        self,
        table: str,
        schema: str = None,
        source_path: str = None,
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
                FIELDTERMINATOR=',',
                ROWTERMINATOR='0x0a',
                FIRSTROW=2,
                KEEPIDENTITY,
                TABLOCK
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
