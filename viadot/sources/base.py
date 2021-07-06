import os
from abc import abstractmethod
from typing import Any, Dict, Literal

import pandas as pd
import pyarrow as pa
import pyodbc
from prefect.utilities import logging

from ..config import local_config
from ..signals import SKIP

logger = logging.get_logger(__name__)


class Source:
    def __init__(self, *args, credentials: Dict[str, Any] = None, **kwargs):
        self.credentials = credentials
        self.data: pa.Table = None

    @abstractmethod
    def to_json(self):
        pass

    @abstractmethod
    def to_df(self, if_empty: str = None):
        pass

    @abstractmethod
    def query():
        pass

    def to_arrow(self, if_empty: str = "warn") -> pa.Table:

        try:
            df = self.to_df(if_empty=if_empty)
        except SKIP:
            return False

        table = pa.Table.from_pandas(df)
        return table

    def to_csv(
        self, path: str, if_exists: str = "replace", if_empty: str = "warn", sep="\t"
    ) -> bool:

        try:
            df = self.to_df(if_empty=if_empty)
        except SKIP:
            return False

        if if_exists == "append":
            if os.path.isfile(path):
                csv_df = pd.read_csv(path, sep=sep)
                out_df = pd.concat([csv_df, df])
            else:
                out_df = df
        elif if_exists == "replace":
            out_df = df
        out_df.to_csv(path, sep=sep, index=False)
        return True

    def to_excel(
        self, path: str, if_exists: str = "replace", if_empty: str = "warn"
    ) -> bool:

        try:
            df = self.to_df(if_empty=if_empty)
        except SKIP:
            return False

        if if_exists == "append":
            if os.path.isfile(path):
                excel_df = pd.read_excel(path)
                out_df = pd.concat([excel_df, df])
            else:
                out_df = df
        elif if_exists == "replace":
            out_df = df
        out_df.to_excel(path, index=False, encoding="utf8")
        return True


    def _handle_if_empty(self, if_empty: str = None):
        if if_empty == "warn":
            logger.warning("The query produced no data.")
        elif if_empty == "skip":
            raise SKIP("The query produced no data. Skipping...")
        elif if_empty == "fail":
            raise ValueError("The query produced no data.")


class SQL(Source):
    def __init__(
        self,
        driver: str = None,
        config_key: str = None,
        credentials: str = None,
        *args,
        **kwargs,
    ):
        if config_key:
            config_credentials = local_config.get(config_key)

        credentials = config_credentials if config_key else credentials

        if driver:
            credentials["driver"] = driver

        super().__init__(*args, credentials=credentials, **kwargs)

        self._con = None

    @property
    def conn_str(self):
        """Generate a connection string from params or config.
        Note that the user and password are escapedd with '{}' characters.

        Returns:
            str: The ODBC connection string.
        """
        driver = self.credentials["driver"]
        server = self.credentials["server"]
        db_name = self.credentials["db_name"]
        uid = self.credentials["user"]
        pwd = self.credentials["password"]

        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db_name};UID={uid};PWD={pwd};"

        if "authentication" in self.credentials:
            conn_str += "Authentication=" + self.credentials["authentication"] + ";"

        return conn_str

    @property
    def con(self) -> pyodbc.Connection:
        """A singleton-like property for initiating a connection to the database.

        Returns:
            pyodbc.Connection: database connection.
        """
        if not self._con:
            self._con = pyodbc.connect(self.conn_str)
        return self._con

    def run(self, query: str):
        cursor = self.con.cursor()
        cursor.execute(query)
        if query.upper().startswith("SELECT"):
            return cursor.fetchall()
        self.con.commit()

    def to_df(self, query: str):
        conn = self.con
        if query.upper().startswith("SELECT"):
            return pandas.read_sql_query(query, conn)
        else:
            return pandas.DataFrame()

    def create_table(
        self,
        table: str,
        schema: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace"] = "fail",
    ) -> bool:
        """Create a Table in the Database

        Args:
            table (str): The destination table. Defaults to None.
            schema (str, optional): The destination schema. Defaults to None.
            dtypes (Dict[str, Any], optional): [description]. Defaults to None.
            if_exists (Literal, optional): [description]. Defaults to "fail".

        Returns:
            bool: Whether the operation was successful.
        """
        if schema is None:
            fqn = f"{table}"
        else:
            fqn = f"{schema}.{table}"
        indent = "  "
        dtypes_rows = [
            indent + f'"{col}"' + " " + dtype for col, dtype in dtypes.items()
        ]

        dtypes_formatted = ",\n".join(dtypes_rows)
        create_table_sql = f"CREATE TABLE {fqn}(\n{dtypes_formatted}\n)"
        if if_exists == "replace":
            try:
                if schema == None:
                    self.run(f"DROP TABLE {table}")
                else:
                    self.run(f"DROP TABLE {schema}.{table}")
            except:
                pass
        self.run(create_table_sql)
        return True

    def insert_into(self, table: str, df: pd.DataFrame) -> str:
        """Inserts values from a pandas dataframe into an existing
        database table

        Args:
            table (str): table name
            df (pd.DataFrame): pandas dataframe

        Returns:
            str: The executed SQL insert query.
        """

        values = ""
        rows_count = df.shape[0]
        counter = 0
        for row in df.values:
            counter += 1
            out_row = ", ".join(map(self._sql_column, row))
            comma = ",\n"
            if counter == rows_count:
                comma = ";"
            out_row = f"({out_row}){comma}"
            values += out_row

        columns = ", ".join(df.columns)
        sql = f"INSERT INTO {table} ({columns})\n VALUES {values}"

        return self.run(sql)

    def _sql_column(self, column_name: str) -> str:
        if isinstance(column_name, str):
            out_name = f"'{column_name}'"
        else:
            out_name = str(column_name)
        return out_name
