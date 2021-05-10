import os
from abc import abstractmethod
from typing import Any, Dict, Literal

import pandas
import pyarrow as pa
import pyodbc
from prefect.utilities import logging

from ..config import local_config

logger = logging.get_logger(__name__)


class Source:
    def __init__(self, *args, credentials: Dict[str, Any] = None, **kwargs):
        self.credentials = credentials
        self.data: pa.Table = None

    @abstractmethod
    def to_json(self):
        pass

    @abstractmethod
    def to_df():
        pass

    @abstractmethod
    def query():
        pass

    def to_arrow(self):
        df = self.to_df()
        table = pa.Table.from_pandas(df)
        return table

    def to_csv(self, path: str, if_exists="replace"):
        df = self.to_df()
        if if_exists == "append":
            if os.path.isfile(path):
                csv_df = pandas.read_csv(path)
                out_df = pandas.concat([csv_df, df])
            else:
                out_df = df
        elif if_exists == "replace":
            out_df = df
        out_df.to_csv(path, index=False)
        return True


class SQL(Source):
    def __init__(
        self,
        server: str = None,
        db: str = None,
        user: str = None,
        pw: str = None,
        driver: str = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        if config_key:
            DEFAULT_CREDENTIALS = local_config.get(config_key)
            credentials = kwargs.pop("credentials", DEFAULT_CREDENTIALS)
            credentials["driver"] = "ODBC Driver 17 for SQL Server"
        else:
            credentials = {
                "driver": driver,
                "server": server,
                "db_name": db,
                "user": user,
                "password": pw,
            }
        super().__init__(*args, credentials=credentials, **kwargs)

        self._con = None

    @property
    def conn_str(self):
        conn_str = ""
        if "driver" in self.credentials:
            conn_str += "DRIVER=" + self.credentials["driver"] + ";"
        if "server" in self.credentials:
            conn_str += "SERVER=" + self.credentials["server"] + ";"
        if "db_name" in self.credentials:
            conn_str += "DATABASE=" + self.credentials["db_name"] + ";"
        if "user" in self.credentials and self.credentials["user"] != None:
            conn_str += "UID=" + self.credentials["user"] + ";"
        if "password" in self.credentials and self.credentials["password"] != None:
            conn_str += "PWD=" + self.credentials["password"] + ";"
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

    def create_table(
        self,
        table: str,
        schema: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal = ["fail", "replace"],
    ):
        """Create a Table in the Database

        Args:
            table (str): Table name
            schema (str, optional): [description]. Defaults to None.
            dtypes (Dict[str, Any], optional): [description]. Defaults to None.
            if_exists (Literal, optional): [description]. Defaults to ["fail", "replace"].

        Returns:
            [type]: [description]
        """
        if schema is None:
            fqn = f"{table}"
        else:
            fqn = f"{schema}.{table}"
        indent = "  "
        dtypes_rows = [indent + col + " " + dtype for col, dtype in dtypes.items()]
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

    def insert_into(self, table: str, df: pandas.DataFrame):
        """Inserts values from a pandas dataframe into an existing
        database table

        Args:
            table (str): table name
            df (pandas.DataFrame): pandas dataframe

        Returns:
            [type]: [description]
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
        return sql

    def _sql_column(self, column_name):
        if isinstance(column_name, str):
            out_name = f"'{column_name}'"
        else:
            out_name = str(column_name)
        return out_name
