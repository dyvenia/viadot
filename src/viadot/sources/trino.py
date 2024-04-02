import re
import warnings
from contextlib import contextmanager
from typing import Generator, Literal, Optional

import pandas as pd
import pyarrow as pa
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SADeprecationWarning
from trino.auth import BasicAuthentication

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import get_fqn

# Triggered by trino lib
warnings.filterwarnings("ignore", category=SADeprecationWarning)


class TrinoCredentials(BaseModel):
    http_scheme: str = "https"
    host: str = "localhost"
    port: int = 443
    user: str
    password: Optional[str] = None
    catalog: str
    schema_name: Optional[str] = Field(None, alias="schema")
    verify: bool = True


class Trino(Source):
    """
    A class for interacting with Trino as a database. Currently supports only generic
    and Iceberg operations.

    Args:
        credentials (TrinoCredentials): Trino credentials.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
    """

    def __init__(
        self,
        credentials: TrinoCredentials = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = TrinoCredentials(**raw_creds).dict(
            by_alias=True
        )  # validate the credentials

        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.http_scheme = self.credentials.get("http_scheme")
        self.host = self.credentials.get("host")
        self.port = self.credentials.get("port")
        self.username = self.credentials.get("user")
        self.password = self.credentials.get("password")
        self.catalog = self.credentials.get("catalog")
        self.schema = self.credentials.get("schema")
        self.verify = self.credentials.get("verify")

        self.connection_string = (
            f"trino://{self.username}@{self.host}:{self.port}/{self.catalog}"
        )
        self.connect_args = {
            "verify": self.verify,
            "auth": BasicAuthentication(self.username, self.password),
            "http_scheme": self.http_scheme,
        }
        self.engine = create_engine(
            self.connection_string, connect_args=self.connect_args, future=True
        )

    @contextmanager
    def get_connection(self):
        """Provide a transactional scope around a series of operations.

        ----
        Examples:

        >>> trino = Trino()
        >>> with trino.get_connection() as connection:
        >>>    trino.run(query1, connection=connection)
        >>>    trino.run(query2, connection=connection)
        """
        connection = self.engine.connect()
        try:
            yield connection
            connection.commit()
        except:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get_tables(self, schema_name: str) -> list[str]:
        query = f"SHOW TABLES FROM {schema_name}"
        with self.get_connection() as connection:
            return list(self.run(query, connection=connection))

    def drop_table(self, table_name: str, schema_name: str = None) -> None:
        fqn = get_fqn(schema_name=schema_name, table_name=table_name)
        query = f"DROP TABLE IF EXISTS {fqn}"

        self.logger.info(f"Dropping table '{fqn}'...")
        with self.get_connection() as connection:
            self.run(query, connection=connection)
        self.logger.info(f"Table '{fqn}' has been successfully dropped.")

    def delete_table(self, table_name: str, schema_name: str = None) -> None:
        fqn = get_fqn(schema_name=schema_name, table_name=table_name)
        query = f"DELETE FROM {fqn}"
        self.logger.info(f"Removing all data from table '{fqn}'...")
        with self.get_connection() as connection:
            self.run(query, connection=connection)
        self.logger.info(f"Data from table '{fqn}' has been successfully removed.")

    def _check_if_table_exists(self, table_name: str, schema_name: str) -> None:
        query = f"""
SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '{schema_name}'
AND TABLE_NAME = '{table_name}'"""
        with self.get_connection() as connection:
            results = list(self.run(query, connection=connection))
        return len(results) > 0

    def get_schemas(self) -> list[str]:
        query = "SHOW SCHEMAS"
        with self.get_connection() as connection:
            return list(self.run(query, connection=connection))

    def _check_if_schema_exists(self, schema_name: str) -> None:
        query = f"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema_name}'"
        with self.get_connection() as connection:
            results = list(self.run(query, connection=connection))
        return bool(results)

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        if not self._check_if_schema_exists(schema_name):
            return

        if cascade:
            tables = self.get_tables(schema_name)
            [self.drop_table(schema_name=schema_name, table_name=t) for t in tables]

        self.logger.info(f"Dropping schema {schema_name}...")
        with self.get_connection() as connection:
            self.run(f"DROP SCHEMA {schema_name}", connection=connection)
        self.logger.info(f"Schema {schema_name} has been successfully dropped.")

    def create_iceberg_schema(
        self,
        schema_name: str,
        location: str,
        if_exists: Literal["fail", "skip"] = "fail",
    ) -> None:
        exists = self._check_if_schema_exists(schema_name)

        if exists and if_exists == "fail":
            raise ValueError(f"Schema '{schema_name}' already exists.")
        else:
            self.logger.info(f"Schema '{schema_name}' already exists. Skipping...")
            return

        query = f"""
CREATE SCHEMA {schema_name}
WITH (location = '{location}')
        """
        self.logger.info(f"Creating schema '{schema_name}'...")
        with self.get_connection() as connection:
            self.run(query, connection=connection)
        self.logger.info(f"Schema '{schema_name}' has been successfully created.")

    def create_iceberg_table_from_arrow(
        self,
        table: pa.Table,
        table_name: str,
        schema_name: str | None = None,
        location: str | None = None,
        format: Literal["PARQUET", "ORC"] = "PARQUET",
        partition_cols: list[str] | None = None,
        sort_by: list[str] | None = None,
    ) -> None:
        columns = table.schema.names
        types = [self.pyarrow_to_trino_type(str(typ)) for typ in table.schema.types]
        create_table_query = self._create_table_query(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            types=types,
            location=location,
            format=format,
            partition_cols=partition_cols,
            sort_by_cols=sort_by,
        )

        fqn = get_fqn(schema_name=schema_name, table_name=table_name)
        self.logger.info(f"Creating table '{fqn}'...")
        with self.get_connection() as connection:
            self.run(create_table_query, connection=connection)
        self.logger.info(f"Table '{fqn}' has been successfully created.")

    def create_iceberg_table_from_pandas(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema_name: str = None,
        location: str = None,
        format: Literal["PARQUET", "ORC"] = "PARQUET",
        partition_cols: list[str] = None,
        sort_by: list[str] = None,
    ) -> None:
        pa_table = pa.Table.from_pandas(df)
        self.create_iceberg_table_from_arrow(
            table=pa_table,
            schema_name=schema_name,
            table_name=table_name,
            location=location,
            format=format,
            partition_cols=partition_cols,
            sort_by=sort_by,
        )

    def _create_table_query(
        self,
        table_name: str,
        columns: list[str],
        types: list[str],
        schema_name: str = None,
        location: str = None,
        format: Literal["PARQUET", "ORC"] = "PARQUET",
        partition_cols: list[str] = None,
        sort_by_cols: list[str] = None,
    ):
        cols_and_dtypes = ",\n\t".join(
            col + " " + dtype for col, dtype in zip(columns, types)
        )
        fqn = get_fqn(schema_name=schema_name, table_name=table_name)
        with_clause = f"format = '{format}'"

        if partition_cols:
            with_clause += ",\n\tpartitioning = ARRAY" + str(partition_cols)

        if sort_by_cols:
            with_clause += ",\n\tsorted_by = ARRAY" + str(sort_by_cols)

        if location:
            with_clause += f",\n\tlocation = '{location}'"

        query = f"""
CREATE TABLE IF NOT EXISTS {fqn} (
    {cols_and_dtypes}
)
WITH (
    {with_clause}
)"""

        return query

    def run(
        self, sql: str, connection: Connection
    ) -> Generator[tuple, None, None] | None:
        def row_generator(result):
            # Fetch rows in chunks of size `yield_per`.
            # This has to be inside a function due to how Python generators work.
            for partition in result.partitions():
                yield from partition

        self.logger.debug("Executing SQL:\n" + sql)

        try:
            # Execute with server-side cursor of size 5000.
            result = connection.execution_options(yield_per=5000).execute(text(sql))
        except Exception as e:
            raise ValueError(f"Failed executing SQL:\n{sql}") from e

        query_keywords = ["SELECT", "SHOW", "PRAGMA", "WITH"]
        is_query = any(sql.strip().upper().startswith(word) for word in query_keywords)

        if is_query:
            return row_generator(result)

    @staticmethod
    def pyarrow_to_trino_type(pyarrow_type: str) -> str:
        mapping = {
            "string": "VARCHAR",
            "large_string": "VARCHAR",
            "int8": "TINYINT",
            "int16": "SMALLINT",
            "int32": "INTEGER",
            "int64": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE",
            "bool": "BOOLEAN",
            "date32[day]": "DATE",
            "timestamp[ns]": "TIMESTAMP(6)",
            "decimal": "DECIMAL",
            "decimal128": "DECIMAL",
            "decimal256": "DECIMAL",
        }
        precision, scale = None, None
        decimal_match = re.match(r"(\w+)\((\d+), (\d+)\)", pyarrow_type)
        if decimal_match:
            pyarrow_type = decimal_match.group(1)
            precision = int(decimal_match.group(2))
            scale = int(decimal_match.group(3))

        mapped_type = mapping.get(pyarrow_type) or "VARCHAR"

        if precision and scale:
            mapped_type += f"({precision}, {scale})"

        return mapped_type

    def _check_connection(self):
        try:
            with self.get_connection() as connection:
                self.run("select 1", connection=connection)
        except Exception as e:
            raise ValueError(f"Failed to connect to Trino server at {self.host}") from e
