import warnings
from typing import Literal, Optional, Union

import pandas as pd
import pyarrow as pa
import urllib3
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SADeprecationWarning
from trino.auth import BasicAuthentication

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import get_fqn

# TODO: remove this and verify=False in Trino.con()
urllib3.disable_warnings()

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

        self._con = None

        self.http_scheme = self.credentials.get("http_scheme")
        self.host = self.credentials.get("host")
        self.port = self.credentials.get("port")
        self.username = self.credentials.get("user")
        self.password = self.credentials.get("password")
        self.catalog = self.credentials.get("catalog")
        self.schema = self.credentials.get("schema")
        self.verify = self.credentials.get("verify")

    @property
    def con(self):
        if self._con is None:
            connection_string = (
                f"trino://{self.username}@{self.host}:{self.port}/{self.catalog}"
            )
            connect_args = {
                "verify": self.verify,
                "auth": BasicAuthentication(self.username, self.password),
                "http_scheme": self.http_scheme,
            }
            engine = create_engine(connection_string, connect_args=connect_args)
            return engine.connect()
        return self._con

    def get_tables(self, schema_name: str) -> list[str]:
        query = f"SHOW TABLES FROM {schema_name}"
        return self.run(query)

    def drop_table(self, table_name: str, schema_name: str = None) -> None:
        fqn = get_fqn(schema_name=schema_name, table_name=table_name)
        query = f"DROP TABLE {fqn}"

        self.logger.info(f"Dropping table {fqn}...")
        self.run(query)
        self.logger.info(f"Table {fqn} has been successfully dropped.")

    def _check_if_table_exists(self, table_name: str, schema_name: str) -> None:
        query = f"""
SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '{schema_name}'
AND TABLE_NAME = '{table_name}'"""
        results = self.run(query)
        return len(results) > 0

    def get_schemas(self) -> list[str]:
        query = f"SHOW SCHEMAS"
        return self.run(query)

    def _check_if_schema_exists(self, schema_name: str) -> None:
        query = f"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema_name}'"
        results = self.run(query)
        return bool(results)

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        if not self._check_if_schema_exists(schema_name):
            return

        if cascade:
            tables = self.get_tables(schema_name)
            [self.drop_table(schema_name=schema_name, table_name=t) for t in tables]

        self.logger.info(f"Dropping schema {schema_name}...")
        self.run(f"DROP SCHEMA {schema_name}")
        self.logger.info(f"Schema {schema_name} has been successfully dropped.")

    def create_iceberg_schema(self, schema_name: str, location: str) -> None:
        query = f"""
CREATE SCHEMA {schema_name}
WITH (location = '{location}')
        """
        self.logger.info(f"Creating schema {schema_name}...")
        self.run(query)
        self.logger.info(f"Schema {schema_name} has been successfully created.")

    def create_iceberg_table(
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
        columns = pa_table.schema.names
        types = [self.pyarrow_to_trino_type(str(typ)) for typ in pa_table.schema.types]
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

        self.logger.info(f"Creating table {fqn}...")
        self.run(create_table_query)
        self.logger.info(f"Table {fqn} has been successfully created.")

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
CREATE TABLE {fqn} (
    {cols_and_dtypes}
)
WITH (
    {with_clause}
)"""

        return query

    def run(self, query: str) -> Union[list[str], None]:
        
        self.logger.debug("Executing query:\n" + query)

        try:
            result = self.con.execute(text(query))
            self.con.commit()
            query_keywords = ["SELECT", "SHOW", "PRAGMA", "WITH"]
            if any(query.strip().upper().startswith(word) for word in query_keywords):
                results = result.fetchall()
                return [r[0] for r in results] if results else []
        except Exception as e:
            raise ValueError(f"Failed executing query:\n{query}") from e
        finally:
            self.con.close()

    @staticmethod
    def pyarrow_to_trino_type(pyarrow_type: str) -> str:
        # TODO: add remaining types
        mapping = {
            "string": "VARCHAR",
            "int64": "BIGINT",
            "double": "DOUBLE",
            "bool": "BOOLEAN",
            "timestamp[ns]": "TIMESTAMP(6)",
        }
        return mapping[pyarrow_type]

    def _check_connection(self):
        try:
            self.run("select 1")
        except Exception as e:
            raise ValueError(f"Failed to connect to Trino server at {self.host}") from e
