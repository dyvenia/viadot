"""Base classes for data sources."""

from abc import abstractmethod
from collections.abc import Iterable, Iterator
import inspect
from itertools import batched, chain
import logging
from pathlib import Path
from typing import Any, Literal, overload

import pandas as pd
import pyarrow as pa
import pyodbc

from viadot.config import get_max_batch_size, get_source_credentials
from viadot.signals import SKIP
from viadot.utils import (
    add_viadot_metadata_columns,
    add_viadot_metadata_columns_arrow,
    validate,
)


logger = logging.getLogger(__name__)

Record = tuple[Any]
RecordBatch = Iterable[Any]


class _DefaultBatchSize:
    """Marker for an omitted public batch-size argument."""


_DEFAULT_BATCH_SIZE = _DefaultBatchSize()


class Source:
    # Connectors that can natively return data in the requested batch size within
    # `_to_records()` should override this with True. The default keeps the generic
    # re-batching fallback for sources where batch size cannot be controlled, so that
    # `to_records()` can still yield batches of the requested size (even if
    # `_to_records()` cannot).
    supports_batch_size = False

    def __init__(self, *args, credentials: dict[str, Any] | None = None, **kwargs):  # noqa: ARG002
        """Base class for data sources.

        Args:
            credentials (dict[str, Any] | None, optional): The credentials for the
                source. Defaults to None.
        """
        self.credentials = credentials
        self.data: pa.Table = None
        self.logger = logger

    @abstractmethod
    def to_json(self) -> dict:
        """Download data from source to a dictionary."""

    @overload
    def to_df(
        self,
        batch_size: None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame: ...

    @overload
    def to_df(
        self,
        batch_size: int,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> Iterator[pd.DataFrame]: ...

    def to_df(
        self,
        batch_size: int | _DefaultBatchSize | None = _DEFAULT_BATCH_SIZE,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame | Iterator[pd.DataFrame]:
        """Convert the result of ``to_arrow()`` to pandas.

        Args:
            batch_size (int | None, optional): Maximum number of records per
                output batch. If None, return one DataFrame containing all records.
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if
                there is no data. Defaults to "warn".

        Returns:
            pd.DataFrame | Iterator[pd.DataFrame]: A DataFrame or a lazy iterator
                of DataFrames, depending on ``batch_size``.
        """
        result = self.to_arrow(batch_size=batch_size, if_empty=if_empty)
        if isinstance(result, pa.Table):
            return result.to_pandas()

        return (
            pa.Table.from_batches([record_batch]).to_pandas() for record_batch in result
        )

    @abstractmethod
    def query(self) -> list[Record] | bool:
        """Run a query and possibly return the results."""

    @overload
    def to_arrow(
        self,
        batch_size: None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pa.Table: ...

    @overload
    def to_arrow(
        self,
        batch_size: _DefaultBatchSize = _DEFAULT_BATCH_SIZE,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> Iterator[pa.RecordBatch]: ...

    @overload
    def to_arrow(
        self,
        batch_size: int,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> Iterator[pa.RecordBatch]: ...

    @add_viadot_metadata_columns_arrow
    def to_arrow(
        self,
        batch_size: int | _DefaultBatchSize | None = _DEFAULT_BATCH_SIZE,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pa.Table | Iterator[pa.RecordBatch]:
        """Create an Arrow table or lazily yield Arrow record batches.

        Args:
            batch_size (int | None, optional): Maximum number of records per
                output batch. If None, return one table containing all records.
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
                source contains no data. Defaults to "warn".
        """
        records = self.to_records(batch_size=batch_size)
        if not isinstance(records, Iterator):
            table = pa.Table.from_pylist(records)
            if table.num_rows == 0:
                self._handle_if_empty(if_empty=if_empty)
            return table

        # We need to wrap the yield in order to be able to return arrow.Table directly
        # above (otherwise, the return would just call StopIteration due to how
        # generators work).
        def record_batches() -> Iterator[pa.RecordBatch]:
            has_records = False
            for record_batch in records:
                has_records = True
                yield pa.RecordBatch.from_pylist(list(record_batch))

            if not has_records:
                self._handle_if_empty(if_empty=if_empty)

        return record_batches()

    @overload
    def to_records(self, batch_size: None) -> RecordBatch: ...

    @overload
    def to_records(
        self, batch_size: int | _DefaultBatchSize = _DEFAULT_BATCH_SIZE
    ) -> Iterator[RecordBatch]: ...

    def to_records(
        self,
        batch_size: int | _DefaultBatchSize | None = _DEFAULT_BATCH_SIZE,
    ) -> RecordBatch | Iterator[RecordBatch]:
        """Return source records, lazily batched unless batching is disabled.

        Concrete sources provide the source-specific ``_to_records()`` method,
        which yields source-native batches. This method owns the generic output
        batching and logging behavior shared by all sources.

        Args:
            batch_size (int | None, optional): Maximum number of records per
                output batch. If None, expose all records as one logical batch.

        Returns:
            RecordBatch | Iterator[RecordBatch]: All records as one materialized
                batch when ``batch_size`` is None, otherwise a lazy iterator of
                batches containing at most ``batch_size`` records.
        """
        batch_size = self._resolve_batch_size(batch_size)
        source_batches = self._to_records(batch_size=batch_size) or ()

        if batch_size is None:
            self.logger.info("Processing batch 1...")
            records = list(chain.from_iterable(source_batches))
            self.logger.info("All batches processed successfully.")
            return records

        def record_batches() -> Iterator[RecordBatch]:
            if self.supports_batch_size:
                # The connector natively supports precise batch sizes - no need to
                # re-batch.
                for batch_number, source_batch in enumerate(source_batches, start=1):
                    self.logger.info(f"Processing batch {batch_number}...")
                    yield source_batch
            else:
                # Enforce `batch_size` by re-batching source-native batches. This
                # allows the connector to control batch size via parameters instead
                # of hardcoding it in the source implementation, but will incur
                # additional processing and memory copies.
                for batch_number, batch in enumerate(
                    batched(chain.from_iterable(source_batches), n=batch_size),
                    start=1,
                ):
                    self.logger.info(f"Processing batch {batch_number}...")
                    yield batch

            self.logger.info("All batches processed successfully.")

        return record_batches()

    def _resolve_batch_size(
        self, batch_size: int | _DefaultBatchSize | None
    ) -> int | None:
        """Resolve omitted batch size according to connector capabilities."""
        project_batch_size = get_max_batch_size()
        if batch_size is _DEFAULT_BATCH_SIZE:
            parameter = inspect.signature(self._to_records).parameters.get("batch_size")
            connector_batch_size = (
                parameter.default
                if parameter
                and type(parameter.default) is int
                and parameter.default > 0
                else None
            )
            batch_size = connector_batch_size or project_batch_size
        elif not isinstance(batch_size, int):
            return None

        return min(batch_size, project_batch_size)

    @abstractmethod
    def _to_records(self, batch_size: int | None = None) -> Iterator[RecordBatch]:
        """Fetch and yield source-native batches of records.

        Implementations should use ``batch_size`` when the underlying source
        supports configurable fetch sizes and set ``supports_batch_size = True``
        on the connector so the base class can avoid re-batching.
        """

    def to_csv(
        self,
        path: str,
        if_exists: Literal["append", "replace"] = "replace",
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        sep: str = "\t",
        **kwargs,
    ) -> bool:
        r"""Write from source to a CSV file.

        Note that the source can be a particular file or table,
        but also a database in general. Therefore, some sources may require
        additional parameters to pull the right resource. Hence this method
        passes kwargs to the `to_df()` method implemented by the concrete source.

        Args:
            path (str): The destination path.
            if_exists (Literal[, optional): What to do if the file exists. Defaults to
                "replace".
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
                source contains no data. Defaults to "warn".
            sep (str, optional): The separator to use in the CSV. Defaults to "\t".

        Raises:
            ValueError: If the `if_exists` argument is incorrect.

        Returns:
            bool: Whether the operation was successful.
        """
        try:
            kwargs.pop("batch_size", None)
            df = self.to_df(batch_size=None, if_empty=if_empty, **kwargs)
        except SKIP:
            return False

        if if_exists == "append":
            mode = "a"
        elif if_exists == "replace":
            mode = "w"
        else:
            msg = "'if_exists' must be one of ['append', 'replace']"
            raise ValueError(msg)

        df.to_csv(path, sep=sep, mode=mode, index=False, header=not Path(path).exists())

        return True

    def to_excel(
        self,
        path: str,
        if_exists: str = "replace",
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> bool:
        """Write from source to a excel file.

        Args:
            path (str): The destination path.
            if_exists (str, optional): What to do if the file exists. Defaults to
                "replace".
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
                source contains no data.

        """
        try:
            df = self.to_df(batch_size=None, if_empty=if_empty)
        except SKIP:
            return False

        if if_exists == "append":
            if Path(path).is_file():
                excel_df = pd.read_excel(path)
                out_df = pd.concat([excel_df, df])
            else:
                out_df = df
        elif if_exists == "replace":
            out_df = df
        else:
            msg = "'if_exists' must be one of ['append', 'replace']"
            raise ValueError(msg)
        out_df.to_excel(path, index=False)
        return True

    def _handle_if_empty(
        self,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        message: str = "The query produced no data.",
    ) -> None:
        """What to do if a fetch (database query, API request) produced no data."""
        if if_empty == "warn":
            self.logger.warning(message)
        elif if_empty == "skip":
            raise SKIP(message)
        elif if_empty == "fail":
            raise ValueError(message)


class SQL(Source):
    def __init__(
        self,
        driver: str | None = None,
        config_key: str | None = None,
        credentials: str | None = None,
        query_timeout: int = 60 * 60,
        *args,
        **kwargs,
    ):
        """A base SQL source class.

        Args:
            driver (str, optional): The SQL driver to use. Defaults to None.
            config_key (str, optional): The key inside local config containing the
                config. User can choose to use this or pass credentials directly to the
                `credentials` parameter. Defaults to None.
            credentials (str, optional): Credentials for the connection. Defaults to
                None.
            query_timeout (int, optional): The timeout for executed queries. Defaults to
                1 hour.
        """
        self.query_timeout = query_timeout

        config_credentials = get_source_credentials(config_key) if config_key else None

        credentials = credentials or config_credentials or {}

        if driver:
            credentials["driver"] = driver

        super().__init__(*args, credentials=credentials, **kwargs)

        self._con = None

    @property
    def conn_str(self) -> str:
        """Generate a connection string from params or config.

        Note that the user and password are escaped with '{}' characters.

        Returns:
            str: The ODBC connection string.
        """
        driver = self.credentials["driver"]
        server = self.credentials["server"]
        db_name = self.credentials["db_name"]
        uid = self.credentials.get("user") or ""
        pwd = self.credentials.get("password") or ""

        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db_name};UID={uid};PWD={pwd};"

        if "authentication" in self.credentials:
            conn_str += "Authentication=" + self.credentials["authentication"] + ";"

        if "trust_server_certificate" in self.credentials:
            conn_str += (
                "TrustServerCertificate="
                + self.credentials["trust_server_certificate"]
                + ";"
            )
        if "encrypt" in self.credentials:
            conn_str += "Encrypt=" + self.credentials["encrypt"] + ";"
        return conn_str

    @property
    def con(self) -> pyodbc.Connection:
        """A singleton-like property for initiating a connection to the database.

        Returns:
            pyodbc.Connection: database connection.
        """
        if not self._con:
            self._con = pyodbc.connect(self.conn_str, timeout=5)
            self._con.timeout = self.query_timeout
        return self._con

    def run(self, query: str) -> list[Record] | bool:
        """Execute a query and return the result.

        Args:
            query (str): The query to execute.

        Returns:
            list[Record] | bool: If the query is a SELECT, return the result as a list
                of records.
        """
        cursor = self.con.cursor()
        cursor.execute(query)

        query_sanitized = query.strip().upper()
        if query_sanitized.startswith("SELECT") or query_sanitized.startswith("WITH"):
            result = cursor.fetchall()
        else:
            result = True

        self.con.commit()
        cursor.close()

        return result

    @add_viadot_metadata_columns
    def to_df(
        self,
        query: str,
        con: pyodbc.Connection | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        tests: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a query and return the result as a pandas DataFrame.

        Args:
            query (str): The query to execute.
            con (pyodbc.Connection, optional): The connection to use to pull the data.
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
                query returns no data. Defaults to None.
            tests (dict[str, Any], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from utils. Defaults to None.
                Available tests:
                    - `column_size`: dict{column: size}
                    - `column_unique_values`: list[columns]
                    - `column_list_to_match`: list[columns]
                    - `dataset_row_count`: dict: {'min': number, 'max', number}
                    - `column_match_regex`: dict: {column: 'regex'}
                    - `column_sum`: dict: {column: {'min': number, 'max': number}}
                Defaults to: None

        Returns:
            pd.DataFrame: The resulting data as a pandas DataFrame.
        """
        conn = con or self.con

        query_sanitized = query.strip().upper()
        if query_sanitized.startswith("SELECT") or query_sanitized.startswith("WITH"):
            df = pd.read_sql_query(query, conn)
            if df.empty:
                self._handle_if_empty(if_empty=if_empty)
        else:
            df = pd.DataFrame()

        if tests:
            validate(df=df, tests=tests)

        return df

    def _check_if_table_exists(self, table: str, schema: str | None = None) -> bool:
        """Check if table exists in a specified schema.

        Args:
            table (str): Table name.
            schema (str, optional): Schema name. Defaults to None.
        """
        exists_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME='{table}'"  # noqa: S608
        return bool(self.run(exists_query))

    def create_table(
        self,
        table: str,
        schema: str | None = None,
        dtypes: dict[str, Any] | None = None,
        if_exists: Literal["fail", "replace", "skip", "delete"] = "fail",
    ) -> bool:
        """Create a table.

        Args:
            table (str): The destination table. Defaults to None.
            schema (str, optional): The destination schema. Defaults to None.
            dtypes (Dict[str, Any], optional): The data types to use for the table.
                Defaults to None.
            if_exists (Literal, optional): What to do if the table already exists.
                Defaults to "fail".

        Returns:
            bool: Whether the operation was successful.
        """
        fqn = f"{schema}.{table}" if schema is not None else table
        exists = self._check_if_table_exists(schema=schema, table=table)

        if exists:
            if if_exists == "replace":
                self.run(f"DROP TABLE {fqn}")
            elif if_exists == "delete":
                self.run(f"DELETE FROM {fqn}")  # noqa: S608
                return True
            elif if_exists == "fail":
                msg = (
                    f"The table {fqn} already exists and 'if_exists' is set to 'fail'."
                )
                raise ValueError(msg)
            elif if_exists == "skip":
                return False

        indent = "  "
        dtypes_rows = [
            indent + f'"{col}"' + " " + dtype for col, dtype in dtypes.items()
        ]
        dtypes_formatted = ",\n".join(dtypes_rows)
        create_table_sql = f"CREATE TABLE {fqn}(\n{dtypes_formatted}\n)"
        self.run(create_table_sql)
        return True

    def insert_into(self, table: str, df: pd.DataFrame) -> str:
        """Insert values from a pandas DataFrame into an existing database table.

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
            counter += 1  # noqa: SIM113
            out_row = ", ".join(map(self._escape_column_name, row))
            comma = ",\n"
            if counter == rows_count:
                comma = ";"
            out_row = f"({out_row}){comma}"
            values += out_row

        columns = ", ".join(df.columns)

        sql = f"INSERT INTO {table} ({columns})\n VALUES {values}"  # noqa: S608
        self.run(sql)

        return sql

    def _escape_column_name(self, column_name: str) -> str:
        """Return an escaped column name."""
        return f"'{column_name}'"
