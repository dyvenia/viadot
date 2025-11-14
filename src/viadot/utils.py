"""Util functions."""

from collections.abc import Callable
import contextlib
from datetime import datetime, timedelta, timezone
import functools
import logging
import re
import subprocess
from typing import TYPE_CHECKING, Any, Literal, Optional

import pandas as pd
import pendulum
import pyodbc
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    ReadTimeout,
    Timeout,
)
from requests.packages.urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError

from viadot.exceptions import APIError, ValidationError
from viadot.orchestration.prefect.utils import DynamicDateHandler
from viadot.signals import SKIP


if TYPE_CHECKING:
    from viadot.sources.base import Source


with contextlib.suppress(ImportError):
    import pyspark.sql.dataframe as spark


def slugify(name: str) -> str:
    """Slugify a string."""
    return name.replace(" ", "_").lower()


def handle_api_request(
    url: str,
    auth: tuple | None = None,
    params: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    timeout: tuple = (3.05, 60 * 30),
    method: Literal["GET", "POST", "DELETE"] = "GET",
    data: str | None = None,
    verify: bool = True,
) -> requests.Response:
    """Send an HTTP request to the specified URL using the provided parameters.

    Args:
        url (str): The URL to send the request to.
        auth (tuple, optional): A tuple of (username, password) for basic
            authentication. Defaults to None.
        params (Dict[str, Any], optional): A dictionary of query string parameters.
            Defaults to None.
        headers (Dict[str, Any], optional): A dictionary of HTTP headers to include with
            the request. Defaults to None.
        timeout (tuple, optional): A tuple of (connect_timeout, read_timeout) in
            seconds. Defaults to (3.05, 60 * 30).
        method (Literal["GET", "POST", "DELETE"], optional): The HTTP method to use for
            the request. Defaults to "GET".
        data (str, optional): The request body data as a string. Defaults to None.
        verify (bool, optional): Whether to verify certificates. Defaults to True.

    Returns:
        requests.Response: The HTTP response object.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1,
        allowed_methods=["GET", "POST", "DELETE"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session.request(
        method=method,
        url=url,
        auth=auth,
        params=params,
        headers=headers,
        timeout=timeout,
        data=data,
        verify=verify,
    )


def _handle_response(
    response: requests.Response, timeout: tuple = (3.05, 60 * 30)
) -> requests.Response:
    url = response.url
    try:
        response.raise_for_status()
    except ReadTimeout as e:
        msg = "The connection was successful, "
        msg += f"however the API call to {url} timed out after {timeout[1]}s "
        msg += "while waiting for the server to return data."
        raise APIError(msg) from e
    except HTTPError as e:
        msg = f"The API call to {url} failed with status code {response.status_code}."
        msg += "\nPerhaps your account credentials need to be refreshed?"
        raise APIError(msg) from e
    except (ConnectionError, Timeout) as e:
        msg = f"The API call to {url} failed due to connection issues."
        raise APIError(msg) from e
    except ProtocolError as e:
        msg = f"Did not receive any response for the API call to {url}."
        raise APIError(msg) from e
    except Exception as e:
        msg = "Unknown error."
        raise APIError(msg) from e

    return response


def handle_api_response(
    url: str,
    auth: tuple | None = None,
    params: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    timeout: tuple = (3.05, 60 * 30),
    method: Literal["GET", "POST", "DELETE"] = "GET",
    data: str | None = None,
    verify: bool = True,
) -> requests.models.Response:
    """Handle an HTTP response.

    Apply retries and handle some common response codes.

    Args:
        url (str): The URL which trying to connect.
        auth (tuple, optional): A tuple of (username, password) for basic
            authentication. Defaults to None.
        params (Dict[str, Any], optional): The request parameters. Defaults to None.
        headers (Dict[str, Any], optional): The request headers. Defaults to None.
        method (Literal["GET", "POST", "DELETE"], optional): The HTTP method to use for
            the request. Defaults to "GET".
        timeout (tuple, optional): A tuple of (connect_timeout, read_timeout) in
            seconds. Defaults to (3.05, 60 * 30).
        data (str, optional): The request body data as a string. Defaults to None.
        verify (bool, optional): Whether to verify certificates. Defaults to True.

    Raises:
        - ConnectionError: If the connection timed out, as specified in `timeout`.
        - ReadTimeout: If the read timed out, as specified in `timeout`.
        - HTTPError: If the API raised an HTTPError.
        - APIError: Generic API error.

    Returns:
        requests.models.Response
    """
    response = handle_api_request(
        url=url,
        auth=auth,
        params=params,
        headers=headers,
        timeout=timeout,
        method=method,
        data=data,
        verify=verify,
    )

    return _handle_response(response)


def get_sql_server_table_dtypes(
    table: str, con: pyodbc.Connection, schema: str | None = None
) -> dict:
    """Get column names and types from a SQL Server database table.

    Args:
        table (str): The table for which to fetch dtypes.
        con (pyodbc.Connection): The connection to the database where the table
            is located.
        schema (str, optional): The schema where the table is located. Defaults
            to None.

    Returns:
        dict: A dictionary of the form {column_name: dtype, ...}.
    """
    query = f"""
    SELECT
        col.name,
        t.name,
        col.max_length
    FROM sys.tables AS tab
        INNER JOIN sys.columns AS col
            ON tab.object_id = col.object_id
        LEFT JOIN sys.types AS t
            ON col.user_type_id = t.user_type_id
    WHERE tab.name = '{table}'
    AND schema_name(tab.schema_id) = '{schema}'
    ORDER BY column_id;
    """
    cursor = con.cursor()
    query_result = cursor.execute(query).fetchall()
    cursor.close()

    dtypes = {}
    for row in query_result:
        column_name = row[0]
        dtype = row[1]
        length = row[2]
        if dtype == "varchar":
            dtypes[column_name] = dtype + f"({length})"
        else:
            dtypes[column_name] = dtype

    return dtypes


def _cast_df_cols(
    df: pd.DataFrame,
    types_to_convert: list[Literal["datetime", "bool", "int", "object"]] | None = None,
) -> pd.DataFrame:
    """Cast the data types of columns in a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        types_to_convert (Literal[datetime, bool, int, object], optional): List of types
            to be converted. Defaults to ["datetime", "bool", "int"].

    Returns:
        pd.DataFrame: A DataFrame with modified data types.
    """
    if not types_to_convert:
        types_to_convert = ["datetime", "bool", "int"]

    df = df.replace({"False": False, "True": True})

    datetime_cols = (col for col, dtype in df.dtypes.items() if dtype.kind == "M")
    bool_cols = (col for col, dtype in df.dtypes.items() if dtype.kind == "b")
    int_cols = (col for col, dtype in df.dtypes.items() if dtype.kind == "i")
    object_cols = (col for col, dtype in df.dtypes.items() if dtype.kind == "O")

    if "datetime" in types_to_convert:
        for col in datetime_cols:
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
    if "bool" in types_to_convert:
        for col in bool_cols:
            df[col] = df[col].astype(pd.Int64Dtype())
    if "int" in types_to_convert:
        for col in int_cols:
            df[col] = df[col].astype(pd.Int64Dtype())
    if "object" in types_to_convert:
        for col in object_cols:
            df[col] = df[col].astype(pd.StringDtype())

    return df


def build_merge_query(
    table: str,
    primary_key: str,
    source: "Source",
    stg_schema: str | None = None,
    stg_table: str = "stg",
    schema: str | None = None,
    df: Optional["spark.DataFrame"] = None,
) -> str:
    """Build a merge query for the simplest possible upsert scenario.

    Used for:
    - updating and inserting all fields
    - merging on a single column, which has the same name in both tables

    Args:
        table (str): The table to merge into.
        primary_key (str): The column on which to merge.
        source (Source): The Databricks object used to connect to the Spark cluster on
            which the query will be executed.
        stg_schema (str, Optional): The schema where the staging table is located.
        stg_table (str, Optional): The table with new/updated data.
        schema (str, Optional): The schema where the table is located.
        df (spark.DataFrame, Optional): A Spark DataFrame whose data will be upserted
            to a table.
    """
    fqn = f"{schema}.{table}"
    stg_fqn = f"{stg_schema}.{stg_table}" if stg_schema else {stg_table}

    if schema is None:
        schema = source.DEFAULT_SCHEMA

    # The `DataFrame` *is* the staging table.
    if df:
        df.createOrReplaceTempView(stg_fqn)

    # Get column names
    columns_query_result = _get_table_columns(schema=schema, table=table, source=source)
    columns = list(columns_query_result)

    columns_stg_fqn = [f"{stg_table}.{col}" for col in columns]

    # Build merge query
    update_pairs = [f"existing.{col} = {stg_table}.{col}" for col in columns]
    return f"""
    MERGE INTO {fqn} existing
        USING {stg_fqn} {stg_table}
        ON {stg_table}.{primary_key} = existing.{primary_key}
        WHEN MATCHED
            THEN UPDATE SET {", ".join(update_pairs)}
        WHEN NOT MATCHED
            THEN INSERT({", ".join(columns)})
            VALUES({", ".join(columns_stg_fqn)});
    """


def _get_table_columns(schema: str, table: str, source: "Source") -> str:
    if source.__class__.__name__ == "Databricks":
        result = source.run(f"SHOW COLUMNS IN {schema}.{table}", "pandas")
        columns_query_result = result["col_name"].values
    else:
        columns_query = f"""
        SELECT
            col.name
        FROM sys.tables AS tab
            INNER JOIN sys.columns AS col
                ON tab.object_id = col.object_id
        WHERE tab.name = '{table}'
        AND schema_name(tab.schema_id) = '{schema}'
        ORDER BY column_id;
        """
        cursor = source.con.cursor()
        columns_query_result = cursor.execute(columns_query).fetchall()
        cursor.close()

    return columns_query_result


def gen_bulk_insert_query_from_df(
    df: pd.DataFrame, table_fqn: str, chunksize: int = 1000, **kwargs
) -> str:
    """Converts a DataFrame to a bulk INSERT query.

    Args:
        df (pd.DataFrame): The DataFrame which data should be put into the INSERT query.
        table_fqn (str): The fully qualified name (schema.table) of the table
            to be inserted into.

    Returns:
        str: A bulk insert query that will insert all data from `df` into `table_fqn`.

    Examples:
    >>> data = [(1, "_suffixnan", 1), (2, "Noneprefix", 0), (3, "fooNULLbar", 1, 2.34)]
    >>> df = pd.DataFrame(data, columns=["id", "name", "is_deleted", "balance"])
    >>> df
       id        name  is_deleted  balance
    0   1  _suffixnan           1      NaN
    1   2  Noneprefix           0      NaN
    2   3  fooNULLbar           1     2.34
    >>> query = gen_bulk_insert_query_from_df(
    >>>     df, table_fqn="users", status="APPROVED", address=None
    >>> )
    >>> print(query)
    INSERT INTO users (id, name, is_deleted, balance, status, address)
    VALUES (1, '_suffixnan', 1, NULL, 'APPROVED', NULL),
           (2, 'Noneprefix', 0, NULL, 'APPROVED', NULL),
           (3, 'fooNULLbar', 1, 2.34, 'APPROVED', NULL);
    """
    if df.shape[1] == 1:
        msg = "Currently, this function only handles DataFrames with at least two columns."
        raise NotImplementedError(msg)

    def _gen_insert_query_from_records(records: list[tuple]) -> str:
        tuples = map(str, tuple(records))

        # Change Nones to NULLs
        none_nan_pattern = r"(?<=\W)(nan|None)(?=\W)"
        values = re.sub(none_nan_pattern, "NULL", (",\n" + " " * 7).join(tuples))

        # Change the double quotes into single quotes, as explained above.
        # Note this pattern should be improved at a later time to cover more edge cases.
        double_quotes_pattern = r'(")(.*)(")(\)|,)'
        values_clean = re.sub(double_quotes_pattern, r"'\2'\4", values)
        # Hacky - replaces starting and ending double quotes.
        values_clean = (
            values.replace('",', "',").replace(', "', ", '").replace('("', "('")
        )

        return f"INSERT INTO {table_fqn} ({columns})\n\nVALUES {values_clean}"

    df = df.copy().assign(**kwargs)
    df = _cast_df_cols(df)

    columns = ", ".join(df.columns)

    tuples_raw = df.itertuples(index=False, name=None)
    # Escape values with single quotes inside by adding another single quote
    # ("val'ue" -> "val''ue").
    # As Python wraps such strings in double quotes, which are interpreted as
    # column names by SQL Server, we later also replace the double quotes with
    # single quotes.
    tuples_escaped = [
        tuple(
            f"""{value.replace("'", "''")}""" if isinstance(value, str) else value
            for value in row
        )
        for row in tuples_raw
    ]

    if len(tuples_escaped) > chunksize:
        insert_query = ""
        chunk_start = 0
        for chunk_end in range(chunksize, len(tuples_escaped), chunksize):
            chunk = tuples_escaped[chunk_start:chunk_end]
            chunk_start += chunksize
            if len(tuples_escaped) - chunk_end < chunksize:
                chunk = tuples_escaped[chunk_end:]
            chunk_insert_query = _gen_insert_query_from_records(chunk)
            insert_query += chunk_insert_query + ";\n\n"
        return insert_query
    return _gen_insert_query_from_records(tuples_escaped)


def handle_if_empty(
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    message: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Task for handling empty input.

    Args:
        if_empty (Literal, optional): Action to take when input is empty.
            Options are "warn", "skip", or "fail". Defaults to "warn".
        message (str, optional): Message to show in warnings or errors.
            Defaults to None.
        logger (logging.Logger, optional): Logger instance to use for warnings.
            If None, a default logger is created.

    Raises:
        ValueError: If `if_empty` is set to `fail`.
        SKIP: If `if_empty` is set to `skip`.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    allowed = ["warn", "skip", "fail"]
    if if_empty not in allowed:
        error_msg = (
            f"Invalid value for if_empty: {if_empty}. Allowed values are {allowed}."
        )
        raise ValueError(error_msg)

    if if_empty == "warn":
        logger.warning(message)
    elif if_empty == "skip":
        raise SKIP(message)
    elif if_empty == "fail":
        raise ValueError(message)


def cleanup_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove some common data corruption from a pandas DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to be cleaned up.

    Returns:
        pd.DataFrame: The cleaned up DataFrame
    """
    return df.replace(r"\n|\t", "", regex=True)


def call_shell(command: str) -> str:
    """Run a shell command and return the output."""
    try:
        result = subprocess.check_output(command, shell=True)  # noqa: S602
    except subprocess.CalledProcessError as e:
        # TODO: read the error message from stdout and pass here
        msg = "Generating the file failed."
        raise ValueError(msg) from e
    else:
        return result


def df_snakecase_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Snakecase the column names of a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to snakecase.

    Returns:
        pd.DataFrame: The DataFrame with snakecased column names.
    """
    df.columns = (
        df.columns.str.strip().str.replace(" ", "_").str.replace("-", "_").str.lower()
    )
    return df


def join_dfs(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    left_on: str,
    right_on: str,
    columns_from_right_df: list[str] | None = None,
    how: Literal["left", "right", "outer", "inner", "cross"] = "left",
) -> pd.DataFrame:
    """Combine Data Frames according to the chosen method.

    Args:
        df_left (pd.DataFrame): Left dataframe.
        df_right (pd.DataFrame): Right dataframe.
        left_on (str): Column or index level names to join on in the left DataFrame.
        right_on (str): Column or index level names to join on in the right DataFrame.
        columns_from_right_df (list[str], optional): List of column to get from right
            dataframe. Defaults to None.
        how (Literal["left", "right", "outer", "inner", "cross"], optional): Type of
            merge to be performed. Defaults to "left".

    Returns:
        pd.DataFrame: Final dataframe after merging.
    """
    if columns_from_right_df is None:
        columns_from_right_df = df_right.columns

    return df_left.merge(
        df_right[columns_from_right_df],
        left_on=left_on,
        right_on=right_on,
        how=how,
    )


def add_viadot_metadata_columns(func: Callable) -> Callable:
    """A decorator for the 'to_df()' method.

    Adds viadot metadata columns to the returned DataFrame.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> pd.DataFrame:
        df = func(*args, **kwargs)

        # Accessing instance
        instance = args[0]
        _viadot_source = instance.__class__.__name__
        df["_viadot_source"] = _viadot_source
        df["_viadot_downloaded_at_utc"] = datetime.now(timezone.utc).replace(
            microsecond=0
        )
        return df

    return wrapper


def get_fqn(table_name: str, schema_name: str | None = None) -> str:
    """Get the fully qualified name of a table."""
    return f"{schema_name}.{table_name}" if schema_name else table_name


def validate_column_size(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> tuple[int, list[str]]:
    """Validate the size of the columns in the DataFrame.

    Logic: TODO

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): A dictionary containing validation parameters.
            Expected format: `column_size`: dict{column: size}
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): Counter for the number of failed tests.
        failed_tests_list (list): List to store descriptions of failed tests.

    Returns:
        tuple[int, list[str]]: Updated count of failed tests and list of failure
        descriptions.

    """
    try:
        for k, v in tests["column_size"].items():
            column_max_length = (
                df.astype(str).apply(lambda s: s.str.len()).max().to_dict()
            )
            try:
                if v == column_max_length[k]:
                    logger.log(level=stream_level, msg="[column_size] for {k} passed.")
                else:
                    logger.log(
                        level=stream_level,
                        msg=f"[column_size] test for {k} failed. field length is different than {v}",
                    )
                    failed_tests += 1
                    failed_tests_list.append("column_size error")
            except Exception as e:
                logger.log(level=stream_level, msg=f"{e}")
    except TypeError:
        logger.log(
            level=stream_level,
            msg=f"Please provide `column_size` parameter as dictionary {'columns': value}.",
        )
    return failed_tests, failed_tests_list


def validate_column_unique_values(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> tuple[int, list[str]]:
    """Validate whether a DataFrame column only contains unique values.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): A dictionary containing validation parameters.
            Expected format: `column_unique_values`: list[columns]
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): Counter for the number of failed tests.
        failed_tests_list (list): List to store descriptions of failed tests.

    Returns:
        tuple[int, list[str]]: Updated count of failed tests and list of failure
        descriptions.

    """
    for column in tests["column_unique_values"]:
        df_size = df.shape[0]
        if df[column].nunique() == df_size:
            logger.log(
                level=stream_level,
                msg=f"[column_unique_values] Values are unique for {column} column.",
            )
        else:
            failed_tests += 1
            failed_tests_list.append("column_unique_values error")
            logger.log(
                level=stream_level,
                msg=f"[column_unique_values] Values for {column} are not unique.",
            )
    return failed_tests, failed_tests_list


def validate_column_list_to_match(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> tuple[int, list[str]]:
    """Validate whether the columns of the DataFrame match the expected list.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): A dictionary containing validation parameters.
            Expected format: `column_list_to_match`: list[columns]
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): Counter for the number of failed tests.
        failed_tests_list (list): List to store descriptions of failed tests.

    Returns:
        tuple[int, list[str]]: Updated count of failed tests and list of failure
        descriptions.

    """
    if set(tests["column_list_to_match"]) == set(df.columns):
        logger.log(level=stream_level, msg=f"{tests['column_list_to_match']} passed.")
    else:
        failed_tests += 1
        failed_tests_list.append("column_list_to_match error")
        logger.log(
            level=stream_level,
            msg="[column_list_to_match] failed. Columns are different than expected.",
        )
    return failed_tests, failed_tests_list


def validate_dataset_row_count(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> tuple[int, list[str]]:
    """Validate the DataFrame row count.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): A dictionary containing validation parameters.
            Expected format: `dataset_row_count`: dict: {'min': number, 'max', number}
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): Counter for the number of failed tests.
        failed_tests_list (list): List to store descriptions of failed tests.

    Returns:
        tuple[int, list[str]]: Updated count of failed tests and list of failure
        descriptions.

    """
    row_count = len(df.iloc[:, 0])
    max_value = tests["dataset_row_count"]["max"] or 100_000_000
    min_value = tests["dataset_row_count"]["min"] or 0

    if (row_count > min_value) and (row_count < max_value):
        logger.log(level=stream_level, msg="[dataset_row_count] passed.")
    else:
        failed_tests += 1
        failed_tests_list.append("dataset_row_count error")
        logger.log(
            level=stream_level,
            msg=f"[dataset_row_count] Row count ({row_count}) is not between {min_value} and {max_value}.",
        )
    return failed_tests, failed_tests_list


def validate_column_match_regex(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> tuple[int, list[str]]:
    """Validate whether the values of a column match a regex pattern.

    Logic: TODO

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): A dictionary containing validation parameters.
            Expected format: `column_match_regex`: dict: {column: 'regex'}
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): Counter for the number of failed tests.
        failed_tests_list (list): List to store descriptions of failed tests.

    Returns:
        tuple[int, list[str]]: Updated count of failed tests and list of failure
        descriptions.

    """
    for k, v in tests["column_match_regex"].items():
        try:
            matches = df[k].apply(lambda x: bool(re.match(v, str(x))))  # noqa: B023
            if all(matches):
                logger.log(
                    level=stream_level,
                    msg=f"[column_match_regex] on {k} column passed.",
                )
            else:
                failed_tests += 1
                failed_tests_list.append("column_match_regex error")
                logger.log(
                    level=stream_level,
                    msg=f"[column_match_regex] on {k} column failed!",
                )
        except Exception as e:
            failed_tests += 1
            failed_tests_list.append("column_match_regex error")
            logger.log(
                level=stream_level,
                msg=f"[column_match_regex] Error in {k} column: {e}",
            )
    return failed_tests, failed_tests_list


def validate_column_sum(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> tuple[int, list[str]]:
    """Validate the sum of a column in the DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): A dictionary containing validation parameters.
            Expected format: `column_sum`: dict: {column: {'min': number,
              'max': number}}
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): Counter for the number of failed tests.
        failed_tests_list (list): List to store descriptions of failed tests.

    Returns:
        tuple[int, list[str]]: Updated count of failed tests and list of failure
        descriptions.

    """
    for column, bounds in tests["column_sum"].items():
        col_sum = df[column].sum()
        min_bound = bounds["min"]
        max_bound = bounds["max"]
        if min_bound <= col_sum <= max_bound:
            logger.log(
                level=stream_level,
                msg=f"[column_sum] Sum of {col_sum} for {column} is within the expected range.",
            )
        else:
            failed_tests += 1
            failed_tests_list.append("column_sum error")
            logger.log(
                level=stream_level,
                msg=f"[column_sum] Sum of {col_sum} for {column} is out of the expected range - <{min_bound}:{max_bound}>",
            )
    return failed_tests, failed_tests_list


def validate(
    df: pd.DataFrame,
    tests: dict | None = None,
    stream_level: int = logging.INFO,
    logger: logging.Logger | None = None,
) -> None:
    """Validate data. All numbers in the ranges are inclusive.

    Available tests:
        - `column_size`: dict{column: size}
        - `column_unique_values`: list[columns]
        - `column_list_to_match`: list[columns]
        - `dataset_row_count`: dict: {'min': number, 'max', number}
        - `column_match_regex`: dict: {column: 'regex'}
        - `column_sum`: dict: {column: {'min': number, 'max': number}}

    Args:
        df (pd.DataFrame): The dataframe to validate.
        tests (dict, optional): Tests to apply on the data frame. Defaults to None.

    Raises:
        ValidationError: If validation failed for at least one test.
    """
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    failed_tests = 0
    failed_tests_list = []

    if tests is not None:
        if "column_size" in tests:
            failed_tests, failed_tests_list = validate_column_size(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_unique_values" in tests:
            failed_tests, failed_tests_list = validate_column_unique_values(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_list_to_match" in tests:
            failed_tests, failed_tests_list = validate_column_list_to_match(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "dataset_row_count" in tests:
            failed_tests, failed_tests_list = validate_dataset_row_count(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_match_regex" in tests:
            failed_tests, failed_tests_list = validate_column_match_regex(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_sum" in tests:
            failed_tests, failed_tests_list = validate_column_sum(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

    if failed_tests > 0:
        failed_tests_msg = ", ".join(failed_tests_list)
        msg = f"Validation failed for {failed_tests} test(s): {failed_tests_msg}"
        raise ValidationError(msg)


def validate_and_reorder_dfs_columns(
    dataframes_list: list[pd.DataFrame],
) -> list[pd.DataFrame]:
    """Validate if dataframes from the list have the same column structure.

    Reorder columns to match the first DataFrame if necessary.

    Args:
        dataframes_list (list[pd.DataFrame]): List containing DataFrames.

    Raises:
        IndexError: If the list of DataFrames is empty.
        ValueError: If DataFrames have different column structures.
    """
    if not dataframes_list:
        message = "The list of dataframes is empty."
        raise IndexError(message)

    first_df_columns = dataframes_list[0].columns

    # Check that all DataFrames have the same columns
    for i, df in enumerate(dataframes_list):
        if set(df.columns) != set(first_df_columns):
            message = f"""DataFrame at index {i} does not have the same structure as
            the first DataFrame."""
            raise ValueError(message)
        if not df.columns.equals(first_df_columns):
            # Reorder columns for DataFrame at index 'i' to match the first DataFrame.
            dataframes_list[i] = df.loc[:, first_df_columns]

    return dataframes_list


def skip_test_on_missing_extra(source_name: str, extra: str) -> None:
    """Skip all tests in a file when a required extra is not installed.

    Args:
        source_name (str): The name of the source for which dependencies are missing.
        extra (str): The name of the extra that is missing.
    """
    import pytest

    msg = f"Missing required extra '{extra}' for source '{source_name}'."
    pytest.skip(
        msg,
        allow_module_level=True,
    )


def filter_df_columns(data_frame: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Function for creating DataFrame with only specified columns.

    Returns:
        pd.DataFrame: Pandas DataFrame
    """
    # Converting data_frame columns to lowercase for comparison
    data_frame_columns = [col.casefold() for col in data_frame.columns]
    needed_columns = []
    non_available_columns = []

    for column in columns:
        # Converting user-specified column to lowercase for comparison
        column_lower = column.casefold()

        if column_lower in data_frame_columns:
            # Find the original column name from data_frame.columns
            original_column_name = next(
                col for col in data_frame.columns if col.casefold() == column_lower
            )
            needed_columns.append(original_column_name)
        else:
            non_available_columns.append(column)

    # Error logger
    if non_available_columns:
        logging.error(
            f"Name of the columns: '{' | '.join(non_available_columns)}' are not in DataFrame. Please check spelling!\n"
            f"Available columns: {' | '.join(data_frame.columns)}"
        )
        msg = "Provided columns are not available!"
        raise ValueError(msg)

    # Selecting only needed columns from the original DataFrame
    return data_frame.loc[:, needed_columns]


def anonymize_df(
    df: pd.DataFrame,
    columns: list[str],
    method: Literal["mask", "hash"] = "mask",
    mask_value: str = "***",
    date_column: str | None = None,
    days: int | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Function that anonymize data in the dataframe in selected columns.

    It is possible to specify the condtition, for which data older than specified
    number of days will be anonymized.

    Args:
        df (pd.DataFrame): Dataframe with data to anonymize.
        columns (list[str]): List of columns to anonymize.
        method (Literal["mask", "hash"], optional): Method of anonymizing data.
            "mask" -> replace the data with "value" arg.
            "hash" -> replace the data with the hash value of an object
            (using `hash()` method). Defaults to "mask".
        mask_value (str, optional): Value to replace the data with  when using the
            "mask" method. Defaults to "***".
        date_column (str, optional): Name of the date column used to identify
            rows that are older than a specified number of days. Defaults to None.
        days (int, optional): The number of days beyond which we want to
            anonymize the data, e.g. older that 2 years can be: 2*365. Defaults to None.

    Examples:
        1. Implement "mask" method with "***" for all data in columns:
            ["email", "last_name", "phone"]:
            >>> anonymize_df(df=df, columns=["email", "last_name", "phone"])
        2. Implement "hash" method with in columns: ["email", "last_name", "phone"]:
            >>> anonymize_df(df=df, columns=["email", "last_name", "phone"],
            method = "hash")
        3. Implement "mask" method with "***" for data in columns:
            ["email", "last_name", "phone"], that is older than two years in
            "submission_date" column:
            >>> anonymize_df(df=df, columns=["email", "last_name", "phone"],
            date_column="submission_date", days=2*365)

    Raises:
        ValueError: If method or columns not found.

    Returns:
        pd.DataFrame: Operational dataframe with anonymized data.
    """
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    if all(col not in df.columns for col in columns):
        msg = f"""At least one of the following columns is not found in dataframe:
            {columns} or argument is not list. Provide list with proper column names."""
        raise ValueError(msg)

    if days and date_column:
        days_ago = datetime.now().date() - timedelta(days=days)
        df["temp_date_col"] = pd.to_datetime(df[date_column]).dt.date

        to_hash = df["temp_date_col"] < days_ago
        if any(to_hash) is False:
            logger.warning(f"No data that is older than {days} days.")
        else:
            logger.info(
                f"Data older than {days} days in {columns} columns will be anonymized."
            )
    else:
        to_hash = len(df.index) * [True]
        logger.info(
            f"""The 'days' and 'date_column' arguments were not specified. All data in
            {columns} columns will be anonymized."""
        )

    if method == "mask":
        df.loc[to_hash, columns] = mask_value
    elif method == "hash":
        df.loc[to_hash, columns] = df.loc[to_hash, columns].apply(
            lambda x: x.apply(hash)
        )
    else:
        msg = "Method not found. Use one of the available methods: 'mask', 'hash'."
        raise ValueError(msg)

    df.drop(columns=["temp_date_col"], inplace=True, errors="ignore")
    return df


def df_converts_bytes_to_int(df: pd.DataFrame) -> pd.DataFrame:
    """Task to convert bytes values to int.

    Args:
        df (pd.DataFrame): Data Frame to convert

    Returns:
        pd.DataFrame: Data Frame after convert
    """
    return df.applymap(lambda x: int(x) if isinstance(x, bytes) else x)


def df_clean_column(
    df: pd.DataFrame, columns_to_clean: list[str] | None = None
) -> pd.DataFrame:
    """Remove special characters from a pandas DataFrame.

    Args:
    df (pd.DataFrame): The DataFrame to clean.
    columns_to_clean (List[str]): A list of columns to clean. Defaults is None.

    Returns:
    pd.DataFrame: The cleaned DataFrame
    """
    df = df.copy()

    if columns_to_clean is None:
        df.replace(
            to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
            value=["", ""],
            regex=True,
            inplace=True,
        )
    else:
        for col in columns_to_clean:
            df[col].replace(
                to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                value=["", ""],
                regex=True,
                inplace=True,
            )
    return df


def parse_dates(
    date_filter: str | tuple[str, str] | None = None,
    dynamic_date_symbols: list[str] | None = None,
    dynamic_date_format: str = "%Y-%m-%d",
    dynamic_date_timezone: str = "UTC",
) -> pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None:
    """Parses a date or date range, supporting dynamic date symbols.

    Args:
        date_filter (str | tuple[str, str] | None):
            - A single date string (e.g., "2024-03-03").
            - A tuple containing exactly two date strings, 'start' and 'end' date.
            - None, which applies no date filter.
            Defaults to None.
        dynamic_date_symbols (list[str]): Symbols for dynamic date handling.
            Defaults to None.
        dynamic_date_format (str): Format used for dynamic date parsing.
            Defaults to "%Y-%m-%d".
        dynamic_date_timezone (str): Timezone used for dynamic date processing.
            Defaults to "UTC".

    Returns:
        pendulum.Date: If a single date is provided.
        tuple[pendulum.Date, pendulum.Date]: If a date range is provided.
        None: If `date_filter` is None.

    Raises:
        ValueError: If `date_filter` is neither a string nor a tuple of exactly
            two strings.
    """
    if date_filter is None:
        return None

    dynamic_date_symbols = dynamic_date_symbols or ["<<", ">>"]

    ddh = DynamicDateHandler(
        dynamic_date_symbols=dynamic_date_symbols,
        dynamic_date_format=dynamic_date_format,
        dynamic_date_timezone=dynamic_date_timezone,
    )

    match date_filter:
        case str():
            return pendulum.parse(ddh.process_dates(date_filter)).date()

        case (start, end) if isinstance(start, str) and isinstance(end, str):
            return (
                pendulum.parse(ddh.process_dates(start)).date(),
                pendulum.parse(ddh.process_dates(end)).date(),
            )

        case _:
            msg = "date_filter must be a string, a tuple of exactly 2 dates, or None."
            raise ValueError(msg)
