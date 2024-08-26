"""Util functions."""

from collections.abc import Callable
import contextlib
from datetime import datetime, timezone
import functools
import logging
import re
import subprocess
from typing import TYPE_CHECKING, Any, Literal, Optional

import pandas as pd
import pyodbc
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from requests.packages.urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError

from viadot.exceptions import APIError, ValidationError
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

    Raises:
        ReadTimeout: Stop waiting for a response after `timeout` seconds.
        HTTPError: The raised HTTP error.
        ConnectionError: Raised when the client is unable to connect to the server.
        APIError: viadot's generic API error.

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
    """Task for handling empty file.

    Args:
        if_empty (Literal, optional): What to do if file is empty. Defaults to "warn".
        message (str, optional): Massage to show in warning and error messages.
            Defaults to None.

    Raises:
        ValueError: If `if_empty` is set to `fail`.
        SKIP: If `if_empty` is set to `skip`.
    """
    if not logger:
        logger = logging.getLogger(__name__)
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
) -> None:
    """Validate the size of the columns in the DataFrame.

    Logic: TODO

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): _description_
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): _description_
        failed_tests_list (list): _description_
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


def validate_column_unique_values(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> None:
    """Validate whether a DataFrame column only contains unique values.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): _description_
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): _description_
        failed_tests_list (list): _description_
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


def validate_column_list_to_match(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> None:
    """Validate whether the columns of the DataFrame match the expected list.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): _description_
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): _description_
        failed_tests_list (list): _description_
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


def validate_dataset_row_count(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> None:
    """Validate the DataFrame row count.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): _description_
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): _description_
        failed_tests_list (list): _description_
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


def validate_column_match_regex(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> None:
    """Validate whether the values of a column match a regex pattern.

    Logic: TODO

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): _description_
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): _description_
        failed_tests_list (list): _description_
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


def validate_column_sum(
    df: pd.DataFrame,
    tests: dict[str, Any],
    logger: logging.Logger,
    stream_level: int,
    failed_tests: int,
    failed_tests_list: list,
) -> None:
    """Validate the sum of a column in the DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to validate.
        tests (dict[str, Any]): _description_
        logger (logging.Logger): The logger to use.
        stream_level (int): The logging level to use for logging.
        failed_tests (int): _description_
        failed_tests_list (list): _description_
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
        logger = logging.getLogger("prefect_shell.utils")

    failed_tests = 0
    failed_tests_list = []

    if tests is not None:
        if "column_size" in tests:
            validate_column_size(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_unique_values" in tests:
            validate_column_unique_values(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_list_to_match" in tests:
            validate_column_list_to_match(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "dataset_row_count" in tests:
            validate_dataset_row_count(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_match_regex" in tests:
            validate_column_match_regex(
                df, tests, logger, stream_level, failed_tests, failed_tests_list
            )

        if "column_sum" in tests:
            validate_column_sum(
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
