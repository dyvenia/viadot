import functools
import logging
import re
import subprocess
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Union

import pandas as pd
import pyodbc
import pyspark.sql.dataframe as spark
import requests

# from prefect.utilities.graphql import EnumValue, with_args
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from requests.packages.urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError

from .exceptions import APIError
from .signals import SKIP


def slugify(name: str) -> str:
    return name.replace(" ", "_").lower()


def handle_api_request(
    url: str,
    auth: tuple = None,
    params: Dict[str, Any] = None,
    headers: Dict[str, Any] = None,
    timeout: tuple = (3.05, 60 * 30),
    method: Literal["GET", "POST", "DELETE"] = "GET",
    data: str = None,
) -> requests.Response:
    """
    Send an HTTP request to the specified URL using the provided parameters.

    Args:
        url (str): The URL to send the request to.
        auth (tuple, optional): A tuple of (username, password) for basic authentication.
            Defaults to None.
        params (Dict[str, Any], optional): A dictionary of query string parameters.
            Defaults to None.
        headers (Dict[str, Any], optional): A dictionary of HTTP headers to include with the request.
            Defaults to None.
        timeout (tuple, optional): A tuple of (connect_timeout, read_timeout) in seconds.
            Defaults to (3.05, 60 * 30).
        method (Literal["GET", "POST", "DELETE"], optional): The HTTP method to use for the request.
            Defaults to "GET".
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

    response = session.request(
        method=method,
        url=url,
        auth=auth,
        params=params,
        headers=headers,
        timeout=timeout,
        data=data,
    )

    return response


def handle_response(
    response: requests.Response, timeout: tuple = (3.05, 60 * 30)
) -> requests.Response:
    url = response.url
    try:
        response.raise_for_status()
    except ReadTimeout as e:
        msg = "The connection was successful, "
        msg += f"however the API call to {url} timed out after {timeout[1]}s "
        msg += "while waiting for the server to return data."
        raise APIError(msg)
    except HTTPError as e:
        raise APIError(
            f"The API call to {url} failed. "
            "Perhaps your account credentials need to be refreshed?",
        ) from e
    except (ConnectionError, Timeout) as e:
        raise APIError(f"The API call to {url} failed due to connection issues.") from e
    except ProtocolError as e:
        raise APIError(f"Did not receive any reponse for the API call to {url}.")
    except Exception as e:
        raise APIError("Unknown error.") from e

    return response


def handle_api_response(
    url: str,
    auth: tuple = None,
    params: Dict[str, Any] = None,
    headers: Dict[str, Any] = None,
    timeout: tuple = (3.05, 60 * 30),
    method: Literal["GET", "POST", "DELETE"] = "GET",
    data: str = None,
) -> requests.models.Response:
    """
    Handle an HTTP response by applying retries and handling some common response
    codes.

    Args:
        url (str): The URL which trying to connect.
        auth (tuple, optional): A tuple of (username, password) for basic authentication.
            Defaults to None.
        params (Dict[str, Any], optional): The request parameters. Defaults to None.
        headers (Dict[str, Any], optional): The request headers. Defaults to None.
        method (Literal["GET", "POST", "DELETE"], optional): The HTTP method to use for the request.
            Defaults to "GET".
        timeout (tuple, optional): A tuple of (connect_timeout, read_timeout) in seconds.
            Defaults to (3.05, 60 * 30).
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

    response_handled = handle_response(response)
    return response_handled


def get_sql_server_table_dtypes(
    table: str, con: pyodbc.Connection, schema: str = None
) -> dict:
    """
    Get column names and types from a SQL Server database table.

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


def _cast_df_cols(df):

    df = df.replace({"False": False, "True": True})

    datetime_cols = (col for col, dtype in df.dtypes.items() if dtype.kind == "M")
    bool_cols = (col for col, dtype in df.dtypes.items() if dtype.kind == "b")
    int_cols = (col for col, dtype in df.dtypes.items() if dtype.kind == "i")

    for col in datetime_cols:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")

    for col in bool_cols:
        df[col] = df[col].astype(pd.Int64Dtype())

    for col in int_cols:
        df[col] = df[col].astype(pd.Int64Dtype())

    return df


def build_merge_query(
    table: str,
    primary_key: str,
    source: Union[pyodbc.Connection, "Databricks"],
    stg_schema: str = None,
    stg_table: str = "stg",
    schema: str = None,
    df: spark.DataFrame = None,
) -> str:
    """
    Build a merge query for the simplest possible upsert scenario:
    - updating and inserting all fields
    - merging on a single column, which has the same name in both tables

    Args:
        table (str): The table to merge into.
        primary_key (str): The column on which to merge.
        source (pyodbc.Connection or Databricks): Either the connection to the database
            or the Databricks object used to connect to the Spark cluster on which
            the query will be executed.
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
    columns = [tup for tup in columns_query_result]

    columns_stg_fqn = [f"{stg_table}.{col}" for col in columns]

    # Build merge query
    update_pairs = [f"existing.{col} = {stg_table}.{col}" for col in columns]
    merge_query = f"""
    MERGE INTO {fqn} existing
        USING {stg_fqn} {stg_table}
        ON {stg_table}.{primary_key} = existing.{primary_key}
        WHEN MATCHED
            THEN UPDATE SET {", ".join(update_pairs)}
        WHEN NOT MATCHED
            THEN INSERT({", ".join(columns)})
            VALUES({", ".join(columns_stg_fqn)});
    """
    return merge_query


def _get_table_columns(
    schema: str, table: str, source: Union[pyodbc.Connection, "Databricks"]
) -> str:
    if isinstance(source, pyodbc.Connection):
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
        cursor = source.cursor()
        columns_query_result = cursor.execute(columns_query).fetchall()
        cursor.close()

    else:
        result = source.run(f"SHOW COLUMNS IN {schema}.{table}", "pandas")
        columns_query_result = result["col_name"].values

    return columns_query_result


def gen_bulk_insert_query_from_df(
    df: pd.DataFrame, table_fqn: str, chunksize=1000, **kwargs
) -> str:
    """
    Converts a DataFrame to a bulk INSERT query.

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
    >>> query = gen_bulk_insert_query_from_df(df, "users", status="APPROVED", address=None)
    >>> print(query)
    INSERT INTO users (id, name, is_deleted, balance, status, address)
    VALUES (1, '_suffixnan', 1, NULL, 'APPROVED', NULL),
           (2, 'Noneprefix', 0, NULL, 'APPROVED', NULL),
           (3, 'fooNULLbar', 1, 2.34, 'APPROVED', NULL);
    """
    if df.shape[1] == 1:
        raise NotImplementedError(
            "Currently, this function only handles DataFrames with at least two columns."
        )

    def _gen_insert_query_from_records(records: List[tuple]) -> str:

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
            f"""{value.replace("'", "''")}""" if type(value) == str else value
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
    else:
        return _gen_insert_query_from_records(tuples_escaped)


def handle_if_empty(
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    message: str = None,
    logger: logging.Logger = logging.getLogger(__name__),
):
    """
    Task for handling empty file.
    Args:
        if_empty (Literal, optional): What to do if file is empty. Defaults to "warn".
        message (str, optional): Massage to show in warning and error messages.
            Defaults to None.
    Raises:
        ValueError: If `if_empty` is set to `fail`.
        SKIP: If `if_empty` is set to `skip`.
    """
    if if_empty == "warn":
        logger.warning(message)
    elif if_empty == "skip":
        raise SKIP(message)
    elif if_empty == "fail":
        raise ValueError(message)


def cleanup_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove some common data corruption from a pandas DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to be cleaned up.

    Returns:
        pd.DataFrame: The cleaned up DataFrame
    """
    return df.replace(r"\n|\t", "", regex=True)


def call_shell(command):
    try:
        result = subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as e:
        # TODO: read the error message fro mstdout and pass here
        raise ValueError("Generating the file failed.") from e
    return result


def df_snakecase_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip().str.replace(" ", "_").str.replace("-", "_").str.lower()
    )
    return df


def add_viadot_metadata_columns(func: Callable) -> Callable:
    "Decorator that adds metadata columns to df in 'to_df' method"

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
