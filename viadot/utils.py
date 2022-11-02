import os
import re
from itertools import chain
from typing import Any, Dict, List, Literal

import pandas as pd
import prefect
import pyarrow.parquet
import pyodbc
import requests
from prefect.utilities import logging
from prefect.utilities.graphql import EnumValue, with_args
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from requests.packages.urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError

from .exceptions import APIError
from .signals import SKIP

logger = logging.get_logger(__name__)


def slugify(name: str) -> str:
    return name.replace(" ", "_").lower()


def handle_api_response(
    url: str,
    auth: tuple = None,
    params: Dict[str, Any] = None,
    headers: Dict[str, Any] = None,
    timeout: tuple = (3.05, 60 * 30),
    method: Literal["GET", "POST", "DELETE"] = "GET",
    body: str = None,
) -> requests.models.Response:
    """Handle and raise Python exceptions during request with retry strategy for specyfic status.
    Args:
        url (str): the URL which trying to connect.
        auth (tuple, optional): authorization information. Defaults to None.
        params (Dict[str, Any], optional): the request params also includes parameters such as the content type. Defaults to None.
        headers: (Dict[str, Any], optional): the request headers. Defaults to None.
        timeout (tuple, optional): the request times out. Defaults to (3.05, 60 * 30).
        method (Literal ["GET", "POST","DELETE"], optional): REST API method to use. Defaults to "GET".
        body (str, optional): Data to send using POST method. Defaults to None.
    Raises:
        ValueError: raises when 'method' parameter value hasn't been specified
        ReadTimeout: stop waiting for a response after a given number of seconds with the timeout parameter.
        HTTPError: exception that indicates when HTTP status codes returned values different than 200.
        ConnectionError: exception that indicates when client is unable to connect to the server.
        APIError: defined by user.
    Returns:
        requests.models.Response
    """
    if method.upper() not in ["GET", "POST", "DELETE"]:
        raise ValueError(
            f"Method not found. Please use one of the available methods: 'GET', 'POST', 'DELETE'."
        )
    try:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        with session.request(
            url=url,
            auth=auth,
            params=params,
            headers=headers,
            timeout=timeout,
            data=body,
            method=method,
        ) as response:
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


def get_flow_last_run_date(flow_name: str) -> str:
    """
    Retrieve a flow's last run date as an ISO datetime string.

    This function assumes you are already authenticated with Prefect Cloud.
    """
    client = prefect.Client()
    result = client.graphql(
        {
            "query": {
                with_args(
                    "flow_run",
                    {
                        "where": {
                            "flow": {"name": {"_eq": flow_name}},
                            "start_time": {"_is_null": False},
                            "state": {"_eq": "Success"},
                        },
                        "order_by": {"start_time": EnumValue("desc")},
                        "limit": 1,
                    },
                ): {"start_time"}
            }
        }
    )
    flow_run_data = result.get("data", {}).get("flow_run")

    if not flow_run_data:
        return None

    last_run_date_raw_format = flow_run_data[0]["start_time"]
    last_run_date = last_run_date_raw_format.split(".")[0] + "Z"
    return last_run_date


def get_sql_server_table_dtypes(
    table, con: pyodbc.Connection, schema: str = None
) -> dict:
    """Get column names and types from a SQL Server database table.

    Args:
        table (_type_): The table for which to fetch dtypes.
        con (pyodbc.Connection): The connection to the database where the table is located.
        schema (str, optional): The schema where the table is located. Defaults to None.

    Returns:
        dict: A dictionary of the form {column_name: dtype, column_name2: dtype2, ...}.
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
    stg_schema: str,
    stg_table: str,
    schema: str,
    table: str,
    primary_key: str,
    con: pyodbc.Connection,
) -> str:
    """
    Build a merge query for the simplest possible upsert scenario:
    - updating and inserting all fields
    - merging on a single column, which has the same name in both tables

    Args:
        stg_schema (str): The schema where the staging table is located.
        stg_table (str): The table with new/updated data.
        schema (str): The schema where the table is located.
        table (str): The table to merge into.
        primary_key (str): The column on which to merge.
        con (pyodbc.Connection) The connection to the database on which the
        query will be executed.
    """

    # Get column names
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
    cursor = con.cursor()
    columns_query_result = cursor.execute(columns_query).fetchall()
    cursor.close()

    columns = [tup[0] for tup in columns_query_result]
    columns_stg_fqn = [f"stg.{col}" for col in columns]

    # Build merge query
    update_pairs = [f"existing.{col} = stg.{col}" for col in columns]
    merge_query = f"""
    MERGE INTO {schema}.{table} existing
        USING {stg_schema}.{stg_table} stg
        ON stg.{primary_key} = existing.{primary_key}
        WHEN MATCHED
            THEN UPDATE SET {", ".join(update_pairs)}
        WHEN NOT MATCHED
            THEN INSERT({", ".join(columns)})
            VALUES({", ".join(columns_stg_fqn)});
    """
    return merge_query


def gen_bulk_insert_query_from_df(
    df: pd.DataFrame, table_fqn: str, chunksize=1000, **kwargs
) -> str:
    """
    Converts a DataFrame to a bulk INSERT query.

    Args:
        df (pd.DataFrame): The DataFrame which data should be put into the INSERT query.
        table_fqn (str): The fully qualified name (schema.table) of the table to be inserted into.

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
            values.replace('",', "',")
            .replace(', "', ", '")
            .replace('("', "('")
            .replace("\\'", "")
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


def union_dict(*dicts):
    """
    Function that union list of dictionaries

    Args:
        dicts (List[Dict]): list of dictionaries with credentials.

    Returns:
        Dict: A single dictionary createb by union method.

    Examples:

    >>> a = {"a":1}
    >>> b = {"b":2}
    >>> union_credentials_dict(a ,b)
    {'a': 1, 'b': 2}

    """
    return dict(chain.from_iterable(dct.items() for dct in dicts))


def handle_if_empty_file(
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    message: str = None,
):
    """
    Task for handling empty file.
    Args:
        if_empty (Literal, optional): What to do if file is empty. Defaults to "warn".
        message (str, optional): Massage to show in warning and error messages. Defaults to None.
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


def check_if_empty_file(
    path: str,
    if_empty: Literal["warn", "skip", "fail"] = "warn",
):
    """
    Task for checking if the file is empty and handling it.

    Args:
        path (str, required): Path to the local file.
        if_empty (Literal, optional): What to do if file is empty. Defaults to "warn".

    """
    if_empty = if_empty or "warn"
    if os.stat(path).st_size == 0:
        handle_if_empty_file(if_empty, message=f"Input file - '{path}' is empty.")

    # when Parquet file contains only metadata
    elif os.path.splitext(path)[1] == ".parquet":
        if pyarrow.parquet.read_metadata(path).num_columns == 0:
            handle_if_empty_file(if_empty, message=f"Input file - '{path}' is empty.")
