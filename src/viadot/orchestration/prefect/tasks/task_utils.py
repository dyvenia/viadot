"""Utility tasks."""

import json
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet


@task
def dtypes_to_json_task(dtypes_dict: dict[str, Any], local_json_path: str) -> None:
    """Creates json file from a dictionary.

    Args:
        dtypes_dict (dict): Dictionary containing data types.
        local_json_path (str): Path to local json file.
    """
    with Path(local_json_path).open("w") as file_path:
        json.dump(dtypes_dict, file_path)


@task
def chunk_df(df: pd.DataFrame, size: int = 10_000) -> list[pd.DataFrame]:
    """Chunks a data frame into multiple smaller data frames of a specified size.

    Args:
        df (pd.DataFrame): Input pandas DataFrame.
        size (int, optional): Size of a chunk. Defaults to 10000.
    """
    n_rows = df.shape[0]
    return [df[i : i + size] for i in range(0, n_rows, size)]


@task
def df_get_data_types_task(df: pd.DataFrame) -> dict:
    """Returns dictionary containing datatypes of pandas DataFrame columns.

    Args:
        df (pd.DataFrame): Input pandas DataFrame.
    """
    typeset = CompleteSet()
    dtypes = infer_type(df, typeset)
    return {k: str(v) for k, v in dtypes.items()}


@task
def get_sql_dtypes_from_df(df: pd.DataFrame) -> dict:
    """Obtain SQL data types from a pandas DataFrame."""
    typeset = CompleteSet()
    dtypes = infer_type(df.head(10000), typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    dict_mapping = {
        "Float": "REAL",
        "Image": None,
        "Categorical": "VARCHAR(500)",
        "Time": "TIME",
        "Boolean": "VARCHAR(5)",  # Bool is True/False, Microsoft expects 0/1
        "DateTime": "DATETIMEOFFSET",  # DATETIMEOFFSET is timezone-aware dtype in TSQL
        "Object": "VARCHAR(500)",
        "EmailAddress": "VARCHAR(50)",
        "File": None,
        "Geometry": "GEOMETRY",
        "Ordinal": "INT",
        "Integer": "INT",
        "Generic": "VARCHAR(500)",
        "UUID": "VARCHAR(50)",  # Microsoft uses a custom UUID format so we can't use it
        "Complex": None,
        "Date": "DATE",
        "String": "VARCHAR(500)",
        "IPAddress": "VARCHAR(39)",
        "Path": "VARCHAR(255)",
        "TimeDelta": "VARCHAR(20)",  # datetime.datetime.timedelta; eg.'1 days 11:00:00'
        "URL": "VARCHAR(255)",
        "Count": "INT",
    }
    dict_dtypes_mapped = {}
    for k in dtypes_dict:
        dict_dtypes_mapped[k] = dict_mapping[dtypes_dict[k]]

    # This is required as pandas cannot handle mixed dtypes in Object columns
    return {
        k: ("String" if v == "Object" else str(v))
        for k, v in dict_dtypes_mapped.items()
    }


@task
def df_map_mixed_dtypes_for_parquet(
    df: pd.DataFrame, dtypes_dict: dict
) -> pd.DataFrame:
    """Handle mixed dtypes in DataFrame columns.

    Mapping 'object' visions dtype to 'string' dtype to allow Pandas to_parquet

    Args:
        dict_dtypes_mapped (dict): Data types dictionary, inferred by Visions.
        df (pd.DataFrame): input DataFrame.

    Returns:
        df_mapped (pd.DataFrame): Pandas DataFrame with mapped Data Types to workaround
        Pandas to_parquet bug connected with mixed dtypes in object:.
    """
    df_mapped = df.copy()
    for col, dtype in dtypes_dict.items():
        if dtype == "Object":
            df_mapped[col] = df_mapped[col].astype("string")
    return df_mapped


@task
def update_dtypes_dict(dtypes_dict: dict) -> dict:
    """Task to update dtypes_dictionary that will be stored in the schema.

    It's required due to workaround Pandas to_parquet bug connected with mixed dtypes in
    object.

    Args:
        dtypes_dict (dict): Data types dictionary inferred by Visions.

    Returns:
        dtypes_dict_updated (dict): Data types dictionary updated to follow Pandas
        requirements in to_parquet functionality.
    """
    return {k: ("String" if v == "Object" else str(v)) for k, v in dtypes_dict.items()}


@task
def df_to_csv(
    df: pd.DataFrame,
    path: str,
    sep: str = "\t",
    if_exists: Literal["append", "replace", "skip"] = "replace",
    **kwargs,
) -> None:
    r"""Write data from a pandas DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): Input pandas DataFrame.
        path (str): Path to output csv file.
        sep (str, optional): The separator to use in the CSV. Defaults to "\t".
        if_exists (Literal["append", "replace", "skip"], optional): What to do if the
            table exists. Defaults to "replace".
    """
    logger = get_run_logger()

    if Path(path).exists():
        if if_exists == "append":
            existing_df = pd.read_csv(path, sep=sep)
            out_df = pd.concat([existing_df, df])
        elif if_exists == "replace":
            out_df = df
        elif if_exists == "skip":
            logger.info("Skipped.")
            return
    else:
        out_df = df

    # Create directories if they don't exist.
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    out_df.to_csv(path, index=False, sep=sep, **kwargs)


@task
def df_to_parquet(
    df: pd.DataFrame,
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    **kwargs,
) -> None:
    """Task to create parquet file based on pandas DataFrame.

    Args:
        df (pd.DataFrame): Input pandas DataFrame.
        path (str): Path to output parquet file.
        if_exists (Literal["append", "replace", "skip"], optional): What to do if the
            table exists. Defaults to "replace".
    """
    logger = get_run_logger()

    if Path(path).exists():
        if if_exists == "append":
            existing_df = pd.read_parquet(path)
            out_df = pd.concat([existing_df, df])
        elif if_exists == "replace":
            out_df = df
        elif if_exists == "skip":
            logger.info("Skipped.")
            return
    else:
        out_df = df

    # Create directories if they don't exist.
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    out_df.to_parquet(path, index=False, **kwargs)


@task
def union_dfs_task(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Create one DataFrame from a list of pandas DataFrames.

    Args:
        dfs (List[pd.DataFrame]): List of pandas data frames to concat. In case of
            different size of DataFrames NaN values can appear.
    """
    return pd.concat(dfs, ignore_index=True)
