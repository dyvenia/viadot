import copy
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

from prefect import task
from prefect.logging import get_run_logger


@task
def add_ingestion_metadata_task(
    df: pd.DataFrame,
):
    """Add ingestion metadata columns, eg. data download date

    Args:
        df (pd.DataFrame): input DataFrame.
    """
    df2 = df.copy(deep=True)
    df2["_viadot_downloaded_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0)
    return df2


@task
def dtypes_to_json_task(dtypes_dict, local_json_path: str):
    """
    Creates json file from a dictionary.
    Args:
        dtypes_dict (dict): Dictionary containing data types.
        local_json_path (str): Path to local json file.
    """
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task
def chunk_df(df: pd.DataFrame, size: int = 10_000) -> List[pd.DataFrame]:
    """
    Creates pandas Dataframes list of chunks with a given size.
    Args:
        df (pd.DataFrame): Input pandas DataFrame.
        size (int, optional): Size of a chunk. Defaults to 10000.
    """
    n_rows = df.shape[0]
    chunks = [df[i : i + size] for i in range(0, n_rows, size)]
    return chunks


@task
def df_get_data_types_task(df: pd.DataFrame) -> dict:
    """
    Returns dictionary containing datatypes of pandas DataFrame columns.
    Args:
        df (pd.DataFrame): Input pandas DataFrame.
    """
    typeset = CompleteSet()
    dtypes = infer_type(df, typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    return dtypes_dict


@task
def get_sql_dtypes_from_df(df: pd.DataFrame) -> dict:
    """Obtain SQL data types from a pandas DataFrame"""
    typeset = CompleteSet()
    dtypes = infer_type(df.head(10000), typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    dict_mapping = {
        "Float": "REAL",
        "Image": None,
        "Categorical": "VARCHAR(500)",
        "Time": "TIME",
        "Boolean": "VARCHAR(5)",  # Bool is True/False, Microsoft expects 0/1
        "DateTime": "DATETIMEOFFSET",  # DATETIMEOFFSET is the only timezone-aware dtype in TSQL
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
        "TimeDelta": "VARCHAR(20)",  # datetime.datetime.timedelta; eg. '1 days 11:00:00'
        "URL": "VARCHAR(255)",
        "Count": "INT",
    }
    dict_dtypes_mapped = {}
    for k in dtypes_dict:
        dict_dtypes_mapped[k] = dict_mapping[dtypes_dict[k]]

    # This is required as pandas cannot handle mixed dtypes in Object columns
    dtypes_dict_fixed = {
        k: ("String" if v == "Object" else str(v))
        for k, v in dict_dtypes_mapped.items()
    }

    return dtypes_dict_fixed


@task
def update_dict(d: dict, d_new: dict) -> dict:
    d_copy = copy.deepcopy(d)
    d_copy.update(d_new)
    return d_copy


@task
def df_map_mixed_dtypes_for_parquet(
    df: pd.DataFrame, dtypes_dict: dict
) -> pd.DataFrame:
    """
    Pandas is not able to handle mixed dtypes in the column in to_parquet
    Mapping 'object' visions dtype to 'string' dtype to allow Pandas to_parquet

    Args:
        dict_dtypes_mapped (dict): Data types dictionary inferenced by Visions
        df (pd.DataFrame): input DataFrame.

    Returns:
        df_mapped (pd.DataFrame): Pandas DataFrame with mapped Data Types to workaround Pandas to_parquet bug connected with mixed dtypes in object:.
    """
    df_mapped = df.copy()
    for col, dtype in dtypes_dict.items():
        if dtype == "Object":
            df_mapped[col] = df_mapped[col].astype("string")
    return df_mapped


@task
def update_dtypes_dict(dtypes_dict: dict) -> dict:
    """
    Task to update dtypes_dictionary that will be stored in the schema. It's required due to workaround Pandas to_parquet bug connected with mixed dtypes in object

    Args:
        dtypes_dict (dict): Data types dictionary inferenced by Visions

    Returns:
        dtypes_dict_updated (dict): Data types dictionary updated to follow Pandas requeirments in to_parquet functionality.
    """
    dtypes_dict_updated = {
        k: ("String" if v == "Object" else str(v)) for k, v in dtypes_dict.items()
    }

    return dtypes_dict_updated


@task
def df_to_csv(
    df: pd.DataFrame,
    path: str,
    sep="\t",
    if_exists: Literal["append", "replace", "skip"] = "replace",
    **kwargs,
) -> None:
    """
    Task to create csv file based on pandas DataFrame.
    Args:
    df (pd.DataFrame): Input pandas DataFrame.
    path (str): Path to output csv file.
    sep (str, optional): The separator to use in the CSV. Defaults to "\t".
    if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
    """
    logger = get_run_logger()

    if if_exists == "append" and os.path.isfile(path):
        csv_df = pd.read_csv(path, sep=sep)
        out_df = pd.concat([csv_df, df])
    elif if_exists == "replace":
        out_df = df
    elif if_exists == "skip" and os.path.isfile(path):
        logger.info("Skipped.")
        return
    else:
        out_df = df

    # create directories if they don't exist
    try:
        if not os.path.isfile(path):
            directory = os.path.dirname(path)
            os.makedirs(directory, exist_ok=True)
    except Exception:
        pass

    out_df.to_csv(path, index=False, sep=sep)


@task
def df_to_parquet(
    df: pd.DataFrame,
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    **kwargs,
) -> None:
    """
    Task to create parquet file based on pandas DataFrame.
    Args:
    df (pd.DataFrame): Input pandas DataFrame.
    path (str): Path to output parquet file.
    if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
    """
    logger = get_run_logger()

    if if_exists == "append" and os.path.isfile(path):
        parquet_df = pd.read_parquet(path)
        out_df = pd.concat([parquet_df, df])
    elif if_exists == "replace":
        out_df = df
    elif if_exists == "skip":
        logger.info("Skipped.")
        return
    else:
        out_df = df

    # create directories if they don't exist
    try:
        if not os.path.isfile(path):
            directory = os.path.dirname(path)
            os.makedirs(directory, exist_ok=True)
    except Exception:
        pass

    out_df.to_parquet(path, index=False, **kwargs)


@task
def union_dfs_task(dfs: List[pd.DataFrame]):
    """
    Create one DataFrame from a list of pandas DataFrames.
    Args:
        dfs (List[pd.DataFrame]): List of pandas Dataframes to concat. In case of different size of DataFrames NaN values can appear.
    """
    return pd.concat(dfs, ignore_index=True)


@task
def cleanup_validation_clutter(expectations_path):
    ge_project_path = Path(expectations_path).parent
    shutil.rmtree(ge_project_path)


@task
def df_to_dataset(
    df: pd.DataFrame, partitioning_flavor="hive", format="parquet", **kwargs
) -> None:
    """
    Use `pyarrow.dataset.write_to_dataset()` to write from a pandas DataFrame to a dataset.
    This enables several data lake-specific optimizations such as parallel writes, partitioning,
    and file size (via `max_rows_per_file` parameter).

    Args:
        df (pd.DataFrame): The pandas DataFrame to write.
        partitioning_flavor (str, optional): The partitioning flavor to use. Defaults to "hive".
        format (str, optional): The dataset format. Defaults to 'parquet'.
        kwargs: Keyword arguments to be passed to `write_to_dataset()`. See
        https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html.

    Examples:
        table = pa.Table.from_pandas(df_contact)
        base_dir = "/home/viadot/contact"
        partition_cols = ["updated_at_year", "updated_at_month", "updated_at_day"]

        df_to_dataset(
            data=table,
            base_dir=base_dir,
            partitioning=partition_cols,
            existing_data_behavior='overwrite_or_ignore',
            max_rows_per_file=100_000
        )
    """
    table = pa.Table.from_pandas(df)
    ds.write_dataset(
        data=table, partitioning_flavor=partitioning_flavor, format=format, **kwargs
    )


@task
def df_clean_column(
    df: pd.DataFrame, columns_to_clean: List[str] = None
) -> pd.DataFrame:
    """
    Function that removes special characters (such as escape symbols)
    from a pandas DataFrame.

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
