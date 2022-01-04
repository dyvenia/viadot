import json
import os
from datetime import datetime, timezone
from typing import List, Literal

import pandas as pd
from pathlib import Path
import shutil
import json

from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

import prefect
from prefect import task
from prefect.storage import Git
from prefect.utilities import logging


logger = logging.get_logger()
METADATA_COLUMNS = {"_viadot_downloaded_at_utc": "DATETIME"}


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
def get_latest_timestamp_file_path(files: List[str]) -> str:
    """
    Return the name of the latest file in a given data lake directory,
    given a list of paths in that directory. Such list can be obtained using the
    `AzureDataLakeList` task. This task is useful for working with immutable data lakes as
    the data is often written in the format /path/table_name/TIMESTAMP.parquet.
    """

    logger = prefect.context.get("logger")

    extract_fname = (
        lambda f: os.path.basename(f).replace(".csv", "").replace(".parquet", "")
    )
    file_names = [extract_fname(file) for file in files]
    latest_file_name = max(file_names, key=lambda d: datetime.fromisoformat(d))
    latest_file = files[file_names.index(latest_file_name)]

    logger.debug(f"Latest file: {latest_file}")

    return latest_file


@task
def dtypes_to_json_task(dtypes_dict, local_json_path: str):
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task
def chunk_df(df: pd.DataFrame, size: int = 10_000) -> List[pd.DataFrame]:
    n_rows = df.shape[0]
    chunks = [df[i : i + size] for i in range(0, n_rows, size)]
    return chunks


@task
def df_get_data_types_task(df: pd.DataFrame) -> dict:
    typeset = CompleteSet()
    dtypes = infer_type(df, typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    return dtypes_dict


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
    if if_exists == "append" and os.path.isfile(path):
        csv_df = pd.read_csv(path, sep=sep)
        out_df = pd.concat([csv_df, df])
    elif if_exists == "replace":
        out_df = df
    elif if_exists == "skip":
        logger.info("Skipped.")
        return
    else:
        out_df = df
    out_df.to_csv(path, index=False, sep=sep)


@task
def df_to_parquet(
    df: pd.DataFrame,
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    **kwargs,
) -> None:
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
    out_df.to_parquet(path, index=False, **kwargs)


@task
def dtypes_to_json(dtypes_dict: dict, local_json_path: str) -> None:
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task
def union_dfs_task(dfs: List[pd.DataFrame]):
    return pd.concat(dfs, ignore_index=True)


@task
def write_to_json(dict_, path):

    logger = prefect.context.get("logger")

    if os.path.isfile(path):
        logger.warning(f"File {path} already exists. Overwriting...")
    else:
        logger.debug(f"Writing to {path}...")

    # create parent directories if they don't exist
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode="w") as f:
        json.dump(dict_, f)

    logger.debug(f"Successfully wrote to {path}.")


@task
def cleanup_validation_clutter(expectations_path):
    ge_project_path = Path(expectations_path).parent
    shutil.rmtree(ge_project_path)


class Git(Git):
    @property
    def git_clone_url(self):
        """
        Build the git url to clone
        """
        if self.use_ssh:
            return f"git@{self.repo_host}:{self.repo}"
        return f"https://{self.git_token_secret}@{self.repo_host}/{self.repo}"
