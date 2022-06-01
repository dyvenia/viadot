import copy
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal, Dict, Any

import pandas as pd
import prefect
import pyarrow as pa
import pyarrow.dataset as ds
from prefect import task, Task
from prefect.storage import Git
from prefect.utilities import logging
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet
from .tasks.azure_key_vault import AzureKeyVaultSecret

from .exceptions import ValidationError
from .sources.base import SQL

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
    except:
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
    except:
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
def write_to_json(dict_, path):
    """
    Creates json file from a dictionary. Log record informs about the writing file proccess.
    Args:
        dict_ (dict): Dictionary.
        path (str): Path to local json file.
    """
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


@task
def df_converts_bytes_to_int(df: pd.DataFrame) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("Converting bytes in dataframe columns to list of integers")
    return df.applymap(lambda x: list(map(int, x)) if isinstance(x, bytes) else x)


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


class EnsureDFColumnOrder(Task):
    """
    Task for checking the order of columns in the loaded DF and in the SQL table into which the data from DF will be loaded.
    If order is different then DF columns are reordered according to the columns of the SQL table.

    Args:
            table (str, optional): SQL table name without schema. Defaults to None.
            schema (str, optional): SQL schema name. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            df (pd.DataFrame, optional): Data Frame. Defaults to None.
            dtypes (Dict[str,Any], optional): The data types to use for the table. Defaults to None.
            config_key (str, optional): Config key containing credentials to database. Defaults to None.
            driver (str, optional): The SQL driver to use. Defaults to "ODBC Driver 17 for SQL Server".

    """

    def __init__(
        self,
        table: str = None,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "replace",
        df: pd.DataFrame = None,
        dtypes: Dict[str, Any] = None,
        config_key: str = None,
        driver: str = "ODBC Driver 17 for SQL Server",
        *args,
        **kwargs,
    ):
        self.table = table
        self.schema = schema
        self.if_exists = if_exists
        self.df = df
        self.dtypes = dtypes
        self.config_key = config_key
        self.driver = driver

        super().__init__(name="run_ensure_df_column_order", *args, **kwargs)

    def df_change_order(
        self, df: pd.DataFrame = None, sql_column_list: List[str] = None
    ):
        """
        Function to change column order in a DataFrame based on order in SQL table.
        Args:
            df(pd.DataFrame, optional): DataFrame to transform. Defaults to None.
            sql_column_list(List[str], optional): List of columns in SQL table. Defaults to None.
        """
        df_column_list = list(df.columns)
        if set(df_column_list) == set(sql_column_list):
            df_changed = df.loc[:, sql_column_list]
        else:
            raise ValidationError(
                "Detected discrepancies in number of columns or different column names between the CSV file and the SQL table!"
            )

        return df_changed

    def sanitize_columns(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Function to remove spaces from the beginning and from the end of a column name.
        Args:
            df(pd.DataFrame): Dataframe to transform. Defaults to None.
        """
        for col in df.columns:
            df = df.rename(columns={col: col.strip()})
        return df

    def run(
        self,
        table: str = None,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "replace",
        df: pd.DataFrame = None,
        dtypes: Dict[str, Any] = None,
        driver: str = "ODBC Driver 17 for SQL Server",
        credentials_secret: dict = None,
        vault_name: str = None,
    ):
        """
        Run a checking column order

        Args:
            table (str, optional): SQL table name without schema. Defaults to None.
            schema (str, optional): SQL schema name. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            df (pd.DataFrame, optional): Data Frame. Defaults to None.
            dtypes (Dict[str,Any], optional): The data types to use for the table. Defaults to None.
            driver (str, optional): The SQL driver to use. Defaults to "ODBC Driver 17 for SQL Server".
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with SQL db credentials (server, db_name, user, and password). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

        """
        credentials_str = AzureKeyVaultSecret(
            credentials_secret, vault_name=vault_name
        ).run()
        credentials = json.loads(credentials_str)
        sql = SQL(driver=driver, credentials=credentials)
        df = self.sanitize_columns(df)
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
        check_result = sql.to_df(query)
        if if_exists not in ["replace", "fail"]:
            if if_exists == "append" and check_result.empty:
                self.logger.warning("Aimed table doesn't exists.")
                return
            elif check_result.empty == False:
                result = sql.to_df(query)
                sql_column_list = list(result["COLUMN_NAME"])
                df_column_list = list(df.columns)
                if sql_column_list != df_column_list:
                    self.logger.warning(
                        "Detected column order difference between the CSV file and the table. Reordering..."
                    )
                    df = self.df_change_order(df=df, sql_column_list=sql_column_list)
                else:
                    return df
        elif if_exists == "replace" and dtypes is not None:
            dtypes_columns_order = list(dtypes.keys())
            df = self.df_change_order(df=df, sql_column_list=dtypes_columns_order)
            return df
        else:
            self.logger.info("The table will be replaced.")
            return df


class Git(Git):
    @property
    def git_clone_url(self):
        """
        Build the git url to clone
        """
        if self.use_ssh:
            return f"git@{self.repo_host}:{self.repo}"
        return f"https://{self.git_token_secret}@{self.repo_host}/{self.repo}"
