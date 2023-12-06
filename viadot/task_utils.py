import copy
import json
import os
import re
import shutil
import pendulum
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, List, Literal, Union, cast, Tuple

import pandas as pd
import prefect
import pyarrow as pa
import pyarrow.dataset as ds
from prefect import Flow, Task, task
from prefect.backend import set_key_value
from prefect.engine.state import Failed
from prefect.engine.runner import ENDRUN
from prefect.storage import Git
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from toolz import curry
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

from viadot.config import local_config
from viadot.exceptions import CredentialError, ValidationError
from viadot.tasks import AzureDataLakeUpload, AzureKeyVaultSecret


logger = logging.get_logger()
METADATA_COLUMNS = {"_viadot_downloaded_at_utc": "DATETIME"}


@task(timeout=3600)
def add_ingestion_metadata_task(
    df: pd.DataFrame,
):
    """Add ingestion metadata columns, eg. data download date

    Args:
        df (pd.DataFrame): input DataFrame.
    """
    # Don't skip when df has columns but has no data
    if len(df.columns) == 0:
        return df
    else:
        df2 = df.copy(deep=True)
        df2["_viadot_downloaded_at_utc"] = datetime.now(timezone.utc).replace(
            microsecond=0
        )
        return df2


@task(timeout=3600)
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


@task(timeout=3600)
def dtypes_to_json_task(dtypes_dict, local_json_path: str):
    """
    Creates json file from a dictionary.
    Args:
        dtypes_dict (dict): Dictionary containing data types.
        local_json_path (str): Path to local json file.
    """
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task(timeout=3600)
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


@task(timeout=3600)
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


@task(timeout=3600)
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


@task(timeout=3600)
def update_dict(d: dict, d_new: dict) -> dict:
    d_copy = copy.deepcopy(d)
    d_copy.update(d_new)
    return d_copy


@task(timeout=3600)
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


@task(timeout=3600)
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


@task(timeout=3600)
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


@task(timeout=3600)
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


@task(timeout=3600)
def union_dfs_task(dfs: List[pd.DataFrame]):
    """
    Create one DataFrame from a list of pandas DataFrames.
    Args:
        dfs (List[pd.DataFrame]): List of pandas Dataframes to concat. In case of different size of DataFrames NaN values can appear.
    """
    return pd.concat(dfs, ignore_index=True)


@task(timeout=3600)
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


@task(timeout=3600)
def cleanup_validation_clutter(expectations_path):
    ge_project_path = Path(expectations_path).parent
    shutil.rmtree(ge_project_path)


@task(timeout=3600)
def df_converts_bytes_to_int(df: pd.DataFrame) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("Converting bytes in dataframe columns to list of integers")
    return df.applymap(lambda x: list(map(int, x)) if isinstance(x, bytes) else x)


@task(max_retries=3, retry_delay=timedelta(seconds=10), timeout=3600)
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


@curry
def custom_mail_state_handler(
    tracked_obj: Union["Flow", "Task"],
    old_state: prefect.engine.state.State,
    new_state: prefect.engine.state.State,
    only_states: list = [Failed],
    local_api_key: str = None,
    credentials_secret: str = None,
    vault_name: str = None,
    from_email: str = None,
    to_emails: str = None,
) -> prefect.engine.state.State:
    """
    Custom state handler configured to work with sendgrid.
    Works as a standalone state handler, or can be called from within a custom state handler.
    Args:
        tracked_obj (Task or Flow): Task or Flow object the handler is registered with.
        old_state (State): previous state of tracked object.
        new_state (State): new state of tracked object.
        only_states ([State], optional): similar to `ignore_states`, but instead _only_
            notifies you if the Task / Flow is in a state from the provided list of `State`
            classes.
        local_api_key (str, optional): Api key from local config.
        credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with API KEY.
        vault_name (str, optional): Name of key vault.
        from_email (str): Sender mailbox address.
        to_emails (str): Receiver mailbox address.
    Returns: State: the `new_state` object that was provided

    """

    if credentials_secret is None:
        try:
            credentials_secret = PrefectSecret("SENDGRID_DEFAULT_SECRET").run()
        except ValueError:
            pass

    if credentials_secret is not None:
        credentials_str = AzureKeyVaultSecret(
            credentials_secret, vault_name=vault_name
        ).run()
        api_key = json.loads(credentials_str).get("API_KEY")
    elif local_api_key is not None:
        api_key = local_config.get(local_api_key).get("API_KEY")
    else:
        raise Exception("Please provide API KEY")

    curr_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    only_states = only_states or []
    if only_states and not any(
        [isinstance(new_state, included) for included in only_states]
    ):
        return new_state
    url = prefect.client.Client().get_cloud_url(
        "flow-run", prefect.context["flow_run_id"], as_user=False
    )
    message = Mail(
        from_email=from_email,
        to_emails=to_emails,
        subject=f"The flow {tracked_obj.name} - Status {new_state}",
        html_content=f"<strong>The flow {cast(str,tracked_obj.name)} FAILED at {curr_dt}. \
    <p>More details here: {url}</p></strong>",
    )
    try:
        send_grid = SendGridAPIClient(api_key)
        response = send_grid.send(message)
    except Exception as e:
        raise e

    return new_state


@task(timeout=3600)
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
    pd.DataFrame: The cleaned DataFrame.
    """

    df = df.copy()
    logger.info(f"Removing special characters from dataframe columns...")

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


@task(timeout=3600)
def concat_dfs(dfs: List[pd.DataFrame]):
    """
    Task to combine list of data frames into one.

    Args:
        dfs (List[pd.DataFrame]): List of dataframes to concat.
    Returns:
        pd.DataFrame(): Pandas dataframe containing all columns from dataframes from list.
    """
    return pd.concat(dfs, axis=1)


@task(timeout=3600)
def cast_df_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task for casting an entire DataFrame to a string data type. Task is needed
    when data is being uploaded from Parquet file to DuckDB because empty columns
    can be casted to INT instead of default VARCHAR.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        df_mapped (pd.DataFrame): Pandas DataFrame casted to string.
    """
    df_mapped = df.astype("string")
    return df_mapped


@task(timeout=3600)
def set_new_kv(kv_name: str, df: pd.DataFrame, filter_column: str):
    """
    Task for updating/setting key value on Prefect based on the newest
    values in pandas DataFrame.

    Args:
        kv_name (str): Name of key value to change.
        df (pd.DataFrame): DataFrame based on which value will be updated.
        filter_column (str): Field from which obtain new value.
    """
    if df.empty:
        logger.warning("Input DataFrame is empty. Cannot set a new key value.")
    else:
        new_value = str(df[filter_column].max()).strip()
        set_key_value(key=kv_name, value=new_value)


class Git(Git):
    @property
    def git_clone_url(self):
        """
        Build the git url to clone.
        """
        if self.use_ssh:
            return f"git@{self.repo_host}:{self.repo}"
        return f"https://{self.git_token_secret}@{self.repo_host}/{self.repo}"


@task(timeout=3600)
def credentials_loader(credentials_secret: str, vault_name: str = None) -> dict:
    """
    Function that gets credentials from azure Key Vault or PrefectSecret or from local config.

    Args:
        credentials_secret (str): The name of the Azure Key Vault secret containing a dictionary
        with credentials.
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

    Returns:
        credentials (dict): Credentials as dictionary.
    """

    if credentials_secret:
        try:
            credentials = local_config[credentials_secret]
            logger.info("Successfully loaded credentials from local config.")
        except (ValueError, KeyError):
            try:
                azure_secret_task = AzureKeyVaultSecret()
                credentials_str = azure_secret_task.run(
                    secret=credentials_secret, vault_name=vault_name
                )
                credentials = json.loads(credentials_str)
                logger.info("Successfully loaded credentials from Azure Key Vault.")
            except Exception:
                try:
                    credentials = PrefectSecret(credentials_secret).run()
                    logger.info("Successfully loaded credentials from PrefectSecret.")
                except Exception:
                    raise CredentialError(
                        "Provided credentials secret not found in resources."
                    )
    else:
        raise CredentialError("Credentials secret not provided.")

    return credentials


@task(timeout=3600)
def adls_bulk_upload(
    file_names: List[str],
    file_name_relative_path: str = "",
    adls_file_path: str = None,
    adls_sp_credentials_secret: str = None,
    adls_overwrite: bool = True,
    timeout: int = 3600,
) -> None:
    """Function that upload files to defined path in ADLS.
    Args:
        file_names (List[str]): List of file names to generate paths.
        file_name_relative_path (str, optional): Path where to save the file locally. Defaults to ''.
        adls_file_path (str, optional): Azure Data Lake path. Defaults to None.
        adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
        adls_overwrite (bool, optional): Whether to overwrite files in the data lake. Defaults to True.
        timeout (int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
    """

    file_to_adls_task = AzureDataLakeUpload(timeout=timeout)

    for file in file_names:
        file_to_adls_task.run(
            from_path=os.path.join(file_name_relative_path, file),
            to_path=os.path.join(adls_file_path, file),
            sp_credentials_secret=adls_sp_credentials_secret,
            overwrite=adls_overwrite,
        )


@task(timeout=3600)
def anonymize_df(
    df: pd.DataFrame,
    columns: List[str],
    method: Literal["mask", "hash"] = "mask",
    value: str = "***",
    date_column: str = None,
    days: int = None,
) -> pd.DataFrame:
    """
    Function that anonymize data in the dataframe in selected columns.
    It is possible to specify the condtition, for which data older than specified number of days will be anonymized.

    Args:
        df (pd.DataFrame): Dataframe with data to anonymize.
        columns (List[str]): List of columns to anonymize.
        method (Literal["mask", "hash"], optional): Method of anonymizing data. "mask" -> replace the data with "value" arg.
            "hash" -> replace the data with the hash value of an object (using `hash()` method). Defaults to "mask".
        value (str, optional): Value to replace the data. Defaults to "***".
        date_column (str, optional): Name of the date column used to identify rows that are older than a specified number of days. Defaults to None.
        days (int, optional): The number of days beyond which we want to anonymize the data, e.g. older that 2 years can be: 2*365. Defaults to None.

    Examples:
        1. Implement "mask" method with "***" for all data in columns: ["email", "last_name", "phone"]:
            >>> anonymize_df(df=df, columns=["email", "last_name", "phone"])
        2. Implement "hash" method with in columns: ["email", "last_name", "phone"]:
            >>> anonymize_df(df=df, columns=["email", "last_name", "phone"], method = "hash")
        3. Implement "mask" method with "***" for data in columns: ["email", "last_name", "phone"], that is older than two years in "submission_date" column:
            >>> anonymize_df(df=df, columns=["email", "last_name", "phone"], date_column="submission_date", days=2*365)

    Raises:
        ValueError: If method or columns not found.

    Returns:
        pd.DataFrame: Operational dataframe with anonymized data.
    """
    if all(col in df.columns for col in columns) == False:
        raise ValueError(
            f"At least one of the following columns is not found in dataframe: {columns} or argument is not list. Provide list with proper column names."
        )

    if days and date_column:
        days_ago = datetime.now().date() - timedelta(days=days)
        df["temp_date_col"] = pd.to_datetime(df[date_column]).dt.date

        to_hash = df["temp_date_col"] < days_ago
        if any(to_hash) == False:
            logger.warning(f"No data that is older than {days} days.")
        else:
            logger.info(
                f"Data older than {days} days in {columns} columns will be anonymized."
            )
    else:
        to_hash = len(df.index) * [True]
        logger.info(
            f"The 'days' and 'date_column' arguments were not specified. All data in {columns} columns will be anonymized."
        )

    if method == "mask":
        df.loc[to_hash, columns] = value
    elif method == "hash":
        df.loc[to_hash, columns] = df.loc[to_hash, columns].apply(
            lambda x: x.apply(hash)
        )
    else:
        raise ValueError(
            f"Method not found. Use one of the available methods: 'mask', 'hash'."
        )

    df.drop(columns=["temp_date_col"], inplace=True, errors="ignore")
    return df


@task(timeout=3600)
def validate_df(df: pd.DataFrame, tests: dict = None) -> None:
    """
    Task to validate the data on DataFrame level. All numbers in the ranges are inclusive.
    tests:
        - `column_size`: dict{column: size}
        - `column_unique_values`: list[columns]
        - `column_list_to_match`: list[columns]
        - `dataset_row_count`: dict: {'min': number, 'max', number}
        - `column_match_regex`: dict: {column: 'regex'}
        - `column_sum`: dict: {column: {'min': number, 'max': number}}

    Args:
        df (pd.DataFrame): The data frame for validation.
        tests (dict, optional): Tests to apply on the data frame. Defaults to None.

    Raises:
        ValidationError: If validation failed for at least one test.
    """
    failed_tests = 0
    failed_tests_list = []

    if tests is not None:
        if "column_size" in tests:
            try:
                for k, v in tests["column_size"].items():
                    column_max_length = (
                        df.astype(str).apply(lambda s: s.str.len()).max().to_dict()
                    )
                    try:
                        if v == column_max_length[k]:
                            logger.info(f"[column_size] for {k} passed.")
                        else:
                            logger.error(
                                f"[column_size] test for {k} failed. field lenght is different than {v}"
                            )
                            failed_tests += 1
                            failed_tests_list.append("column_size error")
                    except Exception as e:
                        logger.error(f"{e}")
            except TypeError as e:
                logger.error(
                    "Please provide `column_size` parameter as dictionary {'columns': value}."
                )

        if "column_unique_values" in tests:
            for column in tests["column_unique_values"]:
                df_size = df.shape[0]
                if df[column].nunique() == df_size:
                    logger.info(
                        f"[column_unique_values] Values are unique for {column} column."
                    )
                else:
                    failed_tests += 1
                    failed_tests_list.append("column_unique_values error")
                    logger.error(
                        f"[column_unique_values] Values for {column} are not unique."
                    )

        if "column_list_to_match" in tests:
            if set(tests["column_list_to_match"]) == set(df.columns):
                logger.info(f"[column_list_to_match] passed.")
            else:
                failed_tests += 1
                failed_tests_list.append("column_list_to_match error")
                logger.error(
                    "[column_list_to_match] failed. Columns are different than expected."
                )

        if "dataset_row_count" in tests:
            row_count = len(df.iloc[:, 0])
            max_value = tests["dataset_row_count"]["max"] or 100_000_000
            min_value = tests["dataset_row_count"]["min"] or 0

            if (row_count > min_value) and (row_count < max_value):
                logger.info("[dataset_row_count] passed.")
            else:
                failed_tests += 1
                failed_tests_list.append("dataset_row_count error")
                logger.error(
                    f"[dataset_row_count] Row count ({row_count}) is not between {min_value} and {max_value}."
                )

        if "column_match_regex" in tests:
            for k, v in tests["column_match_regex"].items():
                try:
                    matches = df[k].apply(lambda x: bool(re.match(v, str(x))))
                    if all(matches):
                        logger.info(f"[column_match_regex] on {k} column passed.")
                    else:
                        failed_tests += 1
                        failed_tests_list.append("column_match_regex error")
                        logger.error(f"[column_match_regex] on {k} column failed!")
                except Exception as e:
                    failed_tests += 1
                    failed_tests_list.append("column_match_regex error")
                    logger.error(f"[column_match_regex] Error in {k} column: {e}")

        if "column_sum" in tests:
            for column, bounds in tests["column_sum"].items():
                col_sum = df[column].sum()
                min_bound = bounds["min"]
                max_bound = bounds["max"]
                if min_bound <= col_sum <= max_bound:
                    logger.info(
                        f"[column_sum] Sum of {col_sum} for {column} is within the expected range."
                    )
                else:
                    failed_tests += 1
                    failed_tests_list.append("column_sum error")
                    logger.error(
                        f"[column_sum] Sum of {col_sum} for {column} is out of the expected range - <{min_bound}:{max_bound}>"
                    )
    else:
        return "No dataframe tests to run."

    if failed_tests > 0:
        failed_tests_msg = ", ".join(failed_tests_list)
        raise ValidationError(
            f"Validation failed for {failed_tests} test/tests: {failed_tests_msg}"
        )


@task(timeout=3600, slug="check_df")
def check_if_df_empty(
    df, if_no_data_returned: Literal["fail", "warn", "skip"] = "fail"
) -> bool:
    """
    Check if a DataFrame received as a data source response is empty.
    If fail is expected , this task will finish with ENDRUN(Failed()) state.

    Args:
        df (pandas.DataFrame): The DataFrame to check.
        if_no_data_returned (Literal["fail", "warn", "skip"], optional): The action to take if no data is returned in the DataFrame. Defaults to "fail".
            Options are "fail" (default), "warn", or "skip".

    Returns:
        bool: True if the DataFrame is empty and the action is "warn", False otherwise.

    Raises:
        ENDRUN: If the DataFrame is empty and the action is "fail".

    Example:
        >>> df = pd.DataFrame()
        >>> check_if_df_empty(df, if_no_data_returned="warn")
        True
    """
    if df.empty:
        if if_no_data_returned == "warn":
            logger.warning("No data in the source response. Df empty.")
            return True
        elif if_no_data_returned == "fail":
            raise ENDRUN(state=Failed("No data in the source response. Df empty..."))
        elif if_no_data_returned == "skip":
            return False
    else:
        return False


@task(timeout=3600)
def get_flow_run_id(client: prefect.Client, flow_name: str, state: str) -> str:
    """Gets the last flow run ID based on the name of the flow and time of its run in descending order of the flow runs.

    Args:
        client (prefect.Client): The Prefect client used to execute the GraphQL query.
        flow_name (str): The name of the flow to search for.
        state (str): The state of the flow run to filter by.

    Returns:
        str: The ID of the last flow run that matches the given flow name and state.

    Raises:
        ValueError: If the given flow name cannot be found in the Prefect Cloud API.

    Example:
        >>> client = prefect.Client()
        >>> flow_name = "My Flow"
        >>> state = "SUCCESS"
        >>> get_flow_run_id(client, flow_name, state)
        "flow_run_id_12345"
    """
    # Construct the GraphQL query
    query = f"""
        {{
        flow_run(
            where: {{
                flow: {{
                    name: {{_eq: "{flow_name}"}}
                }}
                state: {{_eq: "{state}"}}
            }}
            order_by : {{end_time: desc}}
            limit : 1
        ){{
            id
        }}
        }}
        """
    # Execute the GraphQL query
    response = client.graphql(query)
    result_data = response.get("data").get("flow_run")
    if result_data:
        flow_run_id = result_data.get("id")[0]
        return flow_run_id
    else:
        raise ValueError("Given flow name cannot be found in the Prefect Cloud API")


@task(timeout=3600)
def get_task_logs(client: prefect.Client, flow_run_id: str, task_slug: str) -> List:
    """
    Retrieves the logs for a specific task in a flow run using the Prefect client and GraphQL query.

    Args:
        client (prefect.Client): The Prefect client used to execute the GraphQL query.
        flow_run_id (str): The ID of the flow run.
        task_slug (str): The slug of the task to retrieve logs for.

    Returns:
        List[Dict[str, Union[str, List[Dict[str, str]]]]]: A list of log entries for the specified task.
            Each log entry is a dictionary with 'message' and 'level' keys.

    Raises:
        ValueError: If no data is available for the given task slug.

    Example:
        >>> client = prefect.Client()
        >>> flow_run_id = "flow_run_id_12345"
        >>> task_slug = "my_task"
        >>> get_task_logs(client, flow_run_id, task_slug)
        [{'message': 'Log message 1', 'level': 'INFO'}, {'message': 'Log message 2', 'level': 'DEBUG'}]
    """
    # Construct the GraphQL query
    query = f"""
        {{
            task_run(
                where: {{
                    flow_run_id: {{_eq: "{flow_run_id}"}},
                    task: {{slug: {{_eq: "{task_slug}"}}}}
                }}
            ) {{
                state
                logs {{
                    message
                    level
                }}
            }}
        }}
    """
    # Execute the GraphQL query
    logger.info("Executing GraphQL query to get task logs.")
    response = client.graphql(query)
    result_data = response.get("data").get("task_run")
    # Extract task logs
    if result_data:
        logs = result_data[0].get("logs")
        return logs
    else:
        raise ValueError("No data available for the given task slug.")


@task(timeout=3600)
def send_email_notification(
    from_address: Union[str, Tuple],
    to_address: Union[str, List[str], List[Tuple], Tuple[str]],
    content: str,
    subject: str,
    vault_name: str,
    mail_credentials_secret: str,
    timezone: str = "Europe/Warsaw",
) -> str:
    """
    Sends an email notification using SendGrid API.

    Args:
        from_address (Union[str, Tuple]): The email address of the sender.
        to_address (Union[str, List[str], List[Tuple], Tuple[str]]): The email address(es) of the recipient(s).
        content (str): The content of the email.
        subject (str): The subject of the email.
        vault_name (str): The name of the Azure Key Vault.
        mail_credentials_secret (str): The secret name for the SendGrid API key.
        timezone (str, optional): The timezone to use for the current datetime. Defaults to "Europe/Warsaw".

    Returns:
        str: The response from the SendGrid API.

    Raises:
        Exception: If the API key is not provided.

    Example:
        >>> send_email_notification("sender@example.com", "recipient@example.com", "Hello!", "Test Email", "my-vault", "sendgrid-api-key")
        'Email sent successfully'
    """

    # Retrieve the SendGrid API key from the secret
    if mail_credentials_secret is None:
        mail_credentials_secret = PrefectSecret("SENDGRID_DEFAULT_SECRET").run()
    elif mail_credentials_secret is not None:
        credentials_str = AzureKeyVaultSecret(
            mail_credentials_secret, vault_name=vault_name
        ).run()
        api_key = json.loads(credentials_str).get("API_KEY")
    else:
        raise Exception("Please provide API KEY")

    # Get the current datetime in the specified timezone
    curr_dt = pendulum.now(tz=timezone)

    # Create the email message
    message = Mail(
        from_email=from_address,
        to_emails=to_address,
        subject=subject,
        html_content=f"<strong>{content}</strong>",
    )

    # Send the email using SendGrid API
    send_grid = SendGridAPIClient(api_key)
    response = send_grid.send(message)
    return response


@task(timeout=3600)
def search_for_msg_in_logs(logs: list, log_info: str) -> bool:
    """
    Searches for a specific message in Prefect flow or task logs.

    Args:
        logs (list): The logs to search in.
        log_info (str): The message to search for.

    Returns:
        bool: True if the message is found, False otherwise.

    Example:
        >>> logs = [
        ...     {"message": "Error occurred"},
        ...     {"message": "Warning: Invalid input"},
        ...     {"message": "Log message"}
        ... ]
        >>> search_for_msg_in_logs(logs, "Error occurred")
        True
    """
    found_msg = False

    # Iterate over each log entry
    for value in logs:
        if value.get("message") == log_info:
            found_msg = True
            break

    return found_msg
