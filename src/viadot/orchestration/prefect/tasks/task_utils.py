"""Utility tasks."""
import re
import json
from pathlib import Path
from typing import Any, Literal, List

import pandas as pd
from datetime import datetime, timedelta, timezone
from prefect import task
from prefect.logging import get_run_logger
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet
from viadot.exceptions import ValidationError


logger = get_run_logger()

@task
def dtypes_to_json_task(dtypes_dict: dict[str, Any], local_json_path: str) -> None:
    """Creates json file from a dictionary.

    Args:
        dtypes_dict (dict): Dictionary containing data types.
        local_json_path (str): Path to local json file.
    """
    with Path(local_json_path).open("w") as fp:
        json.dump(dtypes_dict, fp)


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
    Path(path).mkdir(parents=True, exist_ok=True)

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
    Path(path).mkdir(parents=True, exist_ok=True)

    out_df.to_parquet(path, index=False, **kwargs)


@task
def union_dfs_task(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Create one DataFrame from a list of pandas DataFrames.

    Args:
        dfs (List[pd.DataFrame]): List of pandas data frames to concat. In case of
            different size of DataFrames NaN values can appear.
    """
    return pd.concat(dfs, ignore_index=True)


@task
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

@task
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
    
    It is possible to specify the condtition, for which data older than specified 
    number of days will be anonymized.

    Args:
        df (pd.DataFrame): Dataframe with data to anonymize.
        columns (List[str]): List of columns to anonymize.
        method (Literal["mask", "hash"], optional): Method of anonymizing data. 
            "mask" -> replace the data with "value" arg.
            "hash" -> replace the data with the hash value of an object 
            (using `hash()` method). Defaults to "mask".
        value (str, optional): Value to replace the data. Defaults to "***".
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
    if all(col in df.columns for col in columns) == False:
        raise ValueError(
            f"""At least one of the following columns is not found in dataframe: 
            {columns} or argument is not list. Provide list with proper column names."""
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
            f"""The 'days' and 'date_column' arguments were not specified. All data in 
            {columns} columns will be anonymized."""
        )

    if method == "mask":
        df.loc[to_hash, columns] = value
    elif method == "hash":
        df.loc[to_hash, columns] = df.loc[to_hash, columns].apply(
            lambda x: x.apply(hash)
        )
    else:
        raise ValueError(
            "Method not found. Use one of the available methods: 'mask', 'hash'."
        )

    df.drop(columns=["temp_date_col"], inplace=True, errors="ignore")
    return df


@task
def validate_df(df: pd.DataFrame, tests: dict = None) -> None:
    """
    Task to validate the data on DataFrame level. All numbers in the ranges are 
        inclusive.
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
                                f"""[column_size] test for {k} failed. field lenght is 
                                different than {v}"""
                            )
                            failed_tests += 1
                            failed_tests_list.append("column_size error")
                    except Exception as e:
                        logger.error(f"{e}")
            except TypeError:
                logger.error(
                    """Please provide `column_size` parameter as dictionary 
                    {'columns': value}."""
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
                logger.info("[column_list_to_match] passed.")
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
                    f"""[dataset_row_count] Row count ({row_count}) is not between 
                    {min_value} and {max_value}."""
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
                        f"""[column_sum] Sum of {col_sum} for {column} is within the 
                        expected range."""
                    )
                else:
                    failed_tests += 1
                    failed_tests_list.append("column_sum error")
                    logger.error(
                        f"""[column_sum] Sum of {col_sum} for {column} is out of the 
                        expected range - <{min_bound}:{max_bound}>"""
                    )
    else:
        return "No dataframe tests to run."

    if failed_tests > 0:
        failed_tests_msg = ", ".join(failed_tests_list)
        raise ValidationError(
            f"Validation failed for {failed_tests} test/tests: {failed_tests_msg}"
        )
