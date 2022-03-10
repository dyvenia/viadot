import pytest
import numpy as np
import os
import pandas as pd
from typing import List
from viadot.config import local_config
from typing import List, Literal
from prefect.tasks.secrets import PrefectSecret
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
from viadot.tasks import AzureSQLCreateTable
import json

from viadot.task_utils import (
    chunk_df,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    union_dfs_task,
    dtypes_to_json,
    write_to_json,
    generate_table_dtypes,
)

SCHEMA = "sandbox"
TABLE = "CRM_00"


def count_dtypes(dtypes_dict: dict = None, dtypes_to_count: List[str] = None) -> int:
    dtypes_counter = 0
    for v in dtypes_dict.values():
        if v in dtypes_to_count:
            dtypes_counter += 1
    return dtypes_counter


def test_map_dtypes_for_parquet():
    df = pd.DataFrame(
        {
            "a": {0: 55.7, 1: "Hello", 2: "Hello"},
            "b": {0: "Start", 1: "Hello", 2: "Hello"},
            "w": {0: 679, 1: "Hello", 2: "Hello"},
            "x": {0: 1, 1: 2, 2: 444},
            "y": {0: 1.5, 1: 11.97, 2: 56.999},
            "z": {0: "Start", 1: 1, 2: "2021-01-01"},
        }
    )
    dtyps_dict = df_get_data_types_task.run(df)
    sum_of_dtypes = count_dtypes(dtyps_dict, ["Object", "String"])

    df_map = df_map_mixed_dtypes_for_parquet.run(df, dtyps_dict)
    dtyps_dict_mapped = df_get_data_types_task.run(df_map)
    sum_of_mapped_dtypes = count_dtypes(dtyps_dict_mapped, ["String"])

    assert sum_of_dtypes == sum_of_mapped_dtypes


def test_chunk_df():
    df = pd.DataFrame(
        {
            "AA": [1, 2, 3, 4, 5],
            "BB": [11, 22, 33, 4, 5],
            "CC": [4, 5, 6, 4, 5],
            "DD": [44, 55, 66, 1, 2],
        }
    )
    res = chunk_df.run(df=df, size=2)
    assert len(res) == 3


def test_df_get_data_types_task():
    df = pd.DataFrame(
        {
            "a": {0: "ann", 1: "test", 2: "Hello"},
            "b": {0: 9, 1: "2021-01-01", 2: "Hello"},
            "w": {0: 679, 1: "Hello", 2: "Hello"},
            "x": {0: -1, 1: 2, 2: 444},
            "y": {0: 1.5, 1: 11.97, 2: 56.999},
            "z": {0: "2022-01-01", 1: "2021-11-01", 2: "2021-01-01"},
        }
    )
    res = df_get_data_types_task.run(df)
    assert res == {
        "a": "String",
        "b": "Object",
        "w": "Object",
        "x": "Integer",
        "y": "Float",
        "z": "Date",
    }


def test_df_to_csv():
    df = pd.DataFrame(
        {
            "a": {0: "a", 1: "b", 2: "c"},
            "b": {0: "a", 1: "b", 2: "c"},
            "w": {0: "a", 1: "b", 2: "c"},
        }
    )

    df_to_csv.run(df, "test.csv")
    result = pd.read_csv("test.csv", sep="\t")
    assert df.equals(result)
    os.remove("test.csv")


def test_df_to_parquet():
    df = pd.DataFrame(
        {
            "a": {0: "a", 1: "b", 2: "c"},
            "b": {0: "a", 1: "b", 2: "c"},
            "w": {0: "a", 1: "b", 2: "c"},
        }
    )

    df_to_parquet.run(df, "test.parquet")
    result = pd.read_parquet("test.parquet")
    assert df.equals(result)
    os.remove("test.parquet")


def test_union_dfs_task():
    df1 = pd.DataFrame(
        {
            "a": {0: "a", 1: "b", 2: "c"},
            "b": {0: "a", 1: "b", 2: "c"},
            "w": {0: "a", 1: "b", 2: "c"},
        }
    )
    df2 = pd.DataFrame(
        {
            "a": {0: "d", 1: "e"},
            "b": {0: "d", 1: "e"},
        }
    )
    list_dfs = []
    list_dfs.append(df1)
    list_dfs.append(df2)
    res = union_dfs_task.run(list_dfs)
    assert isinstance(res, pd.DataFrame)
    assert len(res) == 5


def test_dtypes_to_json():
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    dtypes_to_json.run(dtypes_dict=dtypes, local_json_path="dtypes.json")
    assert os.path.exists("dtypes.json")
    os.remove("dtypes.json")


def test_write_to_json():
    dict = {"name": "John", 1: [2, 4, 3]}
    write_to_json.run(dict, "dict.json")
    assert os.path.exists("dict.json")
    os.remove("dict.json")


def test_generate_dtypes():

    create_table_task = AzureSQLCreateTable()

    create_table_task.run(
        schema=SCHEMA,
        table=TABLE,
        dtypes={"id": "INT", "name": "VARCHAR(25)"},
        if_exists="replace",
    )

    credentials_secret = PrefectSecret("aselite_prod").run()
    vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()

    credentials_str = AzureKeyVaultSecret(
        credentials_secret, vault_name=vault_name
    ).run()
    test_dict = generate_table_dtypes.run(
        credentials=json.loads(credentials_str), table_name=TABLE
    )

    assert isinstance(test_dict, dict)
