import os
import pytest
from unittest import mock

import pandas as pd
from prefect.engine import signals

from viadot.flows import ADLSToAzureSQL
from viadot.flows.adls_to_azure_sql import df_to_csv_task, check_dtypes_sort


def test_get_promoted_adls_path_csv_file():
    adls_path_file = "raw/supermetrics/adls_ga_load_times_fr_test/2021-07-14T13%3A09%3A02.997357%2B00%3A00.csv"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_file)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert (
        promoted_path
        == "conformed/supermetrics/adls_ga_load_times_fr_test/2021-07-14T13%3A09%3A02.997357%2B00%3A00.csv"
    )


def test_get_promoted_adls_path_parquet_file():
    adls_path_file = "raw/supermetrics/adls_ga_load_times_fr_test/2021-07-14T13%3A09%3A02.997357%2B00%3A00.parquet"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_file)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_file_starts_with_slash():
    adls_path_dir_starts_with_slash = "/raw/supermetrics/adls_ga_load_times_fr_test/"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir_starts_with_slash)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_dir_slash():
    adls_path_dir_slash = "raw/supermetrics/adls_ga_load_times_fr_test/"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir_slash)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_dir():
    adls_path_dir = "raw/supermetrics/adls_ga_load_times_fr_test"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_dir_starts_with_slash():
    adls_path_dir_starts_with_slash = "/raw/supermetrics/adls_ga_load_times_fr_test/"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir_starts_with_slash)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_df_to_csv_task():
    d = {"col1": ["rat", "\tdog"], "col2": ["cat", 4]}
    df = pd.DataFrame(data=d)
    assert df["col1"].astype(str).str.contains("\t")[1] == True
    task = df_to_csv_task
    task.run(df, path="result.csv", remove_tab=True)
    assert df["col1"].astype(str).str.contains("\t")[1] != True


def test_df_to_csv_task_none(caplog):
    df = None
    task = df_to_csv_task
    path = "result_none.csv"
    task.run(df, path=path, remove_tab=False)
    assert "DataFrame is None" in caplog.text
    assert os.path.isfile(path) == False


@pytest.mark.dtypes
def test_check_dtypes_sort():
    d = {"col1": ["rat", "cat"], "col2": [3, 4]}
    df = pd.DataFrame(data=d)
    dtypes = {
        "col1": "varchar(6)",
        "col2": "varchar(6)",
    }
    task = check_dtypes_sort

    n_dtypes = task.run(df=df, dtypes=dtypes)
    assert list(dtypes.keys()) == list(n_dtypes.keys())

    dtypes = {
        "col2": "varchar(6)",
        "col1": "varchar(6)",
    }
    n_dtypes = task.run(df=df, dtypes=dtypes)
    assert list(dtypes.keys()) != list(n_dtypes.keys())

    dtypes = {
        "col1": "varchar(6)",
        "col3": "varchar(6)",
    }
    try:
        n_dtypes = task.run(df=df, dtypes=dtypes)
        assert False
    except signals.FAIL:
        assert True
