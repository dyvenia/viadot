import os
from unittest import mock

import pandas as pd
import pytest
from prefect.engine import signals

from viadot.flows import ADLSToAzureSQL
from viadot.flows.adls_to_azure_sql import check_dtypes_sort, df_to_csv_task, len_from_dtypes, check_hardcoded_dtypes_len, get_real_sql_dtypes_from_df

test_df = pd.DataFrame(
    {
        "Date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
        "User ID": ["1a34", "1d34$56", "1a3456&8", "1d3456789!", "1s3"],  # max length = 10
        "Web ID": ["4321", "1234$56", "123", "0", "12"],  # max length = 7
        "User name": ["Ada", "aaaaadAA", "Adulkaaa", "A", " "],  # max length = 8
        "User country": ["Poland", "USA", "Norway", "USA", "USA"],  # max length = 6
        "All Users": [1234, 123456, 12345678, 123456789, 123],
        "Age": [0, 12, 123, 89, 23],
        "Last varchar": ["Last", " ", "varchar", "of this ", "df"],  # max length =8
    }
)
Real_Sql_Dtypes = {
    "Date": "DATE",
    "User ID": "VARCHAR(10)",
    "Web ID": "VARCHAR(7)",
    "User name": "VARCHAR(8)",
    "User country": "VARCHAR(6)",
    "All Users": "INT",
    "Age": "INT",
    "Last varchar": "VARCHAR(8)",
}

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


def test_get_real_sql_dtypes_from_df():
    assert get_real_sql_dtypes_from_df(test_df) == Real_Sql_Dtypes


def test_len_from_dtypes():
    real_df_lengths = {
        "Date": "DATE",
        "User ID": 10,
        "Web ID": 7,
        "User name": 8,
        "User country": 6,
        "All Users": "INT",
        "Age": "INT",
        "Last varchar": 8,
    }
    assert len_from_dtypes(Real_Sql_Dtypes) == real_df_lengths


def test_check_hardcoded_dtypes_len_userid(caplog):
    smaller_dtype_userid = {
        "Date": "DateTime",
        "User ID": "varchar(1)",
        "Web ID": "varchar(10)",
        "User name": "varchar(10)",
        "User country": "varchar(10)",
        "All Users": "int",
        "Age": "int",
        "Last varchar": "varchar(10)",
    }
    with pytest.raises(ValueError):
        check_hardcoded_dtypes_len(test_df, smaller_dtype_userid)
        assert (
            "The length of the column User ID is too big, some data could be lost. Please change the length of the provided dtypes to 10"
            in caplog.text
        )


def test_check_hardcoded_dtypes_len_usercountry(caplog):
    smaller_dtype_usercountry = {
        "Date": "DateTime",
        "User ID": "varchar(10)",
        "Web ID": "varchar(10)",
        "User name": "varchar(10)",
        "User country": "varchar(5)",
        "All Users": "int",
        "Age": "int",
        "Last varchar": "varchar(10)",
    }
    with pytest.raises(ValueError):
        check_hardcoded_dtypes_len(test_df, smaller_dtype_usercountry)
        assert (
            "The length of the column User country is too big, some data could be lost. Please change the length of the provided dtypes to 6"
            in caplog.text
        )


def test_check_hardcoded_dtypes_len():
    good_dtypes = {
        "Date": "DateTime",
        "User ID": "varchar(10)",
        "Web ID": "varchar(10)",
        "User name": "varchar(10)",
        "User country": "varchar(10)",
        "All Users": "int",
        "Age": "int",
        "Last varchar": "varchar(10)",
    }
    assert check_hardcoded_dtypes_len(test_df, good_dtypes) == None