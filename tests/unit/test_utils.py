import logging
import os

import pandas as pd
import pytest

from viadot.signals import SKIP
from viadot.utils import (
    check_if_empty_file,
    gen_bulk_insert_query_from_df,
    add_viadot_metadata_columns,
)

EMPTY_CSV_PATH = "empty.csv"
EMPTY_PARQUET_PATH = "empty.parquet"


class ClassForDecorator:
    source = "Source_name"

    def __init__(self):
        self.df = pd.DataFrame({"a": [123], "b": ["abc"]})

    def to_df(self):
        return self.df

    @add_viadot_metadata_columns()
    def to_df_decorated(self):
        return self.df

    @add_viadot_metadata_columns(source)
    def to_df_decorated_parameter(self):
        return self.df


def test_single_quotes_inside():
    TEST_VALUE = "a'b"
    df1 = pd.DataFrame(
        {
            "a": [
                TEST_VALUE,
            ],
            "b": ["a"],
        }
    )
    test_insert_query = gen_bulk_insert_query_from_df(
        df1, table_fqn="test_schema.test_table"
    )
    TEST_VALUE_ESCAPED = "'a''b'"
    assert (
        test_insert_query
        == f"""INSERT INTO test_schema.test_table (a, b)

VALUES ({TEST_VALUE_ESCAPED}, 'a')"""
    ), test_insert_query


def test_single_quotes_outside():
    TEST_VALUE = "'a'"
    df1 = pd.DataFrame(
        {
            "a": [
                TEST_VALUE,
            ],
            "b": ["b"],
        }
    )
    test_insert_query = gen_bulk_insert_query_from_df(
        df1, table_fqn="test_schema.test_table"
    )
    TEST_VALUE_ESCAPED = "'''a'''"
    assert (
        test_insert_query
        == f"""INSERT INTO test_schema.test_table (a, b)

VALUES ({TEST_VALUE_ESCAPED}, 'b')"""
    ), test_insert_query


def test_double_quotes_inside():
    TEST_VALUE = 'a "b"'
    df1 = pd.DataFrame(
        {
            "a": [
                TEST_VALUE,
            ],
            "b": ["c"],
        }
    )
    test_insert_query = gen_bulk_insert_query_from_df(
        df1, table_fqn="test_schema.test_table"
    )
    TEST_VALUE_ESCAPED = """'a "b"'"""
    assert (
        test_insert_query
        == f"""INSERT INTO test_schema.test_table (a, b)

VALUES ({TEST_VALUE_ESCAPED}, 'c')"""
    ), test_insert_query


def test_check_if_empty_file_csv(caplog):
    with open(EMPTY_CSV_PATH, "w"):
        pass

    with caplog.at_level(logging.WARNING):
        check_if_empty_file(path=EMPTY_CSV_PATH, if_empty="warn")
        assert f"Input file - '{EMPTY_CSV_PATH}' is empty." in caplog.text
    with pytest.raises(ValueError):
        check_if_empty_file(path=EMPTY_CSV_PATH, if_empty="fail")
    with pytest.raises(SKIP):
        check_if_empty_file(path=EMPTY_CSV_PATH, if_empty="skip")

    os.remove(EMPTY_CSV_PATH)


def test_check_if_empty_file_parquet(caplog):
    with open(EMPTY_PARQUET_PATH, "w"):
        pass

    with caplog.at_level(logging.WARNING):
        check_if_empty_file(path=EMPTY_PARQUET_PATH, if_empty="warn")
        assert f"Input file - '{EMPTY_PARQUET_PATH}' is empty." in caplog.text
    with pytest.raises(ValueError):
        check_if_empty_file(path=EMPTY_PARQUET_PATH, if_empty="fail")
    with pytest.raises(SKIP):
        check_if_empty_file(path=EMPTY_PARQUET_PATH, if_empty="skip")

    os.remove(EMPTY_PARQUET_PATH)


def test_check_if_empty_file_no_data(caplog):
    df = pd.DataFrame({"col1": []})
    df.to_parquet(EMPTY_PARQUET_PATH)
    with caplog.at_level(logging.WARNING):
        check_if_empty_file(path=EMPTY_PARQUET_PATH, if_empty="warn")
        assert f"Input file - '{EMPTY_PARQUET_PATH}' is empty." not in caplog.text


def test_add_viadot_metadata_columns_base():
    df_base = ClassForDecorator().to_df()
    df_decorated = ClassForDecorator().to_df_decorated()

    assert df_base.columns.to_list() == ["a", "b"]
    assert df_decorated.columns.to_list() == ["a", "b", "_viadot_source"]
    assert df_decorated["_viadot_source"][0] == "ClassForDecorator"


def test_add_viadot_metadata_columns_with_parameter():
    df_base = ClassForDecorator().to_df()
    df_decorated = ClassForDecorator().to_df_decorated_parameter()

    assert df_base.columns.to_list() == ["a", "b"]
    assert df_decorated.columns.to_list() == ["a", "b", "_viadot_source"]
    assert df_decorated["_viadot_source"][0] == "Source_name"
