import pandas as pd
import pytest
import logging
import os

from viadot.utils import gen_bulk_insert_query_from_df, check_if_empty_file
from viadot.signals import SKIP


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


def test_check_if_empty_file(caplog):
    with open("empty.csv", "w"):
        pass

    with caplog.at_level(logging.WARNING):
        check_if_empty_file(path="empty.csv", if_empty="warn", file_extension=".csv")
        assert "The input file is empty." in caplog.text
    with pytest.raises(ValueError):
        check_if_empty_file(path="empty.csv", if_empty="fail", file_extension=".csv")
    with pytest.raises(SKIP):
        check_if_empty_file(path="empty.csv", if_empty="skip", file_extension=".csv")

    os.remove("empty.csv")


def test_check_if_empty_file_one_column(caplog):
    df = pd.DataFrame(columns=["_viadot_downloaded_at_utc"], index=[0])
    df.to_csv("empty.csv", index=False)

    with caplog.at_level(logging.WARNING):
        check_if_empty_file(path="empty.csv", if_empty="warn", file_extension=".csv")
        assert "The input file is empty." in caplog.text
    with pytest.raises(ValueError):
        check_if_empty_file(path="empty.csv", if_empty="fail", file_extension=".csv")
    with pytest.raises(SKIP):
        check_if_empty_file(path="empty.csv", if_empty="skip", file_extension=".csv")

    os.remove("empty.csv")
