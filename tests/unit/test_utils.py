from contextlib import nullcontext as does_not_raise
import json
import logging

import pandas as pd
import pytest

from viadot.utils import (
    _cast_df_cols,
    add_viadot_metadata_columns,
    gen_bulk_insert_query_from_df,
    get_fqn,
    handle_api_request,
    validate,
    validate_and_reorder_dfs_columns,
)


def test_single_quotes_inside():
    test_value = "a'b"
    df1 = pd.DataFrame(
        {
            "a": [
                test_value,
            ],
            "b": ["a"],
        }
    )
    test_insert_query = gen_bulk_insert_query_from_df(
        df1, table_fqn="test_schema.test_table"
    )
    test_value_escaped = "'a''b'"
    assert (
        test_insert_query
        == f"""INSERT INTO test_schema.test_table (a, b)

VALUES ({test_value_escaped}, 'a')"""
    ), test_insert_query


def test_single_quotes_outside():
    test_value = "'a'"
    df1 = pd.DataFrame(
        {
            "a": [
                test_value,
            ],
            "b": ["b"],
        }
    )
    test_insert_query = gen_bulk_insert_query_from_df(
        df1, table_fqn="test_schema.test_table"
    )
    test_value_escaped = "'''a'''"
    assert (
        test_insert_query
        == f"""INSERT INTO test_schema.test_table (a, b)

VALUES ({test_value_escaped}, 'b')"""
    ), test_insert_query


def test_double_quotes_inside():
    test_value = 'a "b"'
    df1 = pd.DataFrame(
        {
            "a": [
                test_value,
            ],
            "b": ["c"],
        }
    )
    test_insert_query = gen_bulk_insert_query_from_df(
        df1, table_fqn="test_schema.test_table"
    )
    test_value_escaped = """'a "b"'"""
    assert (
        test_insert_query
        == f"""INSERT INTO test_schema.test_table (a, b)

VALUES ({test_value_escaped}, 'c')"""
    ), test_insert_query


def test_handle_api_request():
    url = "https://api.restful-api.dev/objects"
    headers = {"content-type": "application/json"}
    item = {
        "name": "test_item",
        "data": {"color": "blue", "price": 135},
    }
    payload = json.dumps(item)

    response_post = handle_api_request(
        url=url, method="POST", headers=headers, data=payload
    )
    assert response_post.ok

    item_url = f"""{url}/{response_post.json()["id"]}"""
    response_get = handle_api_request(url=item_url, method="GET", headers=headers)
    assert response_get.ok
    assert response_get.json()["data"] == item["data"]

    response_delete = handle_api_request(url=item_url, method="DELETE", headers=headers)
    assert response_delete.ok


def test_add_viadot_metadata_columns():
    class TestingClass:
        @add_viadot_metadata_columns
        def to_df(self):
            my_dict = {"AA": [1, 1], "BB": [2, 2]}
            return pd.DataFrame(my_dict)

    testing_instance = TestingClass()
    df = testing_instance.to_df()
    assert "_viadot_source" in df.columns


def test__cast_df_cols():
    test_df = pd.DataFrame(
        {
            "bool_column": [True, False, True, False],
            "datetime_column": [
                "2023-05-25 10:30:00",
                "2023-05-20 ",
                "2023-05-15 10:30",
                "2023-05-10 10:30:00+00:00 ",
            ],
            "int_column": [5, 10, 15, 20],
            "object_column": ["apple", "banana", "melon", "orange"],
        }
    )
    test_df["datetime_column"] = pd.to_datetime(
        test_df["datetime_column"], format="mixed"
    )
    result_df = _cast_df_cols(
        test_df, types_to_convert=["datetime", "bool", "int", "object"]
    )

    assert result_df["bool_column"].dtype == pd.Int64Dtype()
    assert result_df["datetime_column"].dtype == pd.StringDtype()
    assert result_df["int_column"].dtype == pd.Int64Dtype()
    assert result_df["object_column"].dtype == pd.StringDtype()


def test_get_fqn():
    # Test with schema name.
    fqn = get_fqn(table_name="my_table", schema_name="my_schema")
    assert fqn == "my_schema.my_table"

    # Test without schema name.
    fqn = get_fqn(table_name="my_table")
    assert fqn == "my_table"


def test_validate_column_size_pass():
    df = pd.DataFrame({"col1": ["a", "bb", "ccc"]})
    tests = {"column_size": {"col1": 3}}
    with does_not_raise():
        validate(df, tests)


def test_validate_column_size_fail(caplog):
    df = pd.DataFrame({"col1": ["a", "bb", "cccc"]})
    tests = {"column_size": {"col1": 3}}
    with caplog.at_level(logging.INFO):
        validate(df, tests)
    assert "field length is different than 3" in caplog.text


def test_validate_column_unique_values_pass():
    df = pd.DataFrame({"col1": [1, 2, 3]})
    tests = {"column_unique_values": ["col1"]}
    with does_not_raise():
        validate(df, tests)


def test_validate_column_unique_values_fail(caplog):
    df = pd.DataFrame({"col1": [1, 2, 2]})
    tests = {"column_unique_values": ["col1"]}
    with caplog.at_level(logging.INFO):
        validate(df, tests)
    assert "Values for col1 are not unique." in caplog.text


def test_validate_column_list_to_match_pass():
    df = pd.DataFrame({"col1": [1], "col2": [2]})
    tests = {"column_list_to_match": ["col1", "col2"]}
    with does_not_raise():
        validate(df, tests)


def test_validate_column_list_to_match_fail(caplog):
    df = pd.DataFrame({"col1": [1]})
    tests = {"column_list_to_match": ["col1", "col2"]}
    with caplog.at_level(logging.INFO):
        validate(df, tests)
    assert "Columns are different than expected" in caplog.text


def test_validate_dataset_row_count_pass():
    df = pd.DataFrame({"col1": [1, 2, 3]})
    tests = {"dataset_row_count": {"min": 1, "max": 5}}
    with does_not_raise():
        validate(df, tests)


def test_validate_dataset_row_count_fail(caplog):
    df = pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6]})
    tests = {"dataset_row_count": {"min": 1, "max": 5}}
    with caplog.at_level(logging.INFO):
        validate(df, tests)
    assert "Row count (6) is not between 1 and 5" in caplog.text


def test_validate_column_match_regex_pass():
    df = pd.DataFrame({"col1": ["A12", "B34", "C45"]})
    tests = {"column_match_regex": {"col1": "^[A-Z][0-9]{2}$"}}
    with does_not_raise():
        validate(df, tests)


def test_validate_column_match_regex_fail(caplog):
    df = pd.DataFrame({"col1": ["A123", "B34", "C45"]})
    tests = {"column_match_regex": {"col1": "^[A-Z][0-9]{2}$"}}
    with caplog.at_level(logging.INFO):
        validate(df, tests)
    assert "[column_match_regex] on col1 column failed!" in caplog.text


def test_validate_column_sum_pass():
    df = pd.DataFrame({"col1": [1, 2, 3]})
    tests = {"column_sum": {"col1": {"min": 5, "max": 10}}}
    with does_not_raise():
        validate(df, tests)


def test_validate_column_sum_fail(caplog):
    df = pd.DataFrame({"col1": [1, 2, 3, 4]})
    tests = {"column_sum": {"col1": {"min": 5, "max": 6}}}
    with caplog.at_level(logging.INFO):
        validate(df, tests)
    assert "Sum of 10 for col1 is out of the expected range - <5:6>" in caplog.text


def test_validate_and_reorder_wrong_columns():
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame({"a": [5, 6], "c": [7, 8]})

    with pytest.raises(ValueError):  # noqa: PT011
        validate_and_reorder_dfs_columns([df1, df2])


def test_validate_and_reorder_empty_list():
    with pytest.raises(IndexError):
        validate_and_reorder_dfs_columns([])


def test_validate_and_reorder_identical_columns():
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame({"a": [5, 6], "b": [7, 8]})

    result = validate_and_reorder_dfs_columns([df1, df2])

    assert len(result) == 2
    assert list(result[0].columns) == list(df1.columns)
    assert result[0].equals(df1)
    assert list(result[1].columns) == list(df2.columns)
    assert result[1].equals(df2)


def test_validate_and_reorder_different_order_columns():
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame({"b": [7, 8], "a": [5, 6]})

    expected_df2 = pd.DataFrame({"a": [5, 6], "b": [7, 8]})
    result = validate_and_reorder_dfs_columns([df1, df2])

    assert len(result) == 2
    assert list(result[0].columns) == list(df1.columns)
    assert result[0].equals(df1)
    assert list(result[1].columns) == list(expected_df2.columns)
    assert result[1].equals(expected_df2)
