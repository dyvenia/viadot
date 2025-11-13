from contextlib import nullcontext as does_not_raise
import json
import logging
import re

import pandas as pd
import pendulum
import pytest

from viadot.signals import SKIP
from viadot.utils import (
    _cast_df_cols,
    add_viadot_metadata_columns,
    df_clean_column,
    df_converts_bytes_to_int,
    gen_bulk_insert_query_from_df,
    get_fqn,
    handle_api_request,
    handle_if_empty,
    parse_dates,
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
                "2023-05-20 10:00:00",
                "2023-05-15 10:30:00",
                "2023-05-10 10:30:00",
            ],
            "int_column": [5, 10, 15, 20],
            "object_column": ["apple", "banana", "melon", "orange"],
        }
    )
    test_df["datetime_column"] = pd.to_datetime(
        test_df["datetime_column"], infer_datetime_format=True
    )
    result_df = _cast_df_cols(
        test_df, types_to_convert=["datetime", "bool", "int", "object"]
    )

    assert result_df["bool_column"].dtype == pd.Int64Dtype()
    assert pd.api.types.is_object_dtype(result_df["datetime_column"])
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

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(
            Exception, match=r"Validation failed for 1 test\(s\): column_size error"
        ),
    ):
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

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(
            Exception,
            match=r"Validation failed for 1 test\(s\): column_unique_values error",
        ),
    ):
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

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(
            Exception,
            match=r"Validation failed for 1 test\(s\): column_list_to_match error",
        ),
    ):
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

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(
            Exception,
            match=r"Validation failed for 1 test\(s\): dataset_row_count error",
        ),
    ):
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

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(
            Exception,
            match=r"Validation failed for 1 test\(s\): column_match_regex error",
        ),
    ):
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

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(
            Exception, match=r"Validation failed for \d+ test\(s\): column_sum error"
        ),
    ):
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


def test_df_converts_bytes_to_int():
    df_bytes = pd.DataFrame(
        {
            "A": [b"1", b"2", b"3"],
            "B": [b"4", b"5", b"6"],
            "C": ["no change", "still no change", "me neither"],
        }
    )

    result = df_converts_bytes_to_int(df_bytes)

    expected = pd.DataFrame(
        {
            "A": [1, 2, 3],
            "B": [4, 5, 6],
            "C": ["no change", "still no change", "me neither"],
        }
    )

    pd.testing.assert_frame_equal(result, expected)

    df_no_bytes = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    result_no_bytes = df_converts_bytes_to_int(df_no_bytes)

    pd.testing.assert_frame_equal(result_no_bytes, df_no_bytes)


def test_df_clean_column():
    df_dirty = pd.DataFrame(
        {
            "A": ["Hello\tWorld", "Goodbye\nWorld"],
            "B": ["Keep\nIt\tClean", "Just\tTest"],
        }
    )

    cleaned_df = df_clean_column(df_dirty, columns_to_clean=["A"])

    expected_cleaned_df = pd.DataFrame(
        {"A": ["HelloWorld", "GoodbyeWorld"], "B": ["Keep\nIt\tClean", "Just\tTest"]}
    )

    pd.testing.assert_frame_equal(cleaned_df, expected_cleaned_df)

    cleaned_all_df = df_clean_column(df_dirty)

    expected_all_cleaned_df = pd.DataFrame(
        {"A": ["HelloWorld", "GoodbyeWorld"], "B": ["KeepItClean", "JustTest"]}
    )

    pd.testing.assert_frame_equal(cleaned_all_df, expected_all_cleaned_df)


def test_handle_if_empty_warn(caplog):
    """Test that handle_if_empty logs a warning when if_empty='warn'."""
    message = "Empty input detected."
    caplog.set_level(logging.WARNING)

    handle_if_empty(if_empty="warn", message=message)

    assert message in caplog.text
    assert "WARNING" in caplog.text


def test_handle_if_empty_skip():
    """Test that handle_if_empty raises SKIP exception when if_empty='skip'."""
    message = "Skipping due to empty input."

    with pytest.raises(SKIP, match=message):
        handle_if_empty(if_empty="skip", message=message)


def test_handle_if_empty_fail():
    """Test that handle_if_empty raises ValueError when if_empty='fail'."""
    message = "Failed due to empty input."

    with pytest.raises(ValueError, match=message):
        handle_if_empty(if_empty="fail", message=message)


def test_handle_if_empty_default_logger(caplog):
    """Test that handle_if_empty uses default logger when none is provided."""
    message = "Empty input detected with default logger."
    caplog.set_level(logging.WARNING)

    handle_if_empty(if_empty="warn", message=message)

    assert message in caplog.text
    assert "WARNING" in caplog.text


def test_handle_if_empty_invalid_value():
    """Test that handle_if_empty raises ValueError when an invalid if_empty value is provided."""  # noqa: W505
    invalid_value = "replace"
    expected_msg = f"Invalid value for if_empty: {invalid_value}. Allowed values are ['warn', 'skip', 'fail']."
    escaped_msg = re.escape(expected_msg)  # Escape regex special chars

    with pytest.raises(ValueError, match=escaped_msg):
        handle_if_empty(if_empty=invalid_value, message="This should fail.")  # type: ignore


def test_parse_dates_single_date():
    date_filter_keyword = "<<yesterday>>"
    result = parse_dates(date_filter=date_filter_keyword)

    assert result == pendulum.yesterday().date()

    date_filter_date = "<<pendulum.yesterday()>>"
    result = parse_dates(date_filter=date_filter_date)

    assert result == pendulum.yesterday().date()


def test_parse_dates_date_range():
    date_filter_yesterday = "<<yesterday>>"
    date_filter_today = "<<today>>"

    start_date, end_date = parse_dates(
        date_filter=(date_filter_yesterday, date_filter_today)
    )

    assert start_date == pendulum.yesterday().date()
    assert end_date == pendulum.today().date()


def test_parse_dates_none():
    date_filter = None
    result = parse_dates(date_filter=date_filter)

    assert result is None


def test_parse_dates_wrong_input():
    date_filter = ["<<pendulum.today()>>"]

    with pytest.raises(
        ValueError,
        match="date_filter must be a string, a tuple of exactly 2 dates, or None.",
    ):
        parse_dates(date_filter=date_filter)
