from viadot.utils import (
    gen_bulk_insert_query_from_df,
    add_viadot_metadata_columns,
    handle_api_request,
    _cast_df_cols,
)
import pandas as pd
import json


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
            df = pd.DataFrame(my_dict)
            return df

    testing_instance = TestingClass()
    df = testing_instance.to_df()
    assert "_viadot_source" in df.columns


def test___cast_df_cols():
    TEST_DF = pd.DataFrame(
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
    TEST_DF["datetime_column"] = pd.to_datetime(TEST_DF["datetime_column"])
    result_df = _cast_df_cols(TEST_DF)
    assert result_df["bool_column"].dtype == pd.Int64Dtype()
    assert result_df["datetime_column"].dtype == pd.StringDtype()
    assert result_df["int_column"].dtype == pd.Int64Dtype()
    assert result_df["object_column"].dtype == pd.StringDtype()
