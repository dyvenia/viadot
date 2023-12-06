import logging
import os

import pandas as pd
import pytest
from viadot.exceptions import APIError

from viadot.signals import SKIP
from viadot.sources import AzureSQL
from viadot.utils import (
    add_viadot_metadata_columns,
    check_if_empty_file,
    gen_bulk_insert_query_from_df,
    get_flow_last_run_date,
    get_nested_value,
    get_sql_server_table_dtypes,
    slugify,
    handle_api_response,
    union_dict,
    gen_bulk_insert_query_from_df,
)

EMPTY_CSV_PATH = "empty.csv"
EMPTY_PARQUET_PATH = "empty.parquet"


class ClassForMetadataDecorator:
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


@pytest.fixture(scope="function")
def example_dataframe():
    data = [(1, "_suffixnan", 1), (2, "Noneprefix", 0), (3, "fooNULLbar", 1, 2.34)]
    return pd.DataFrame(data, columns=["id", "name", "is_deleted", "balance"])


@pytest.fixture(scope="function")
def azure_sql():
    azure_sql = AzureSQL(config_key="AZURE_SQL")
    yield azure_sql


@pytest.fixture(scope="function")
def nested_dict():
    nested_dict = {
        "first_known_lvl": {
            "second_known_lvl": {
                "third_known_lvl": {
                    "searched_lvl": {
                        "searched_phrase_1": "First value",
                        "searched_phrase_2": None,
                        "searched_phrase_3": "Found it!",
                    }
                }
            }
        },
        "first_known_lvl_2": {
            "second_known_lvl_2": {"searched_phrase_2": "Found it_2!"}
        },
    }
    return nested_dict


def test_slugify():
    """To test slugify() function functionalities work"""
    test_string = "Text With Spaces Before Changes"
    string_after_changes = slugify(test_string)
    assert string_after_changes == "text_with_spaces_before_changes"


def test_bulk_insert_query_from_df_single_quotes_inside():
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


def test_bulk_insert_query_from_df_single_quotes_outside():
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


def test_bulk_insert_query_from_df_double_quotes_inside():
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


def test_bulk_insert_query_from_df_not_implemeted():
    TEST_VALUE = 'a "b"'
    df1 = pd.DataFrame({"a": [TEST_VALUE]})
    with pytest.raises(
        NotImplementedError,
        match="this function only handles DataFrames with at least two columns.",
    ):
        gen_bulk_insert_query_from_df(df1, table_fqn="test_schema.test_table")


def test_bulk_insert_query_from_df_full_return(example_dataframe):
    result = gen_bulk_insert_query_from_df(
        example_dataframe,
        table_fqn="users",
        chunksize=1000,
        status="APPROVED",
        address=None,
    )

    expected_result = """INSERT INTO users (id, name, is_deleted, balance, status, address)

VALUES (1, '_suffixnan', 1, NULL, 'APPROVED', NULL),
       (2, 'Noneprefix', 0, NULL, 'APPROVED', NULL),
       (3, 'fooNULLbar', 1, 2.34, 'APPROVED', NULL)"""

    assert result == expected_result


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
    df_base = ClassForMetadataDecorator().to_df()
    df_decorated = ClassForMetadataDecorator().to_df_decorated()

    assert df_base.columns.to_list() == ["a", "b"]
    assert df_decorated.columns.to_list() == ["a", "b", "_viadot_source"]
    assert df_decorated["_viadot_source"][0] == "ClassForMetadataDecorator"


def test_add_viadot_metadata_columns_with_parameter():
    df_base = ClassForMetadataDecorator().to_df()
    df_decorated = ClassForMetadataDecorator().to_df_decorated_parameter()

    assert df_base.columns.to_list() == ["a", "b"]
    assert df_decorated.columns.to_list() == ["a", "b", "_viadot_source"]
    assert df_decorated["_viadot_source"][0] == "Source_name"


def test_handle_api_response_wrong_method():
    """Test to check if ValueError is thrown when wrong method is used."""

    api_url = "https://api.api-ninjas.com/v1/randomuser"
    with pytest.raises(ValueError, match="Method not found."):
        handle_api_response(url=api_url, method="WRONG_METHOD")


def test_handle_api_response_credentials_not_provided():
    """Test to check if APIError is thrown when credentials are not provided."""

    api_url = "https://api.api-ninjas.com/v1/randomuser"
    with pytest.raises(
        APIError, match="Perhaps your account credentials need to be refreshed?"
    ):
        handle_api_response(url=api_url)


def test_handle_api_response_wrong_url():
    """Test to check if APIError is thrown when api_url is wrong."""

    api_url = "https://test.com/"
    with pytest.raises(APIError, match="failed due to connection issues."):
        handle_api_response(url=api_url)


def test_handle_api_response_unknown_error():
    """Test to check if APIError is thrown when there is something other than "url" under api_url."""

    api_url = "test_string"
    with pytest.raises(APIError, match="Unknown error"):
        handle_api_response(url=api_url)


def test_handle_api_response_return_type():
    """Test to check if the connection is successful."""

    api_url = "https://jsonplaceholder.typicode.com/posts"
    response = handle_api_response(url=api_url)
    assert response.status_code == 200


def test_get_sql_server_table_dtypes(azure_sql):
    """Checks if dtypes is generated in a good way using `get_sql_server_table_dtypes` function."""

    SCHEMA = "sandbox"
    TABLE = "test_table_dtypes"
    dtypes = {"country": "VARCHAR(100)", "sales": "INT"}

    azure_sql.create_table(
        schema=SCHEMA, table=TABLE, dtypes=dtypes, if_exists="replace"
    )

    dtypes = get_sql_server_table_dtypes(schema=SCHEMA, table=TABLE, con=azure_sql.con)
    assert isinstance(dtypes, dict)
    assert list(dtypes.keys()) == ["country", "sales"]
    assert list(dtypes.values()) == ["varchar(100)", "int"]


def test_union_dict_return():
    """Check if dictionaries are unioned in the correct way."""
    a = {"a": 1}
    b = {"b": 2}
    unioned_dict = union_dict(a, b)
    assert isinstance(unioned_dict, dict)
    assert unioned_dict == {"a": 1, "b": 2}


def test_get_nested_value_found(nested_dict):
    """Sample test checking the correctness of the function when the key is found."""
    result = get_nested_value(
        nested_dict=nested_dict["first_known_lvl"]["second_known_lvl"][
            "third_known_lvl"
        ],
        levels_to_search=["searched_lvl", "searched_phrase_3"],
    )
    assert result == "Found it!"


def test_get_nested_value_not_found(nested_dict):
    """Sample test checking the correctness of the function when the key is not found."""
    result = get_nested_value(
        nested_dict["first_known_lvl"]["second_known_lvl"]["third_known_lvl"],
        levels_to_search=["searched_wrong_lvl"],
    )
    assert result is None


def test_get_nested_value_nested_dict_is_string(caplog):
    """Sample test checking the correctness of the function when non-dictionary value is provided as nested_dict."""
    with caplog.at_level(logging.WARNING):
        get_nested_value(
            nested_dict="this_is_not_dict",
            levels_to_search=["searched_phrase"],
        )
        assert "The 'nested_dict' must be a dictionary." in caplog.text


def test_get_nested_value_without_levels(nested_dict):
    """Sample test checking the correctness of the function when only `nested_value` is provided."""
    result_1 = get_nested_value(nested_dict=nested_dict)
    result_2 = get_nested_value(nested_dict=nested_dict["first_known_lvl_2"])

    assert result_1 == {
        "searched_phrase_1": "First value",
        "searched_phrase_2": None,
        "searched_phrase_3": "Found it!",
    }
    assert result_2 == {"searched_phrase_2": "Found it_2!"}
