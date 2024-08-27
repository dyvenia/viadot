from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from viadot.exceptions import APIError
from viadot.sources.customer_gauge import CustomerGauge


def test_get_json_response_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Mocking the token
    with patch.object(customer_gauge, "get_token", return_value="fake_token"):
        # Mocking the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "some_data", "cursor": 123}
        with patch("viadot.utils.handle_api_response", return_value=mock_response):
            response = customer_gauge.get_json_response(cursor=123, pagesize=100)
            assert response == {"data": "some_data", "cursor": 123}


def test_get_json_response_missing_dates():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Test for missing required date arguments
    with pytest.raises(ValueError):
        customer_gauge.get_json_response(
            date_field="date_creation", start_date=datetime(2023, 1, 1)
        )


def test_get_json_response_no_api_response():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Mocking the token
    with patch.object(customer_gauge, "get_token", return_value="fake_token"):
        # Mocking the absence of an API response
        mock_response = MagicMock()
        mock_response.json.return_value = None
        with patch("viadot.utils.handle_api_response", return_value=mock_response):
            with pytest.raises(APIError):
                customer_gauge.get_json_response(cursor=123, pagesize=100)


def test_get_cursor_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with a valid cursor
    json_response = {"data": "some_data", "cursor": {"next": 456}}

    cursor = customer_gauge.get_cursor(json_response=json_response)
    assert cursor == 456


def test_get_cursor_missing_cursor():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON without a cursor
    json_response = {"data": "some_data"}

    with pytest.raises(ValueError) as excinfo:
        customer_gauge.get_cursor(json_response=json_response)

    assert "Provided argument doesn't contain 'cursor' value" in str(excinfo.value)


def test_get_cursor_missing_next():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with a missing 'next' in 'cursor'
    json_response = {"data": "some_data", "cursor": {}}

    with pytest.raises(ValueError) as excinfo:
        customer_gauge.get_cursor(json_response=json_response)

    assert "Provided argument doesn't contain 'cursor' value" in str(excinfo.value)


def test_get_data_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with a valid "data" key
    json_response = {
        "data": [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}],
        "cursor": {"next": 123},
    }

    data = customer_gauge.get_data(json_response=json_response)
    assert data == [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]


def test_get_data_key_error():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON without the "data" key
    json_response = {"cursor": {"next": 123}}

    with pytest.raises(KeyError) as excinfo:
        customer_gauge.get_data(json_response=json_response)

    assert "'data'" in str(excinfo.value)


def test_get_data_empty_list():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with an empty "data" list
    json_response = {"data": [], "cursor": {"next": 123}}

    data = customer_gauge.get_data(json_response=json_response)
    assert data == []


def test_field_reference_unpacker_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with valid field containing dictionaries with exactly two items
    json_response = {"field": [{"key1": "value1"}, {"key2": "value2"}]}

    expected_result = {"field": {"value1": "key1", "value2": "key2"}}

    result = customer_gauge._field_reference_unpacker(
        json_response=json_response, field="field"
    )
    assert result == expected_result


def test_field_reference_unpacker_invalid_dict():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with a dictionary that does not contain exactly two items
    json_response = {
        "field": [{"key1": "value1"}, {"key2": "value2", "extra_key": "extra_value"}]
    }

    with pytest.raises(ValueError) as excinfo:
        customer_gauge._field_reference_unpacker(
            json_response=json_response, field="field"
        )

    assert (
        "Dictionary within the specified field doesn't contain exactly two items"
        in str(excinfo.value)
    )


def test_field_reference_unpacker_empty_field():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with an empty field
    json_response = {"field": []}

    expected_result = {"field": {}}

    result = customer_gauge._field_reference_unpacker(
        json_response=json_response, field="field"
    )
    assert result == expected_result


def test_field_reference_unpacker_field_not_present():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON where the specified field is not present
    json_response = {"other_field": [{"key1": "value1"}]}

    expected_result = {"other_field": [{"key1": "value1"}]}

    result = customer_gauge._field_reference_unpacker(
        json_response=json_response, field="field"
    )
    assert result == expected_result


def test_nested_dict_transformer_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with nested dictionaries to be transformed
    json_response = {"field": [{"key1": "value1"}, {"key2": "value2"}]}

    expected_result = {"field": {"1_key1": "value1", "2_key2": "value2"}}

    result = customer_gauge._nested_dict_transformer(
        json_response=json_response, field="field"
    )
    assert result == expected_result


def test_nested_dict_transformer_non_dict():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with a non-dictionary value in the field
    json_response = {"field": [{"key1": "value1"}, "not_a_dict"]}

    expected_result = {"field": {"1_key1": "value1"}}

    result = customer_gauge._nested_dict_transformer(
        json_response=json_response, field="field"
    )
    assert result == expected_result


def test_nested_dict_transformer_empty_field():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON with an empty field
    json_response = {"field": []}

    expected_result = {"field": {}}

    result = customer_gauge._nested_dict_transformer(
        json_response=json_response, field="field"
    )
    assert result == expected_result


def test_nested_dict_transformer_field_not_present():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Example JSON where the specified field is not present
    json_response = {"other_field": [{"key1": "value1"}]}

    expected_result = {"other_field": [{"key1": "value1"}]}

    result = customer_gauge._nested_dict_transformer(
        json_response=json_response, field="field"
    )
    assert result == expected_result


def test_column_unpacker_field_reference_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    json_list = [{"field1": [{"key1": "value1"}]}, {"field1": [{"key2": "value2"}]}]

    # Mocking _field_reference_unpacker method
    with patch.object(
        customer_gauge,
        "_field_reference_unpacker",
        side_effect=lambda x, f: {k: v for d in x[f] for k, v in d.items()},
    ) as mock_unpacker:
        result = customer_gauge.column_unpacker(
            json_list=json_list, unpack_by_field_reference_cols=["field1"]
        )

    expected_result = [{"field1": {"key1": "value1"}}, {"field1": {"key2": "value2"}}]
    assert result == expected_result
    mock_unpacker.assert_called_once_with(json_list[0], "field1")


def test_column_unpacker_nested_dict_transformer_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    json_list = [
        {"field2": [{"keyA": "valueA"}, {"keyB": "valueB"}]},
        {"field2": [{"keyC": "valueC"}]},
    ]

    # Mocking _nested_dict_transformer method
    with patch.object(
        customer_gauge,
        "_nested_dict_transformer",
        side_effect=lambda x, f: {
            f"{i+1}_{k}": v for i, d in enumerate(x[f]) for k, v in d.items()
        },
    ) as mock_transformer:
        result = customer_gauge.column_unpacker(
            json_list=json_list, unpack_by_nested_dict_transformer=["field2"]
        )

    expected_result = [
        {"field2": {"1_keyA": "valueA", "2_keyB": "valueB"}},
        {"field2": {"1_keyC": "valueC"}},
    ]
    assert result == expected_result
    mock_transformer.assert_called_once_with(json_list[0], "field2")


def test_column_unpacker_duplicate_columns():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    with pytest.raises(ValueError):
        customer_gauge.column_unpacker(
            json_list=[{}],
            unpack_by_field_reference_cols=["field1"],
            unpack_by_nested_dict_transformer=["field1"],
        )


def test_column_unpacker_none_json_list():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    with pytest.raises(ValueError, match="Input 'json_list' is required."):
        customer_gauge.column_unpacker(
            json_list=None, unpack_by_field_reference_cols=["field1"]
        )


def test_column_unpacker_non_existing_columns():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    json_list = [{"field1": [{"key1": "value1"}]}]

    result = customer_gauge.column_unpacker(
        json_list=json_list,
        unpack_by_field_reference_cols=["field1"],
        unpack_by_nested_dict_transformer=["non_existing_field"],
    )

    expected_result = [{"field1": {"key1": "value1"}}]
    assert result == expected_result


def test_flatten_json_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    json_response = {
        "field1": {"subfield1": "value1", "subfield2": {"subsubfield1": "value2"}},
        "field2": "value3",
    }

    expected_result = {
        "field1_subfield1": "value1",
        "field1_subfield2_subsubfield1": "value2",
        "field2": "value3",
    }

    result = customer_gauge.flatten_json(json_response)
    assert result == expected_result


def test_flatten_json_type_error():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    with pytest.raises(TypeError, match="Input must be a dictionary."):
        # Incorrect input type (string instead of a dictionary)
        customer_gauge.flatten_json({"invalid": "value", "not_a_dict": "string"})


def test_flatten_json_empty_dict():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    json_response = {}

    expected_result = {}
    result = customer_gauge.flatten_json(json_response)
    assert result == expected_result


def test_square_brackets_remover_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Sample DataFrame with square brackets
    df = pd.DataFrame({"col1": ["[value1]", "[value2]"], "col2": ["text", "[text]"]})

    expected_df = pd.DataFrame({"col1": ["value1", "value2"], "col2": ["text", "text"]})

    result_df = customer_gauge.square_brackets_remover(df)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_square_brackets_remover_empty_dataframe():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Empty DataFrame
    df = pd.DataFrame()

    expected_df = pd.DataFrame()
    result_df = customer_gauge.square_brackets_remover(df)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_square_brackets_remover_no_brackets():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # DataFrame without square brackets
    df = pd.DataFrame({"col1": ["value1", "value2"], "col2": ["text", "text"]})

    expected_df = pd.DataFrame({"col1": ["value1", "value2"], "col2": ["text", "text"]})

    result_df = customer_gauge.square_brackets_remover(df)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_square_brackets_remover_type_error():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    with pytest.raises(
        AttributeError, match=".*'DataFrame' object has no attribute 'map'.*"
    ):
        # Incorrect input type (list instead of DataFrame)
        customer_gauge.square_brackets_remover(["not", "a", "dataframe"])


def test_drivers_cleaner_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Sample string with characters to be removed
    drivers = "{'label: Driver A'}{'label: Driver B'}"
    expected_cleaned_drivers = "Driver ADriver B"

    result = customer_gauge._drivers_cleaner(drivers)
    assert result == expected_cleaned_drivers


def test_drivers_cleaner_none_input():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # None input should return an empty string
    result = customer_gauge._drivers_cleaner(None)
    assert result == ""


def test_drivers_cleaner_no_formatting_needed():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"}
    )

    # Data without any characters to remove
    drivers = "Driver A, Driver B"
    expected_cleaned_drivers = "Driver A, Driver B"

    result = customer_gauge._drivers_cleaner(drivers)
    assert result == expected_cleaned_drivers


def test_to_df_success():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"},
        total_load=True,
        pagesize=100,
        anonymize=False,
    )

    # Mocking the methods used in to_df
    with patch.object(
        customer_gauge, "get_json_response"
    ) as mock_get_json_response, patch.object(
        customer_gauge, "get_cursor"
    ) as mock_get_cursor, patch.object(
        customer_gauge, "get_data"
    ) as mock_get_data, patch.object(
        customer_gauge, "column_unpacker"
    ) as mock_column_unpacker, patch.object(
        customer_gauge, "flatten_json"
    ) as mock_flatten_json, patch.object(
        customer_gauge, "square_brackets_remover"
    ) as mock_square_brackets_remover, patch.object(
        customer_gauge, "_drivers_cleaner"
    ) as mock_drivers_cleaner:
        # Setup mock return values
        mock_get_json_response.return_value = {
            "data": [{"drivers": "{'label: Driver A'}"}],
            "cursor": {"next": 2},
        }
        mock_get_cursor.return_value = 2
        mock_get_data.return_value = [{"drivers": "{'label: Driver A'}"}]
        mock_column_unpacker.return_value = [{"drivers": "Driver A"}]
        mock_flatten_json.side_effect = lambda x: x  # Identity function for simplicity
        mock_square_brackets_remover.return_value = pd.DataFrame(
            [{"drivers": "Driver A"}]
        )
        mock_drivers_cleaner.return_value = "Driver A"

        # Call to_df and check results
        df = customer_gauge.to_df()

        # Assertions
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "drivers" in df.columns
        assert df["drivers"].iloc[0] == "Driver A"


def test_to_df_no_data():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"},
        total_load=False,
        pagesize=100,
        anonymize=False,
    )

    # Mocking the methods used in to_df
    with patch.object(
        customer_gauge, "get_json_response"
    ) as mock_get_json_response, patch.object(
        customer_gauge, "get_cursor"
    ) as mock_get_cursor, patch.object(customer_gauge, "get_data") as mock_get_data:
        # Setup mock return values
        mock_get_json_response.return_value = {"data": [], "cursor": {"next": None}}
        mock_get_cursor.return_value = None
        mock_get_data.return_value = []

        # Call to_df and check results
        df = customer_gauge.to_df()

        # Assertions
        assert isinstance(df, pd.DataFrame)
        assert df.empty


def test_to_df_with_validation_and_anonymization():
    customer_gauge = CustomerGauge(
        credentials={"client_id": "test", "client_secret": "test"},
        total_load=True,
        pagesize=100,
        anonymize=True,
        columns_to_anonymize=["drivers"],
        anonymize_method="mask",
        anonymize_value="***",
        validate_df_dict={"test": "value"},
    )

    # Mocking the methods used in to_df
    with patch.object(
        customer_gauge, "get_json_response"
    ) as mock_get_json_response, patch.object(
        customer_gauge, "get_cursor"
    ) as mock_get_cursor, patch.object(
        customer_gauge, "get_data"
    ) as mock_get_data, patch.object(
        customer_gauge, "column_unpacker"
    ) as mock_column_unpacker, patch.object(
        customer_gauge, "flatten_json"
    ) as mock_flatten_json, patch.object(
        customer_gauge, "square_brackets_remover"
    ) as mock_square_brackets_remover, patch.object(
        customer_gauge, "_drivers_cleaner"
    ) as mock_drivers_cleaner, patch(
        "viadot.utils.validate_df"
    ) as mock_validate_df, patch(
        "viadot.orchestration.prefect.tasks.task_utils.anonymize_df"
    ) as mock_anonymize_df:
        # Setup mock return values
        mock_get_json_response.return_value = {
            "data": [{"drivers": "{'label: Driver A'}"}],
            "cursor": {"next": 2},
        }
        mock_get_cursor.return_value = 2
        mock_get_data.return_value = [{"drivers": "{'label: Driver A'}"}]
        mock_column_unpacker.return_value = [{"drivers": "Driver A"}]
        mock_flatten_json.side_effect = lambda x: x  # Identity function for simplicity
        mock_square_brackets_remover.return_value = pd.DataFrame(
            [{"drivers": "Driver A"}]
        )
        mock_drivers_cleaner.return_value = "Driver A"
        mock_anonymize_df.return_value = pd.DataFrame([{"drivers": "***"}])

        # Call to_df and check results
        df = customer_gauge.to_df()

        # Assertions
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "drivers" in df.columns
        assert df["drivers"].iloc[0] == "***"
        mock_validate_df.assert_called_once_with(df=df, tests={"test": "value"})
        mock_anonymize_df.assert_called_once_with(
            df=pd.DataFrame([{"drivers": "Driver A"}]),
            columns=["drivers"],
            method="mask",
            value="***",
            date_column=None,
            days=None,
        )
