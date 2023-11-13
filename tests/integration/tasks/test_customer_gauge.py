import pandas as pd
import pytest

from viadot.tasks import CustomerGaugeToDF

ENDPOINT = "responses"
CG = CustomerGaugeToDF(endpoint=ENDPOINT)
CUR = 185000
PAGESIZE = 1000

DATA_JSON = {'contact': {'first_name': '***', 'last_name': '***'},
   'number_customer': 266,
   'date_email_sent': '2018-02-05 10:42:28',
   'properties': [{'field': 'Postal Code', 'reference': '999'},
    {'field': 'City', 'reference': 'Eldorado'},
    {'field': 'Currency', 'reference': None},
    {'field': 'Item Quantity', 'reference': '7'},
    {'field': 'PostingDate', 'reference': '2018-01-10 00:00:00'}],
   'custom_fields': [{'field': 'Assignment_ID', 'reference': None}],
   'drivers': [{'label': 'Product Quality and Product Performance'},
    {'label': 'Function and Design'},
    {'label': 'Value for Money'},
    {'label': 'Packaging'}]}

RAW_JSON  = {'data': [{'contact': {'first_name': '***', 'last_name': '***'},
   'number_customer': 266,
   'date_email_sent': '2018-02-05 10:42:28',
   'properties': [{'field': 'Postal Code', 'reference': '999'},
    {'field': 'City', 'reference': 'Eldorado'},
    {'field': 'Currency', 'reference': None},
    {'field': 'Item Quantity', 'reference': '7'},
    {'field': 'PostingDate', 'reference': '2018-01-10 00:00:00'}],
   'custom_fields': [{'field': 'Assignment_ID', 'reference': None}],
   'drivers': [{'label': 'Product Quality and Product Performance'},
    {'label': 'Function and Design'},
    {'label': 'Value for Money'},
    {'label': 'Packaging'}]},
  {'contact': {'first_name': '***', 'last_name': '***'},
   'number_customer': 206,
   'date_email_sent': '2018-02-05 10:41:01',
   'properties': [{'field': 'Postal Code', 'reference': '0000'},
    {'field': 'City', 'reference': 'Neverland'},
    {'field': 'Currency', 'reference': None},
    {'field': 'Item Quantity', 'reference': '1'},
    {'field': 'PostingDate', 'reference': '2018-01-26 00:00:00'}],
   'custom_fields': [{'field': 'Assignment_ID', 'reference': None}],
   'drivers': [{'label': 'The website of the online shop (overall impression)'},
    {'label': 'Waiting period'}]}],
 'cursor': {'next': 37}}

WRONG_DATA  = {'cols':[
    {'field': 'City', 'reference': 'Eldorado'},
    {'field': 'Currency', 'reference': None},
    {'field': 'Item Quantity', 'reference': '7'},
    {'field': 'PostingDate', 'reference': '2018-01-10 00:00:00'}]}

@pytest.mark.looping_api_calls
def test_customer_gauge_to_df_loop():
    """
    Test the 'run' method with looping API calls.
    """
    df = CG.run(total_load=True, cursor=CUR, pagesize=PAGESIZE)

    assert isinstance(df, pd.DataFrame)
    assert len(df) > PAGESIZE


@pytest.mark.get_data
def test_get_data():
    """
    Test the 'get_data' method with valid JSON data.
    """
    json_data = CG.get_data(RAW_JSON)
    assert isinstance(json_data, list)


@pytest.mark.get_data_error
def test_get_data_error_raising():
    """
    Test the 'get_data' method with invalid JSON data that raises a KeyError.
    """
    with pytest.raises(KeyError):
        CG.get_data(WRONG_DATA)


@pytest.mark.field_reference_unpacker_success
def test_field_reference_unpacker():
    """
    Test the '_field_reference_unpacker' method with valid data. It should unpack and modify dictionaries within the specified field and return the expected result.
    """ 
    data = DATA_JSON.copy()
    field = 'properties'
    expected_result = {
        'contact': {'first_name': '***', 'last_name': '***'},
        'number_customer': 266,
        'date_email_sent': '2018-02-05 10:42:28',
        'properties': {'Postal Code': '999',
        'City': 'Eldorado',
        'Currency': None,
        'Item Quantity': '7',
        'PostingDate': '2018-01-10 00:00:00'},
        'custom_fields': [{'field': 'Assignment_ID', 'reference': None}],
        'drivers': [{'label': 'Product Quality and Product Performance'},
        {'label': 'Function and Design'},
        {'label': 'Value for Money'},
        {'label': 'Packaging'}]
    }

    result = CG._field_reference_unpacker(json_response=data, field=field)

    assert result == expected_result

@pytest.mark.field_reference_unpacker_value_error
def test_field_reference_unpacker_invalid_data_format():
    """
    Test the '_field_reference_unpacker' method with invalid data format that should raise a ValueError. It should raise a ValueError exception.
    """
    data = DATA_JSON.copy()
    field='contact'
    with pytest.raises(ValueError, match=r"Dictionary within the specified field doesn't contain exactly two items."):
        CG._field_reference_unpacker(json_response=data, field=field)
 

@pytest.mark.field_reference_unpacker_key_error
def test_field_reference_unpacker_missing_field():
    """
    Test the '_field_reference_unpacker' method with a missing field that should raise a KeyError. It should raise a KeyError exception.
    """
    data = DATA_JSON.copy()
    field = "non_existent_field"
    with pytest.raises(KeyError):
        CG._field_reference_unpacker(json_response=data, field=field)


@pytest.mark.nested_dict_transformer_success
def test_nested_dict_transformer():
    """
    Test the '_nested_dict_transformer' method with valid data. It should modify nested dictionaries within the specified field and return the expected result.
    """
    data = DATA_JSON.copy()
    field = 'drivers'
    expected_result = {'contact': {'first_name': '***', 'last_name': '***'},
        'number_customer': 266,
        'date_email_sent': '2018-02-05 10:42:28',
        'properties': [{'field': 'Postal Code', 'reference': '999'},
        {'field': 'City', 'reference': 'Eldorado'},
        {'field': 'Currency', 'reference': None},
        {'field': 'Item Quantity', 'reference': '7'},
        {'field': 'PostingDate', 'reference': '2018-01-10 00:00:00'}],
        'custom_fields': [{'field': 'Assignment_ID', 'reference': None}],
        'drivers': {'1_label': 'Product Quality and Product Performance',
        '2_label': 'Function and Design',
        '3_label': 'Value for Money',
        '4_label': 'Packaging'}}

    result = CG._nested_dict_transformer(json_response=data, field=field)

    assert result == expected_result


@pytest.mark.nested_dict_transformer_type_error
def test_nested_dict_transformer_invalid_data_format():
    """
    Test the '_nested_dict_transformer' method with invalid data format. It should return the same data without modification.
    """
    data = DATA_JSON.copy()
    field='number_customer'
    result = CG._nested_dict_transformer(json_response=data, field=field)

    assert result == data


@pytest.mark.nested_dict_transformer_key_error
def test_nested_dict_transformer_missing_field():
    """
    Test the '_nested_dict_transformer' method with a missing field that should raise a KeyError.
    """
    data = DATA_JSON.copy()
    field = "non_existent_field"
    with pytest.raises(KeyError):
        CG._nested_dict_transformer(json_response=data, field=field)


@pytest.mark.column_unpacker_success
def test_column_unpacker_success_method1_and_method2():
    """
    Test the 'column_unpacker' method with valid data and both Method 1 and Method 2 columns specified. It should return the expected result.
    """
    data = RAW_JSON['data'].copy()
    unpack_by_field_reference_cols = ['properties']
    unpack_by_nested_dict_transformer = ['drivers']

    expected_result = [
        {'contact': {'first_name': '***', 'last_name': '***'},
        'number_customer': 266,
        'date_email_sent': '2018-02-05 10:42:28',
        'properties': {
            'Postal Code': '999',
            'City': 'Eldorado',
            'Currency': None,
            'Item Quantity': '7',
            'PostingDate': '2018-01-10 00:00:00'
            },
        'custom_fields': [{'field': 'Assignment_ID', 'reference': None}],
        'drivers': {'1_label': 'Product Quality and Product Performance',
        '2_label': 'Function and Design',
        '3_label': 'Value for Money',
        '4_label': 'Packaging'}},
        {'contact': {'first_name': '***', 'last_name': '***'},
        'number_customer': 206,
        'date_email_sent': '2018-02-05 10:41:01',
        'properties': {
            'Postal Code': '0000',
            'City': 'Neverland',
            'Currency': None,
            'Item Quantity': '1',
            'PostingDate': '2018-01-26 00:00:00'
            },
        'custom_fields': [{'field': 'Assignment_ID', 'reference': None}],
        'drivers': {'1_label': 'The website of the online shop (overall impression)',
        '2_label': 'Waiting period'}}
    ]

    result = CG.column_unpacker(json_list=data, unpack_by_field_reference_cols=unpack_by_field_reference_cols, unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer)

    assert result == expected_result


@pytest.mark.test_column_unpacker_missing_json_argument
def test_column_unpacker_missing_json_list():
    """
    Test the 'column_unpacker' method with missing 'json_list' argument. It should raise a ValueError.
    """
    unpack_by_field_reference_cols = ['properties']
    unpack_by_nested_dict_transformer = ['drivers']
    with pytest.raises(ValueError, match="Input 'json_list' is required."):
        CG.column_unpacker(json_list=None, unpack_by_field_reference_cols=unpack_by_field_reference_cols, unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer)


@pytest.mark.test_column_unpacker_duplicate_columns
def test_column_unpacker_duplicate_columns():
    """
    Test the 'column_unpacker' method with duplicate columns specified in both Method 1 and Method 2. It should raise a ValueError.
    """
    data = RAW_JSON['data'].copy()
    unpack_by_field_reference_cols = ['properties']
    unpack_by_nested_dict_transformer = ['properties']
    with pytest.raises(ValueError, match="{'properties'} were mentioned in both unpack_by_field_reference_cols and unpack_by_nested_dict_transformer. It's not possible to apply two methods to the same field."):
        CG.column_unpacker(json_list=data, unpack_by_field_reference_cols=unpack_by_field_reference_cols, unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer)


@pytest.mark.test_flatten_json
def test_flatten_json():
    """
    Test the 'flatten_json' method with nested JSON data. It should return a flattened dictionary with expected keys and values.
    """
    nested_json = {
        "user": {
            "name": "Jane",
            "address": {
                "street": "456 Elm St",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94109",
                "country": {"name": "United States", "code": "US"},
            },
            "phone_numbers": {"type": "home", "number": "555-4321"},
        }
    }

    expected_output = {
        "user_name": "Jane",
        "user_address_street": "456 Elm St",
        "user_address_city": "San Francisco",
        "user_address_state": "CA",
        "user_address_zip": "94109",
        "user_address_country_name": "United States",
        "user_address_country_code": "US",
        "user_phone_numbers_type": "home",
        "user_phone_numbers_number": "555-4321",
    }

    output = CG.flatten_json(nested_json)
    assert output == expected_output


@pytest.mark.flatten_json_non_dict_input
def test_flatten_json_non_dict_input():
    """
    Test the 'flatten_json' method with non-dictionary input. It should raise a TypeError.
    """
    input_json = [1, 2, 3]
    with pytest.raises(TypeError):
        CG.flatten_json(input_json)


@pytest.mark.square_brackets_remover
def test_square_brackets_remover_success():
    """
    Test the 'square_brackets_remover' method with a DataFrame containing square brackets. It should remove square brackets from the DataFrame.
    """
    data = {
        "Column1": ["Value1", "[Value2]", "Value3", "[Value4]"],
        "Column2": ["1", "[2]", "3", "[4]"],
    }
    sample_df = pd.DataFrame(data)

    expected_data = {
        "Column1": ["Value1", "Value2", "Value3", "Value4"],
        "Column2": ["1", "2", "3", "4"],
    }
    expected_df = pd.DataFrame(expected_data)

    result = CG.square_brackets_remover(sample_df)
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.drivers_cleaner
def test_drivers_cleaner_success():
    """
    Test the '_drivers_cleaner' method with valid 'drivers' data. It should clean and format the 'drivers' data and return the expected result.
    """
    data = "{'label': 'Driver1'}, {'label': 'Driver2'}, {'label': 'Driver3'}"
    expected_result = "Driver1, Driver2, Driver3"
    result = CG._drivers_cleaner(data)
    assert result == expected_result