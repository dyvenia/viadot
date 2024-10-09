"""'test_eurostat.py'."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from viadot.sources import Eurostat


@pytest.fixture
def eurostat_instance():
    return Eurostat(dataset_code="TEIBS020", params=None, columns=None, tests=None)


@patch.object(Eurostat, "get_parameters_codes")
def test_validate_params_valid(mock_get_params, eurostat_instance):
    mock_get_params.return_value = {"unit": ["EUR", "USD"]}

    valid_params = {"unit": "EUR"}
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/TEIBS020"

    # No exception should be raised for valid parameters
    try:
        eurostat_instance.validate_params("TEIBS020", url, valid_params)
    except ValueError:
        pytest.fail("ValueError raised unexpectedly!")


@patch.object(Eurostat, "get_parameters_codes")
def test_validate_params_invalid_key(mock_get_params, eurostat_instance):
    mock_get_params.return_value = {"unit": ["EUR", "USD"]}

    invalid_params = {"invalid_key": "EUR"}
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/TEIBS020"

    with pytest.raises(ValueError, match="Wrong parameters or codes were provided!"):
        eurostat_instance.validate_params("TEIBS020", url, invalid_params)


@patch("requests.get")
def test_eurostat_dictionary_to_df(mock_requests_get, eurostat_instance):
    mock_data = {
        "dimension": {
            "geo": {
                "category": {
                    "index": {"DE": 0, "FR": 1, "IT": 2},
                    "label": {"DE": "Germany", "FR": "France", "IT": "Italy"},
                }
            },
            "time": {
                "category": {
                    "index": {"2020": 0, "2021": 1, "2022": 2},
                    "label": {"2020": "2020", "2021": "2021", "2022": "2022"},
                }
            },
        },
        "value": {
            "0": 100,
            "1": 150,
            "2": 200,
            "3": 110,
            "4": 160,
            "5": 210,
            "6": 120,
            "7": 170,
            "8": 220,
        },
    }

    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.json.return_value = {}

    signals = [["geo", "time"]]

    # Call the method with signals as a list
    df = eurostat_instance.eurostat_dictionary_to_df(*signals, mock_data)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (9, 3)
    assert set(df.columns) == {"indicator", "geo", "time"}


@patch("viadot.utils._handle_response")
@patch("viadot.utils.handle_api_response")
@patch("viadot.utils.handle_api_request")
def test_get_parameters_codes_empty_response(
    mock_handle_api_request,  # noqa: ARG001
    mock_handle_api_response,
    mock_handle_response,
    eurostat_instance,
):
    mock_response_obj = MagicMock()
    mock_response_obj.json.return_value = {"id": [], "dimension": []}
    mock_handle_api_response.return_value = mock_response_obj

    mock_handle_response.return_value = mock_response_obj

    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/TEIBS020"

    result = eurostat_instance.get_parameters_codes(url)
    assert result == {}


@patch("viadot.utils._handle_response")
@patch("viadot.utils.handle_api_response")
@patch("viadot.utils.handle_api_request")
def test_get_parameters_codes_valid(
    mock_handle_api_request,  # noqa: ARG001
    mock_handle_api_response,
    mock_handle_response,
    eurostat_instance,
):
    mock_response = {
        "id": ["unit", "geo"],
        "dimension": {
            "unit": {
                "category": {
                    "index": {"EUR": 0, "USD": 1},
                    "label": {"Euro": 0, "US Dollar": 1},
                }
            },
            "geo": {
                "category": {
                    "index": {"FR": 0, "DE": 1},
                    "label": {"France": 0, "Germany": 1},
                }
            },
        },
    }

    mock_response_obj = MagicMock()
    mock_response_obj.json.return_value = mock_response
    mock_handle_api_response.return_value = mock_response_obj

    mock_handle_response.return_value = mock_response_obj

    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/TEIBS020"

    result = eurostat_instance.get_parameters_codes(url)

    expected_result = {"unit": ["EUR", "USD"], "geo": ["FR", "DE"]}

    assert result == expected_result


@patch("viadot.utils.handle_api_response")
def test_to_df_invalid_columns_type(mock_handle_api_response, eurostat_instance):
    eurostat_instance.columns = "invalid_columns"  # Not a list

    with pytest.raises(
        TypeError, match="Requested columns should be provided as list of strings."
    ):
        eurostat_instance.to_df()


@patch("viadot.utils.handle_api_response")
def test_to_df_invalid_params_type(mock_handle_api_response, eurostat_instance):
    eurostat_instance.params = "invalid_params"  # Not a dictionary

    with pytest.raises(TypeError, match="Params should be a dictionary."):
        eurostat_instance.to_df()
