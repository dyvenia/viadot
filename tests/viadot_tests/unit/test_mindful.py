from datetime import date, timedelta
from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import APIError
from viadot.sources import Mindful


@pytest.fixture(scope="function")
def var_dictionary():
    variables = {
        "credentials": {
            "customer_uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "auth_token": "xxxxxxxxxxxxxxxxxxxxxxx",
        },
        "date_interval": [
            date.today() - timedelta(days=2),
            date.today() - timedelta(days=1),
        ],
    }
    return variables


class MockClass:
    status_code = 200
    content = b'[{"id":7277599,"survey_id":505,"phone_number":"","survey_type":"inbound"},{"id":7277294,"survey_id":504,"phone_number":"","survey_type":"web"}]'

    def json():
        test = [
            {
                "id": 7277599,
                "survey_id": 505,
                "phone_number": "",
                "survey_type": "inbound",
            },
            {
                "id": 7277294,
                "survey_id": 504,
                "phone_number": "",
                "survey_type": "web",
            },
        ]
        return test


class MockClassException:
    status_code = 404
    content = b""

    def json():
        return None


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.connect
def test_mindful_api_response(mock_connection, var_dictionary):
    mf = Mindful(credentials=var_dictionary["credentials"])
    mf.api_connection(
        endpoint="interactions",
        date_interval=var_dictionary["date_interval"],
    )

    mock_connection.assert_called_once()
    mock_connection.reset_mock()

    mf.api_connection(
        endpoint="responses",
        date_interval=var_dictionary["date_interval"],
    )

    mock_connection.assert_called_once()
    mock_connection.reset_mock()

    mf.api_connection(
        endpoint="surveys",
        date_interval=var_dictionary["date_interval"],
    )
    mock_connection.assert_called_once()


@mock.patch(
    "viadot.sources.mindful.handle_api_response", return_value=MockClassException
)
@pytest.mark.connect
def test_mindful_api_error(mock_connection, var_dictionary):
    mf = Mindful(credentials=var_dictionary["credentials"])
    with pytest.raises(APIError):
        mf.api_connection(
            endpoint="interactions",
            date_interval=var_dictionary["date_interval"],
        )

    mock_connection.assert_called_once()


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.response
def test_mindful_api_df_response(mock_connection, var_dictionary):
    mf = Mindful(credentials=var_dictionary["credentials"])
    mf.api_connection(
        endpoint="interactions",
        date_interval=var_dictionary["date_interval"],
    )
    df = mf.to_df()

    viadot_set = {"_viadot_source", "_viadot_downloaded_at_utc"}

    assert set(df.columns).issuperset(viadot_set)
    assert isinstance(df, pd.DataFrame)
