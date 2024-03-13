import os
from unittest import mock
from datetime import datetime, timedelta

import pytest

from viadot.sources import Mindful


@pytest.fixture(scope="function")
def var_dictionary():
    variables = {
        "credentials": {
            "client_id": "1111",
            "client_secret": "xxxxxxxxxxxxxxxxxxxxxxx",
            "customer_uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "auth_token": "xxxxxxxxxxxxxxxxxxxxxxx",
            "vault": "xxxxxxxxxxxxxxxxxxxxxxx",
        },
        "start_date": datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
        - timedelta(days=2),
        "end_date": datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
        - timedelta(days=1),
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
            {"id": 7277294, "survey_id": 504, "phone_number": "", "survey_type": "web"},
        ]
        return test


class MockClassException:
    status_code = 204
    content = b""

    def json():
        return None


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.connect
def test_mindful_api_response(mock_connection, var_dictionary):
    mf = Mindful(region="eu1", credentials=var_dictionary["credentials"])
    mf.to_df(
        endpoint="interactions",
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )
    mock_connection.assert_called_once()
    mock_connection.reset_mock()

    mf.to_df(
        endpoint="responses",
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )
    mock_connection.assert_called_once()
    mock_connection.reset_mock()

    mf.to_df(endpoint="surveys")
    mock_connection.assert_called_once()


@mock.patch("viadot.sources.Mindful._mindful_api_response", return_value=MockClass)
@pytest.mark.save
def test_mindful_file(mock_connection, var_dictionary):
    mf = Mindful(region="eu1", credentials=var_dictionary["credentials"])
    df = mf.to_df(
        endpoint="interactions",
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )
    mf.to_file(df, file_name="interactions")

    assert os.path.exists("interactions.csv")
