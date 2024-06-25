from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import APIError
from viadot.sources import Hubspot


@pytest.fixture(scope="function")
def var_dictionary():
    variables = {
        "credentials": {
            "token": "xxxxxxxxxxxxxxxxxxxxxxx",
        },
        "filters": [
            {
                "filters": [
                    {
                        "propertyName": "createdate",
                        "operator": "BETWEEN",
                        "highValue": "1642636800000",
                        "value": "1641995200000",
                        "pruebas": "2024-06-24",
                    },
                    {
                        "propertyName": "email",
                        "operator": "CONTAINS_TOKEN",
                        "value": "*@xxxxxxx.xx",
                    },
                ]
            }
        ],
    }
    return variables


class MockClass:
    status_code = 200

    def json():
        test = {
            "objects": [
                {
                    "id": 9999999999,
                    "createdAt": 1719201702051,
                    "updatedAt": 1719201702051,
                    "publishedAt": None,
                    "path": None,
                    "name": None,
                    "values": {"1": "xxxxxx", "2": "xxxxx"},
                    "isSoftEditable": False,
                    "childTableId": 0,
                }
            ],
            "total": 164,
            "limit": 1000,
            "offset": 0,
            "message": None,
            "totalCount": 164,
        }
        return test


class MockClassF:
    status_code = 200

    def json():
        test = {
            "results": {
                "objects": [
                    {
                        "id": 9999999999,
                        "createdAt": 1719201702051,
                        "updatedAt": 1719201702051,
                        "publishedAt": None,
                        "path": None,
                        "name": None,
                        "values": {"1": "xxxxxx", "2": "xxxxx"},
                        "isSoftEditable": False,
                        "childTableId": 0,
                    }
                ],
                "total": 164,
                "limit": 1000,
                "offset": 0,
                "message": None,
                "totalCount": 164,
            }
        }
        return test


class MockClassException:
    status_code = 404
    content = b""

    def json():
        return None


@mock.patch("viadot.sources.hubspot.handle_api_response", return_value=MockClass)
@pytest.mark.connect
def test_hubspot_api_response(mock_connection, var_dictionary):
    hp = Hubspot(credentials=var_dictionary["credentials"])
    hp.api_connection(
        endpoint="hubdb/api/v2/tables/99999999",
    )

    mock_connection.assert_called_once()
    assert isinstance(hp.full_dataset, list)


@mock.patch("viadot.sources.hubspot.handle_api_response", return_value=MockClassF)
@pytest.mark.connect
def test_hubspot_api_response_filters(mock_connection, var_dictionary):
    hp = Hubspot(credentials=var_dictionary["credentials"])
    hp.api_connection(
        endpoint="hubdb/api/v2/tables/99999999",
        filters=var_dictionary["filters"],
    )

    mock_connection.assert_called_once()
    assert isinstance(hp.full_dataset, dict)


@mock.patch(
    "viadot.sources.hubspot.handle_api_response", return_value=MockClassException
)
@pytest.mark.connect
def test_hubspot_api_error(mock_connection, var_dictionary):
    hp = Hubspot(credentials=var_dictionary["credentials"])
    with pytest.raises(APIError):
        hp.api_connection(
            endpoint="hubdb/api/v2/tables/99999999",
        )

    mock_connection.assert_called_once()


@mock.patch("viadot.sources.hubspot.handle_api_response", return_value=MockClass)
@pytest.mark.response
def test_hubspot_api_df_response(mock_connection, var_dictionary):
    hp = Hubspot(credentials=var_dictionary["credentials"])
    hp.api_connection(
        endpoint="hubdb/api/v2/tables/99999999",
    )
    df = hp.to_df()

    viadot_set = {"_viadot_source", "_viadot_downloaded_at_utc"}

    mock_connection.assert_called_once()
    assert set(df.columns).issuperset(viadot_set)
    assert isinstance(df, pd.DataFrame)
