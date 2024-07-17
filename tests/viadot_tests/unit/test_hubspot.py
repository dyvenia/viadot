import unittest
from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import APIError
from viadot.sources import Hubspot
from viadot.sources.hubspot import HubspotCredentials


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


class TestHubspotCredentials:
    @pytest.fixture(scope="function")
    def hubspotcredentials(self):
        return HubspotCredentials()


class TestHubspot:
    @pytest.fixture(scope="function")
    def hubspot(self):
        return Hubspot()

    def test__date_to_unixtimestamp(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot._date_to_unixtimestamp (line 121)
        pass

    def test__get_api_url(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot._get_api_url (line 139)
        pass

    def test__format_filters(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot._format_filters (line 175)
        pass

    def test__get_api_body(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot._get_api_body (line 201)
        pass

    def test__api_call(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot._api_call (line 248)
        pass

    def test__get_offset_from_response(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot._get_offset_from_response (line 287)
        pass

    def test_api_connection(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot.api_connection (line 313)
        pass

    def test_to_df(self, hubspot):
        # TODO [use mock.patch, assert]: Implement test for Hubspot.to_df (line 395)
        pass


if __name__ == "__main__":
    unittest.main()
