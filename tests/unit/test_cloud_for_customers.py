from unittest import mock

from pydantic import SecretStr
import pytest

from viadot.exceptions import CredentialError
from viadot.sources.cloud_for_customers import (
    CloudForCustomers,
    CloudForCustomersCredentials,
)


def test_is_configured_valid():
    credentials = {
        "username": "user@tenant.com",
        "password": SecretStr("password"),
    }
    validated_creds = CloudForCustomersCredentials.is_configured(credentials)
    assert validated_creds == credentials


def test_is_configured_invalid():
    credentials = {"username": "user@tenant.com"}
    with pytest.raises(CredentialError):
        CloudForCustomersCredentials.is_configured(credentials)


@pytest.fixture
def c4c():
    credentials = {
        "username": "user@tenant.com",
        "password": SecretStr("password"),
        "url": "https://example.com",
        "report_url": "https://example.com/report",
    }
    return CloudForCustomers(credentials=credentials)


def test_create_metadata_url(c4c):
    url = "https://example.com/service.svc/Entity"
    expected_metadata_url = "https://example.com/service.svc/$metadata?entityset=Entity"
    assert c4c.create_metadata_url(url) == expected_metadata_url


def test_get_entities(c4c):
    dirty_json = {"d": {"results": [{"key": "value"}]}}
    url = "https://example.com/service.svc/Entity"

    c4c.create_metadata_url = mock.Mock(
        return_value="https://example.com/service.svc/$metadata?entityset=Entity"
    )
    expected_entities = {"key": "new_key"}
    c4c.get_property_to_sap_label_dict = mock.Mock(return_value=expected_entities)

    assert c4c.get_entities(dirty_json, url) == [{"new_key": "value"}]


@mock.patch("viadot.sources.cloud_for_customers.requests.get")
def test_get_property_to_sap_label_dict(mocked_requests_get, c4c):
    mocked_requests_get.return_value.text = (
        """<Property Name="key" sap:label="Label"/>"""
    )
    column_mapping = c4c.get_property_to_sap_label_dict(
        url="https://example.com/metadata"
    )
    assert column_mapping == {"key": "Label"}


@mock.patch("viadot.sources.cloud_for_customers.requests.get")
def test_get_response(mocked_requests_get, c4c):
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"d": {"results": [{"key": "value"}]}}
    mocked_requests_get.return_value = mock_response
    c4c.get_response = mocked_requests_get

    response = c4c.get_response(url="https://example.com/service.svc/Entity")
    assert response.ok
    assert response.json() == {"d": {"results": [{"key": "value"}]}}
