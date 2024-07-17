# tests/viadot_tests/unit/test_cloud_for_customers.py
import unittest
from unittest import mock
from pydantic import SecretStr
from viadot.sources.cloud_for_customers import CloudForCustomers, CloudForCustomersCredentials
from viadot.exceptions import CredentialError

# Test cases for CloudForCustomersCredentials
class TestCloudForCustomersCredentials(unittest.TestCase):
    def setUp(self):
        self.valid_credentials = {"username": "user@tenant.com", "password": SecretStr("password")}
        self.invalid_credentials = {"username": "user@tenant.com"}

    def test_is_configured_valid(self):
        validated_creds = CloudForCustomersCredentials.is_configured(self.valid_credentials)
        self.assertEqual(validated_creds, self.valid_credentials)

    def test_is_configured_invalid(self):
        with self.assertRaises(CredentialError):
            CloudForCustomersCredentials.is_configured(self.invalid_credentials)


# Test cases for CloudForCustomers
class TestCloudForCustomers(unittest.TestCase):
    def setUp(self):
        self.credentials = {
            "username": "user@tenant.com",
            "password": SecretStr("password"),
            "url": "https://example.com",
            "report_url": "https://example.com/report"
        }
        self.cloudforcustomers = CloudForCustomers(credentials=self.credentials)

    def test_create_metadata_url(self):
        url = "https://example.com/service.svc/Entity"
        expected_metadata_url = "https://example.com/service.svc/$metadata?entityset=Entity"
        self.assertEqual(self.cloudforcustomers.create_metadata_url(url), expected_metadata_url)

    def test_get_entities(self):
        dirty_json = {"d": {"results": [{"key": "value"}]}}
        url = "https://example.com/service.svc/Entity"

        self.cloudforcustomers.create_metadata_url = mock.Mock(return_value="https://example.com/service.svc/$metadata?entityset=Entity")
        self.cloudforcustomers.get_property_to_sap_label_dict = mock.Mock(return_value={"key": "new_key"})

        entities = self.cloudforcustomers.get_entities(dirty_json, url)
        self.assertEqual(entities, [{"new_key": "value"}])

    @mock.patch('viadot.sources.cloud_for_customers.requests.get')
    def test_get_property_to_sap_label_dict(self, mock_requests_get):
        mock_requests_get.return_value.text = '<Property Name="key" sap:label="Label"/>'
        column_mapping = self.cloudforcustomers.get_property_to_sap_label_dict(url="https://example.com/metadata")
        self.assertEqual(column_mapping, {"key": "Label"})

    @mock.patch('viadot.sources.cloud_for_customers.requests.get')
    def test_get_response(self, mock_requests_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"d": {"results": [{"key": "value"}]}}
        mock_requests_get.return_value = mock_response
        self.cloudforcustomers.get_response = mock_requests_get
        
        response = self.cloudforcustomers.get_response(url = "https://example.com/service.svc/Entity")
        self.assertIsNotNone(response)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"d": {"results": [{"key": "value"}]}})

if __name__ == '__main__':
    unittest.main()
