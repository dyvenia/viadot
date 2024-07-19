import unittest
from datetime import date, timedelta
from io import StringIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests.auth import HTTPBasicAuth
from requests.models import Response

from viadot.exceptions import APIError, CredentialError
from viadot.sources import Mindful
from viadot.sources.mindful import MindfulCredentials

variables = {"credentials": {"customer_uuid": "fake_uuid", "auth_token": "fake_token"}}


class TestMindfulCredentials:
    """Test Mindful Credentials Class."""

    @pytest.mark.basic
    def test_mindful_credentials(self):
        """Test Genesys credentials."""
        MindfulCredentials(
            customer_uuid="test_customer_uuid", auth_token="test_auth_token"
        )


class TestMindful(unittest.TestCase):
    """Test Mindful Class."""

    @classmethod
    def setUpClass(cls):
        """Defined based Mindful Class for the rest of test."""
        cls.mindful_instance = Mindful(credentials=variables["credentials"])

    @patch("viadot.sources.mindful.get_source_credentials", return_value=None)
    @pytest.mark.basic
    def test_init_no_credentials(self, mock_get_source_credentials):
        """Test raise error without credentials."""
        with pytest.raises(CredentialError):
            Mindful()

        mock_get_source_credentials.assert_called_once()

    @pytest.mark.connect
    @patch("viadot.sources.mindful.handle_api_response")
    def test_mindful_api_response(self, mock_handle_api_response):
        """Test Genesys `_mindful_api_response` method."""
        mock_response = MagicMock(spec=Response)
        mock_handle_api_response.return_value = mock_response

        self.mindful_instance._mindful_api_response(endpoint="interactions")
        mock_handle_api_response.assert_called_once_with(
            url="https://eu1.surveydynamix.com/api/interactions",
            params=None,
            method="GET",
            auth=unittest.mock.ANY,
        )

        auth_arg = mock_handle_api_response.call_args[1]["auth"]
        self.assertIsInstance(auth_arg, HTTPBasicAuth)
        self.assertEqual(auth_arg.username, variables["credentials"]["customer_uuid"])
        self.assertEqual(auth_arg.password, variables["credentials"]["auth_token"])

    @pytest.mark.connect
    @patch("viadot.sources.mindful.handle_api_response")
    def test_api_connection(self, mock_handle_api_response):
        """Test Genesys `api_connection` method."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.content = b'{"data": "some_data"}'
        mock_handle_api_response.return_value = mock_response

        date_interval = [date.today() - timedelta(days=1), date.today()]
        self.mindful_instance.api_connection(
            endpoint="responses", date_interval=date_interval
        )

        mock_handle_api_response.assert_called_once()
        self.assertIsInstance(self.mindful_instance.data, StringIO)

    @pytest.mark.connect
    @patch("viadot.sources.mindful.handle_api_response")
    def test_api_connection_no_data(self, mock_handle_api_response):
        """Test Genesys `api_connection` method without data."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 204
        mock_response.content = b""
        mock_handle_api_response.return_value = mock_response

        date_interval = [date.today() - timedelta(days=1), date.today()]
        self.mindful_instance.api_connection(
            endpoint="responses", date_interval=date_interval
        )

        mock_handle_api_response.assert_called_once()
        self.assertEqual(self.mindful_instance.data, "{}")

    @pytest.mark.connect
    @patch("viadot.sources.mindful.handle_api_response")
    def test_api_connection_error(self, mock_handle_api_response):
        """Test Genesys `api_connection` method, APIError."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 500
        mock_response.content = b"Internal Server Error"
        mock_handle_api_response.return_value = mock_response

        with self.assertRaises(APIError):
            self.mindful_instance.api_connection(endpoint="responses")

    @pytest.mark.functions
    @patch("viadot.sources.mindful.pd.read_json")
    @patch("viadot.sources.mindful.super")
    def test_to_df(self, mock_super, mock_read_json):
        """Test Genesys `to_df` method."""
        mock_super().to_df = MagicMock()
        mock_read_json.return_value = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        self.mindful_instance.data = StringIO('{"A": [1, 2], "B": [3, 4]}')

        result_df = self.mindful_instance.to_df()
        result_df.drop(
            columns=["_viadot_source", "_viadot_downloaded_at_utc"],
            inplace=True,
            axis=1,
        )
        expected_df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

        pd.testing.assert_frame_equal(result_df, expected_df)
        mock_super().to_df.assert_called_once()

    @pytest.mark.functions
    @patch("viadot.sources.mindful.pd.read_json")
    @patch("viadot.sources.mindful.super")
    def test_to_df_empty(self, mock_super, mock_read_json):
        """Test Genesys `to_df` method, checking emptiness."""
        mock_super().to_df = MagicMock()
        mock_read_json.return_value = pd.DataFrame()
        self.mindful_instance.data = StringIO("{}")

        with patch.object(
            self.mindful_instance, "_handle_if_empty"
        ) as mock_handle_if_empty:
            result_df = self.mindful_instance.to_df()
            mock_handle_if_empty.assert_called_once_with(
                if_empty="warn", message="The response does not contain any data."
            )
            self.assertTrue(result_df.empty)
            mock_super().to_df.assert_called_once()


if __name__ == "__main__":
    unittest.main()
