"""'test_genesys.py'."""

import json
import unittest
import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import asynctest
import pandas as pd
import pytest

from viadot.exceptions import APIError, CredentialError
from viadot.sources import Genesys
from viadot.sources.genesys import GenesysCredentials

warnings.filterwarnings("ignore", category=DeprecationWarning)

variables = {
    "credentials": {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
    },
    "request_headers": {
        "token_type": "Bearer",
        "access_token": "test_access_token",
    },
    "authorization_token": {
        "Authorization": "Bearer access_token",
        "Content-Type": "application/json",
    },
    "entities": [
        {
            "id": "report1",
            "downloadUrl": "http://example.com/report1",
            "status": "COMPLETED",
        },
        {
            "id": "report2",
            "downloadUrl": "http://example.com/report2",
            "status": "COMPLETED",
        },
    ],
    "entities_fail": [
        {
            "id": "report1",
            "downloadUrl": "http://example.com/report1",
            "status": "FAILED",
        },
    ],
    "entities_run": [
        {
            "id": "report1",
            "downloadUrl": "http://example.com/report1",
            "status": "RUNNING",
        },
    ],
    "content": b"id,name\n1,Report1\n2,Report2",
    "expected_data": {"id": [1, 2], "name": ["Report1", "Report2"]},
    "report_url": "http://example.com/report",
    "data_to_merge": [
        {
            "conversationId": "conv1",
            "participants": [
                {
                    "externalContactId": "ext1",
                    "participantId": "part1",
                    "sessions": [
                        {
                            "sessionId": "sess1",
                        }
                    ],
                }
            ],
        }
    ],
    "expected_columns": [
        "conversationId",
        "externalContactId",
        "participantId",
        "sessionId",
    ],
    "expected_data_to_merge": {
        "conversationId": ["conv1"],
        "externalContactId": ["ext1"],
        "participantId": ["part1"],
        "sessionId": ["sess1"],
    },
    "mock_post_data_list": [{"data": "some data"}, {"data2": "some data2"}],
    "mock_report": {
        "conversations": [
            {"conversationId": "conv1", "participants": [{"sessionId": "sess1"}]}
        ],
        "totalHits": 100,
    },
}


class TestGenesysCredentials:
    """Test Genesys Credentials Class."""

    @pytest.mark.basic
    def test_genesys_credentials(self):
        """Test Genesys credentials."""
        GenesysCredentials(
            client_id="test_client_id", client_secret="test_client_secret"
        )


class TestGenesys(unittest.TestCase):
    """Test Genesys Class."""

    @classmethod
    def setUpClass(cls):
        """Defined based Genesys Class for the rest of test."""
        cls.genesys_instance = Genesys(
            credentials=variables["credentials"], verbose=True
        )

    @patch("viadot.sources.genesys.get_source_credentials", return_value=None)
    @pytest.mark.basic
    def test_init_no_credentials(self, mock_get_source_credentials):
        """Test raise error without credentials."""
        with pytest.raises(CredentialError):
            Genesys()

        mock_get_source_credentials.assert_called_once()

    @pytest.mark.basic
    def test_init_invalid_environment(self):
        """Test Genesys invalid environment."""
        with pytest.raises(APIError):
            Genesys(
                credentials=variables["credentials"],
                environment="invalid_environment",
            )

    @pytest.mark.connect
    @patch("viadot.sources.genesys.handle_api_response")
    def test_authorization_token(self, mock_handle_api_response):
        """Test Genesys `authorization_token` property."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = variables["request_headers"]
        mock_handle_api_response.return_value = mock_response

        headers = self.genesys_instance.authorization_token
        self.assertEqual(headers["Authorization"], "Bearer test_access_token")

    @pytest.mark.connect
    @asynctest.patch("aiohttp.ClientSession.post")
    async def test_api_call_post_success(self, mock_post):
        """Test Genesys `_api_call` method called with POST."""
        mock_response = AsyncMock()
        mock_response.read.return_value = json.dumps({"key": "value"}).encode("utf-8")
        mock_post.return_value.__aenter__.return_value = mock_response

        response = await self.genesys_instance._api_call(
            endpoint="test_endpoint",
            post_data_list=[{"data_key": "data_value"}],
            method="POST",
        )

        self.assertEqual(response, {"key": "value"})
        mock_post.assert_called_once_with(
            f"https://api.{self.genesys_instance.environment}/api/v2/test_endpoint",
            headers=self.genesys_instance.authorization_token,
            data=json.dumps({"data_key": "data_value"}),
        )

    @pytest.mark.connect
    @asynctest.patch("aiohttp.ClientSession.get")
    async def test_api_call_get_success(self, mock_get):
        """Test Genesys `_api_call` method called with GET."""
        mock_response = AsyncMock()
        mock_response.read.return_value = json.dumps({"key": "value"}).encode("utf-8")
        mock_get.return_value.__aenter__.return_value = mock_response

        response = await self.genesys_instance._api_call(
            endpoint="test_endpoint",
            post_data_list=[],
            method="GET",
            params={"param1": "value1"},
        )

        self.assertEqual(response, {"key": "value"})
        mock_get.assert_called_once_with(
            f"https://api.{self.genesys_instance.environment}/api/v2/test_endpoint",
            headers=self.genesys_instance.authorization_token,
            params={"param1": "value1"},
        )

    @pytest.mark.connect
    @asynctest.patch("aiohttp.ClientSession.post")
    async def test_api_call_post_failure(self, mock_post):
        """Test Genesys `_api_call` method failing when called with POST."""
        mock_response = AsyncMock()
        mock_response.read.return_value = "Bad Request".encode("utf-8")
        mock_response.status = 400
        mock_post.return_value.__aenter__.return_value = mock_response

        with self.assertRaises(APIError) as context:
            await self.genesys_instance._api_call(
                endpoint="test_endpoint",
                post_data_list=[{"data_key": "data_value"}],
                method="POST",
            )

        self.assertIn("API call failed", str(context.exception))
        mock_post.assert_called_once_with(
            f"https://api.{self.genesys_instance.environment}/api/v2/test_endpoint",
            headers=self.genesys_instance.authorization_token,
            data=json.dumps({"data_key": "data_value"}),
        )

    @pytest.mark.response
    @patch("viadot.sources.genesys.handle_api_response")
    @patch.object(Genesys, "authorization_token", new_callable=MagicMock)
    def test_load_reporting_exports(self, mock_auth_token, mock_handle_api_response):
        """Test Genesys `_load_reporting_exports` method."""
        mock_auth_token.return_value = variables["authorization_token"]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"entities": []}
        mock_handle_api_response.return_value = mock_response

        result = self.genesys_instance._load_reporting_exports()
        self.assertEqual(result, {"entities": []})

    @pytest.mark.response
    @patch("viadot.sources.genesys.handle_api_response")
    @patch.object(Genesys, "authorization_token", new_callable=MagicMock)
    def test_load_reporting_exports_failure(
        self, mock_auth_token, mock_handle_api_response
    ):
        """Test Genesys `_load_reporting_exports` method failing."""
        mock_auth_token.return_value = variables["authorization_token"]

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_handle_api_response.return_value = mock_response

        with self.assertRaises(APIError) as context:
            self.genesys_instance._load_reporting_exports()

        self.assertIn("Failed to loaded all exports.", str(context.exception))

    @pytest.mark.response
    @patch("builtins.print")
    def test_get_reporting_exports_url(self, mock_print):
        """Test Genesys `_get_reporting_exports_url` method."""
        entities = variables["entities"]
        expected_ids = ["report1", "report2"]
        expected_urls = ["http://example.com/report1", "http://example.com/report2"]

        actual_ids, actual_urls = self.genesys_instance._get_reporting_exports_url(
            entities
        )

        self.assertEqual(expected_ids, actual_ids)
        self.assertEqual(expected_urls, actual_urls)

        mock_print.assert_called_with(
            "Report status:\n\treport1 -> COMPLETED \n\treport2 -> COMPLETED \n"
        )

    @pytest.mark.response
    @patch("builtins.print")
    def test_get_reporting_exports_url_with_failed_status(self, mock_print):
        """Test Genesys `_get_reporting_exports_url` method FAILED status."""
        entities = variables["entities_fail"]
        expected_ids = ["report1"]
        expected_urls = ["http://example.com/report1"]

        actual_ids, actual_urls = self.genesys_instance._get_reporting_exports_url(
            entities
        )

        self.assertEqual(expected_ids, actual_ids)
        self.assertEqual(expected_urls, actual_urls)
        mock_print.assert_any_call(
            "\x1b[33mERROR\x1b[0m: Some reports have not been successfully created."
        )
        mock_print.assert_called_with("Report status:\n\treport1 -> FAILED \n")

    @pytest.mark.response
    @patch("builtins.print")
    def test_get_reporting_exports_url_with_running_status(self, mock_print):
        """Test Genesys `_get_reporting_exports_url` method RUNNING status."""
        entities = variables["entities_run"]
        expected_ids = ["report1"]
        expected_urls = ["http://example.com/report1"]
        actual_ids, actual_urls = self.genesys_instance._get_reporting_exports_url(
            entities
        )

        self.assertEqual(expected_ids, actual_ids)
        self.assertEqual(expected_urls, actual_urls)
        mock_print.assert_any_call(
            "\x1b[33mERROR\x1b[0m: Some reports are still being "
            + "created and can not be downloaded."
        )
        mock_print.assert_called_with("Report status:\n\treport1 -> RUNNING \n")

    @pytest.mark.response
    @patch("viadot.sources.genesys.handle_api_response")
    @patch("builtins.print")
    def test_download_report_success(self, mock_print, mock_handle_api_response):
        """Test Genesys `_download_report` method."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = variables["content"]
        mock_handle_api_response.return_value = mock_response

        expected_data = variables["expected_data"]
        expected_df = pd.DataFrame(expected_data)

        report_url = variables["report_url"]
        actual_df = self.genesys_instance._download_report(report_url)

        pd.testing.assert_frame_equal(expected_df, actual_df)

        mock_print.assert_called_with(
            "Successfully downloaded report from Genesys API "
            "('http://example.com/report')."
        )

    @pytest.mark.response
    @patch("viadot.sources.genesys.handle_api_response")
    @patch("builtins.print")
    def test_download_report_failure(self, mock_print, mock_handle_api_response):
        """Test Genesys `_download_report` method failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.content = b"Not Found"
        mock_handle_api_response.return_value = mock_response

        report_url = variables["report_url"]
        self.genesys_instance._download_report(report_url)

        mock_print.assert_called_with(
            "\x1b[31mERROR\x1b[0m: Failed to download report from Genesys API "
            + "('http://example.com/report'). - b'Not Found'"
        )

    @pytest.mark.response
    @patch("viadot.sources.genesys.handle_api_response")
    def test_download_report_drop_duplicates(self, mock_handle_api_response):
        """Test Genesys `_download_report` method, dropping duplicates."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = variables["content"]
        mock_handle_api_response.return_value = mock_response

        expected_data = variables["expected_data"]
        expected_df = pd.DataFrame(expected_data)

        report_url = variables["report_url"]
        actual_df = self.genesys_instance._download_report(
            report_url,
            drop_duplicates=True,
        )

        pd.testing.assert_frame_equal(expected_df, actual_df)

    @pytest.mark.response
    @patch("viadot.sources.genesys.handle_api_response")
    @patch("builtins.print")
    def test_delete_report_success(self, mock_print, mock_handle_api_response):
        """Test Genesys `_delete_report` method."""
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_handle_api_response.return_value = mock_response

        report_id = "123456"
        self.genesys_instance._delete_report(report_id)

        mock_print.assert_called_with(
            f"Successfully deleted report '{report_id}' from Genesys API."
        )

    @pytest.mark.response
    @patch("viadot.sources.genesys.handle_api_response")
    @patch("builtins.print")
    def test_delete_report_failure(self, mock_print, mock_handle_api_response):
        """Test Genesys `_delete_report` method failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.content = b"Not Found"
        mock_handle_api_response.return_value = mock_response

        report_id = "123456"
        self.genesys_instance._delete_report(report_id)

        mock_print.assert_called_with(
            f"\x1b[31mERROR\x1b[0m: Failed to deleted report '{report_id}' "
            + "from Genesys API. - b'Not Found'"
        )

    @pytest.mark.functions
    def test_merge_conversations(self):
        """Test Genesys `_merge_conversations` function."""
        mock_data = variables["data_to_merge"]
        expected_columns = variables["expected_columns"]
        expected_data = variables["expected_data_to_merge"]

        result_df = self.genesys_instance._merge_conversations(mock_data)

        self.assertEqual(list(result_df.columns), expected_columns)
        for col, expected_values in expected_data.items():
            self.assertEqual(list(result_df[col]), expected_values)

    @pytest.mark.response
    @patch("viadot.sources.genesys.Genesys._api_call")
    @patch("viadot.sources.genesys.Genesys._load_reporting_exports")
    @patch("viadot.sources.genesys.Genesys._get_reporting_exports_url")
    @patch("viadot.sources.genesys.Genesys._download_report")
    @patch("viadot.sources.genesys.Genesys._delete_report")
    def test_api_connection_reporting_exports(
        self,
        mock_delete,
        mock_download,
        mock_get_url,
        mock_load,
        mock_api_call,
    ):
        """Test Genesys `api_connection` method with reporting exports."""
        mock_entities = variables["entities"]
        mock_load.return_value = {"entities": mock_entities}
        mock_get_url.return_value = (
            ["report1", "report2"],
            ["http://example.com/report1", "http://example.com/report2"],
        )
        mock_download.return_value = pd.DataFrame(
            {"Queue Id": ["queue1;queue2"], "data": [1]}
        )

        self.genesys_instance.api_connection(
            endpoint="analytics/reporting/exports",
            post_data_list=variables["mock_post_data_list"],
            view_type="queue_performance_detail_view",
            view_type_time_sleep=0.5,
        )

        mock_api_call.assert_called_once_with(
            endpoint="analytics/reporting/exports",
            post_data_list=variables["mock_post_data_list"],
            method="POST",
        )
        mock_load.assert_called_once()
        mock_get_url.assert_called_once()
        mock_download.assert_called()
        mock_delete.assert_called()

    @pytest.mark.response
    @patch("viadot.sources.genesys.Genesys._api_call")
    @patch("viadot.sources.genesys.Genesys._merge_conversations")
    def test_api_connection_conversations(
        self,
        mock_merge,
        mock_api_call,
    ):
        """Test Genesys `api_connection` method with conversations details."""
        mock_post_data_list = [{"paging": {"pageNumber": 1}}]
        mock_report = variables["mock_report"]
        mock_merge.return_value = pd.DataFrame(
            {"conversationId": ["conv1"], "data": [1]}
        )
        mock_api_call.side_effect = [mock_report, mock_report]

        self.genesys_instance.api_connection(
            endpoint="analytics/conversations/details/query",
            post_data_list=mock_post_data_list,
        )

        mock_api_call.assert_called()
        mock_merge.assert_called()

    @pytest.mark.response
    @patch("viadot.sources.genesys.Genesys._api_call")
    def test_api_connection_routing_queues_members(self, mock_api_call):
        """Test Genesys `api_connection` method with routing queues."""
        mock_queues_ids = ["queue1"]
        mock_response_page_1 = {"entities": [{"userId": "user1", "name": "Agent1"}]}
        mock_response_page_2 = {"entities": []}
        mock_api_call.side_effect = [mock_response_page_1, mock_response_page_2]

        self.genesys_instance.api_connection(
            endpoint="routing_queues_members", queues_ids=mock_queues_ids
        )

        self.assertEqual(mock_api_call.call_count, 2)

    @pytest.mark.response
    @patch("viadot.sources.genesys.Genesys._api_call")
    def test_api_connection_users(self, mock_api_call):
        """Test Genesys `api_connection` method with users."""
        mock_queues_ids = ["queue1"]
        mock_response_page_1 = {"entities": [{"userId": "user1", "name": "Agent1"}]}
        mock_response_page_2 = {"entities": []}
        mock_api_call.side_effect = [mock_response_page_1, mock_response_page_2]

        self.genesys_instance.api_connection(
            endpoint="users", queues_ids=mock_queues_ids
        )

        self.assertEqual(mock_api_call.call_count, 2)

    @pytest.mark.functions
    @patch("viadot.sources.genesys.Genesys._handle_if_empty")
    @patch("viadot.sources.genesys.super")
    def test_to_df(self, mock_super, mock_handle_if_empty):
        """Test Genesys `to_df` method."""
        mock_super().to_df = MagicMock()
        mock_handle_if_empty = MagicMock()
        self.genesys_instance.data_returned = {
            0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
            1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
        }

        result_df = self.genesys_instance.to_df()
        result_df.drop(
            columns=["_viadot_source", "_viadot_downloaded_at_utc"],
            inplace=True,
            axis=1,
        )
        expected_df = pd.DataFrame({"A": [1, 2, 2, 3], "B": [3, 4, 4, 5]})

        pd.testing.assert_frame_equal(result_df, expected_df)
        mock_super().to_df.assert_called_once()
        mock_handle_if_empty.assert_not_called()

    @pytest.mark.functions
    @patch("viadot.sources.genesys.Genesys._handle_if_empty")
    @patch("viadot.sources.genesys.super")
    def test_to_df_duplicates(self, mock_super, mock_handle_if_empty):
        """Test Genesys `to_df` method, dropping duplicates."""
        mock_super().to_df = MagicMock()
        mock_handle_if_empty = MagicMock()
        self.genesys_instance.data_returned = {
            0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
            1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
        }

        expected_df_no_duplicates = pd.DataFrame({"A": [1, 2, 3], "B": [3, 4, 5]})
        result_df_no_duplicates = self.genesys_instance.to_df(drop_duplicates=True)
        result_df_no_duplicates.drop(
            columns=["_viadot_source", "_viadot_downloaded_at_utc"],
            inplace=True,
            axis=1,
        )

        pd.testing.assert_frame_equal(
            result_df_no_duplicates, expected_df_no_duplicates
        )
        mock_super().to_df.assert_called_once()
        mock_handle_if_empty.assert_not_called()

    @pytest.mark.functions
    @patch("viadot.sources.genesys.validate")
    @patch("viadot.sources.genesys.Genesys._handle_if_empty")
    @patch("viadot.sources.genesys.super")
    def test_to_df_validate(self, mock_super, mock_handle_if_empty, mock_validate):
        """Test Genesys `to_df` method, checking validation function."""
        mock_super().to_df = MagicMock()
        mock_handle_if_empty = MagicMock()
        self.genesys_instance.data_returned = {
            0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
            1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
        }

        validate_df_dict = {"some_key": "some_value"}
        result_df = self.genesys_instance.to_df(validate_df_dict=validate_df_dict)
        result_df.drop(
            columns=["_viadot_source", "_viadot_downloaded_at_utc"],
            inplace=True,
            axis=1,
        )

        mock_validate.assert_called_once_with(df=result_df, tests=validate_df_dict)
        mock_super().to_df.assert_called_once()
        mock_handle_if_empty.assert_not_called()

    @pytest.mark.functions
    @patch("viadot.sources.genesys.Genesys._handle_if_empty")
    @patch("viadot.sources.genesys.super")
    def test_to_df_empty(self, mock_super, mock_handle_if_empty):
        """Test Genesys `to_df` method, checking empty response."""
        mock_super().to_df = MagicMock()
        mock_handle_if_empty = MagicMock()
        self.genesys_instance.data_returned = {
            0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
            1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
        }

        self.genesys_instance.data_returned = {}
        result_df_empty = self.genesys_instance.to_df()

        self.assertTrue(result_df_empty.empty)
        mock_super().to_df.assert_called_once()
        mock_handle_if_empty.assert_not_called()


if __name__ == "__main__":
    unittest.main()
