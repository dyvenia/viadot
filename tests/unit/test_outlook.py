from datetime import datetime, timezone
import unittest
from unittest.mock import MagicMock, patch

from O365.mailbox import MailBox
from O365.message import Message
import pandas as pd
import pytest

from viadot.sources import Outlook
from viadot.sources.outlook import OutlookCredentials


variables = {
    "credentials": {
        "client_id": "fake_client_id",
        "client_secret": "fake_client_secret",
        "tenant_id": "fake_tenant_id",
    },
    "response_1": {
        "from": {"emailAddress": {"address": "sender@example.com"}},
        "toRecipients": [{"emailAddress": {"address": "recipient@example.com"}}],
        "receivedDateTime": "2023-07-18T12:34:56Z",
        "conversationId": "12345",
    },
    "expected_1": {
        "(sub)folder": "Inbox",
        "conversation ID": "12345",
        "conversation index": "ConversationIndex",
        "categories": "Category1, Category2",
        "sender": "sender@example.com",
        "subject": "Test Subject",
        "recivers": "recipient@example.com",
        "received_time": "2023-07-18T12:34:56Z",
        "mail_adress": "test",
        "Inbox": True,
    },
}


class TestOutlookCredentials:
    """Test Outlook Credentials Class."""

    def test_outlook_credentials(self):
        """Test Outlook credentials."""
        OutlookCredentials(
            client_id="test_client_id",
            client_secret="test_client_secret",  # noqa: S106
            tenant_id="test_tenant_id",
        )


class TestOutlook(unittest.TestCase):
    """Test Outlook Class."""

    @classmethod
    def setUpClass(cls):  # noqa: ANN206
        """Defined based Outlook Class for the rest of test."""
        cls.outlook_instance = Outlook(credentials=variables["credentials"])

    @patch("viadot.sources.outlook.get_source_credentials", return_value=None)
    def test_missing_credentials(self, mock_get_source_credentials):
        """Test raise error without credentials."""
        with pytest.raises(TypeError):
            Outlook(credentials=None)

        mock_get_source_credentials.assert_called_once()

    @patch("O365.Account.mailbox")
    def test_get_messages_from_mailbox(self, mock_mailbox):
        """Test Outlook `_get_messages_from_mailbox` function."""
        mock_mailbox_obj = MagicMock(spec=MailBox)
        mock_mailbox_obj.name = "Inbox"

        mock_message = MagicMock(spec=Message)
        mock_message.received = "2023-07-18T12:34:56+00:00"
        mock_message.to_api_data.return_value = variables["response_1"]
        mock_message.subject = "Test Subject"
        mock_message.categories = ["Category1", "Category2"]
        mock_message.conversation_index = "ConversationIndex"

        mock_mailbox_obj.get_messages.return_value = [mock_message]
        mock_mailbox.return_value = mock_mailbox_obj

        date_range_start_time = datetime(2023, 7, 17, tzinfo=timezone.utc)
        date_range_end_time = datetime(2023, 7, 19, tzinfo=timezone.utc)

        messages = self.outlook_instance._get_messages_from_mailbox(
            mailbox_name="test@example.com",
            dict_folder={"Inbox": mock_mailbox_obj},
            date_range_start_time=date_range_start_time,
            date_range_end_time=date_range_end_time,
        )

        expected_message = variables["expected_1"]
        assert messages == [expected_message]

    @patch("O365.Account.authenticate", return_value=True)
    @patch("O365.Account.mailbox")
    def test_api_connection(self, mock_mailbox, mock_authenticate):
        """Test Outlook `api_connection` method."""
        mock_mailbox_obj = MagicMock(spec=MailBox)
        mock_mailbox.return_value = mock_mailbox_obj

        self.outlook_instance._get_subfolders = MagicMock(return_value={})

        mailbox_name = "test@example.com"

        self.outlook_instance.api_connection(mailbox_name=mailbox_name)

        self.outlook_instance._get_subfolders.assert_called_once_with(
            {}, mock_mailbox_obj
        )

        mock_authenticate.assert_called_once()

    @patch("O365.Account.authenticate", return_value=False)
    def test_api_connection_authentication_failure(self, mock_authenticate):  # noqa: ARG002
        """Test Outlook `api_connection` method, failure."""
        mailbox_name = "test@example.com"

        with pytest.raises(ValueError):  # noqa: PT011
            self.outlook_instance.api_connection(mailbox_name=mailbox_name)

    @patch("O365.Account.mailbox")
    def test_to_df(self, mock_mailbox):
        """Test Outlook `to_df` function."""
        mock_mailbox_obj = MagicMock(spec=MailBox)
        mock_mailbox_obj.name = "Inbox"

        mock_message = MagicMock(spec=Message)
        mock_message.received = "2023-07-18T12:34:56+00:00"
        mock_message.to_api_data.return_value = {
            "from": {"emailAddress": {"address": "sender@example.com"}},
            "toRecipients": [{"emailAddress": {"address": "recipient@example.com"}}],
            "receivedDateTime": "2023-07-18T12:34:56Z",
            "conversationId": "12345",
        }
        mock_message.subject = "Test Subject"
        mock_message.categories = ["Category1", "Category2"]
        mock_message.conversation_index = "ConversationIndex"

        mock_mailbox_obj.get_messages.return_value = [mock_message]
        mock_mailbox.return_value = mock_mailbox_obj

        date_range_start_time = datetime(2023, 7, 17, tzinfo=timezone.utc)
        date_range_end_time = datetime(2023, 7, 19, tzinfo=timezone.utc)

        self.outlook_instance.data = self.outlook_instance._get_messages_from_mailbox(
            mailbox_name="test@example.com",
            dict_folder={"Inbox": mock_mailbox_obj},
            date_range_start_time=date_range_start_time,
            date_range_end_time=date_range_end_time,
        )

        df = self.outlook_instance.to_df()
        df.drop(
            columns=["_viadot_source", "_viadot_downloaded_at_utc"],
            inplace=True,
            axis=1,
        )

        expected_df = pd.DataFrame(
            [
                {
                    "(sub)folder": "Inbox",
                    "conversation ID": "12345",
                    "conversation index": "ConversationIndex",
                    "categories": "Category1, Category2",
                    "sender": "sender@example.com",
                    "subject": "Test Subject",
                    "recivers": "recipient@example.com",
                    "received_time": "2023-07-18T12:34:56Z",
                    "mail_adress": "test",
                    "Inbox": True,
                }
            ]
        )

        assert df.equals(expected_df)


if __name__ == "__main__":
    unittest.main()
