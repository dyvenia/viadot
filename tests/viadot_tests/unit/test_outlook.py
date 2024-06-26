import os
from datetime import datetime
from unittest import mock

import pandas as pd
import pytest
from O365.account import Account

from viadot.exceptions import CredentialError
from viadot.sources import Outlook


@pytest.fixture
def var_dictionary():
    variables = {
        "mailbox_name": "random@random.com",
        "start_date": "2023-04-12",
        "end_date": "2023-04-13",
        "credentials": {
            "client_id": "abcdefghijklmnopqrstuvwxyz",
            "client_secret": "abcdefghijklmnopqrstuvwxyz",
            "tenant_id": "abcdefghijklmnopqrstuvwxyz",
        },
    }

    return variables


class MockClass:
    mailbox_name = "Trial and error"

    def authenticate():
        return True

    def mailbox():
        return None


class MockValue:
    name = "mailbox"

    def get_messages(limit=1):
        return [MockMessage]


class MockMessage:
    subject = "subject"
    received = "2023-04-12T06:09:59+00:00"
    categories = ["categories"]
    conversation_index = "xxxxxxxxxxxx"

    def to_api_data():
        data = {
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": "random@random.com",
                        "name": "random",
                    }
                },
                {
                    "emailAddress": {
                        "address": "random@random2.com",
                        "name": "random",
                    }
                },
            ],
            "from": {"emailAddress": {"address": "random@random.ee", "name": "name"}},
            "receivedDateTime": "2022-04-01T06:09:59+00:00",
            "conversationId": "bbbb",
        }
        return data


@mock.patch.object(Outlook, "_get_all_folders", return_value={"Mailbox": MockValue})
@mock.patch("viadot.sources.outlook.Account", return_value=MockClass)
@pytest.mark.connect
def test_outlook_api_response(mock_mailbox, mock_connection, var_dictionary):
    o = Outlook(
        credentials=var_dictionary["credentials"],
    )
    o.api_connection(
        mailbox_name=var_dictionary["mailbox_name"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )

    mock_connection.assert_called_once()
    mock_mailbox.assert_called_once()
    assert isinstance(o.data, list)


@mock.patch.object(Outlook, "_get_all_folders", return_value={"Mailbox": MockValue})
@mock.patch("viadot.sources.outlook.Account", return_value=MockClass)
@pytest.mark.response
def test_outlook_api_response_error(mock_mailbox, mock_connection, var_dictionary):
    o = Outlook(
        credentials=var_dictionary["credentials"],
    )
    o.api_connection(
        mailbox_name=var_dictionary["mailbox_name"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )
    df = o.to_df()

    viadot_set = {"_viadot_source", "_viadot_downloaded_at_utc"}

    mock_connection.assert_called_once()
    mock_mailbox.assert_called_once()
    assert set(df.columns).issuperset(viadot_set)
    assert isinstance(df, pd.DataFrame)
