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
        "final_dict_folders": {"Mailbox": MockValue},
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
    conversation_index = "aaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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


@pytest.mark.basics
def test_outlook_credentials(var_dictionary):
    o = Outlook(
        mailbox_name=var_dictionary["mailbox_name"],
        credentials=var_dictionary["credentials"],
    )

    assert all(
        [
            isinstance(o.credentials, dict),
            isinstance(o.date_range_end_time, datetime),
            isinstance(o.date_range_start_time, datetime),
            isinstance(o.account, Account),
        ]
    )


@pytest.mark.basics
@mock.patch("viadot.sources.outlook.Account", return_value=MockClass)
def test_outlook_mailbox_limit(mock_api_Account, var_dictionary):
    o = Outlook(
        mailbox_name=var_dictionary["mailbox_name"],
        credentials=var_dictionary["credentials"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )
    data = o._get_messages_from_mailbox(
        var_dictionary["final_dict_folders"], address_limit=20
    )
    assert len(data[0]["recivers"]) <= 20


@pytest.mark.basics
@mock.patch("viadot.sources.outlook.Account", return_value=MockClass)
def test_outlook_mailbox_space(mock_api_Account, var_dictionary):
    o = Outlook(
        mailbox_name=var_dictionary["mailbox_name"],
        credentials=var_dictionary["credentials"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )
    data = o._get_messages_from_mailbox(
        var_dictionary["final_dict_folders"], address_limit=5
    )
    assert len(data[0]["recivers"]) == 0


@pytest.mark.exceptions
def test_outlook_credential_exception(var_dictionary):
    with pytest.raises(CredentialError):
        o = Outlook(
            mailbox_name=var_dictionary["mailbox_name"],
            start_date=var_dictionary["start_date"],
            end_date=var_dictionary["end_date"],
        )


@pytest.mark.exceptions
def test_outlook_mailbox_folders_exception(var_dictionary):
    with pytest.raises(Exception):
        o = Outlook(
            mailbox_name=var_dictionary["mailbox_name"],
            start_date=var_dictionary["start_date"],
            end_date=var_dictionary["end_date"],
            mailbox_folders=["trial"],
        )


@mock.patch.object(Outlook, "get_all_mails_to_df", return_value=pd.DataFrame())
@mock.patch("viadot.sources.outlook.Account", return_value=MockClass)
@pytest.mark.to_csv
def test_outlook_to_csv(mock_method_Outlook, mock_api_Account, var_dictionary):
    o = Outlook(
        mailbox_name=var_dictionary["mailbox_name"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
        credentials=var_dictionary["credentials"],
    )
    df = o.get_all_mails_to_df()
    o.to_csv(df)
    file_name = (
        var_dictionary["mailbox_name"].split("@")[0].replace(".", "_").replace("-", "_")
    )
    mock_method_Outlook.assert_called()
    mock_api_Account.assert_called()
    assert os.path.exists(f"{file_name}.csv")
