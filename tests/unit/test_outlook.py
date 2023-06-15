import os
import pytest
from unittest import mock
from datetime import datetime

import pandas as pd
from O365.account import Account

from viadot.sources import Outlook
from viadot.exceptions import CredentialError


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
