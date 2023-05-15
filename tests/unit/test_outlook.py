import os
import pytest
from datetime import datetime

import pandas as pd
from O365.account import Account

from viadot.sources import Outlook

from viadot.task_utils import credentials_loader
from viadot.exceptions import CredentialError

credentials = credentials_loader.run(credentials_secret="OUTLOOK")


@pytest.fixture
def var_dictionary():
    variables = {
        "mailbox_name": "vevoszolgalat@velux.com",
        "start_date": "2023-04-12",
        "end_date": "2023-04-13",
    }

    return variables


@pytest.mark.basics
def test_outlook_credentials(var_dictionary):
    o = Outlook(mailbox_name=var_dictionary["mailbox_name"], credentials=credentials)

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


@pytest.mark.to_csv
def test_outlook_to_csv(var_dictionary):
    o = Outlook(
        mailbox_name=var_dictionary["mailbox_name"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
        credentials=credentials,
    )
    df = o.get_all_mails_to_df()
    o.to_csv(df)
    file_name = (
        var_dictionary["mailbox_name"].split("@")[0].replace(".", "_").replace("-", "_")
    )
    assert os.path.exists(f"{file_name}.csv")
