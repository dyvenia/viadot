import os
import pytest
from unittest import mock
from datetime import datetime

import pandas as pd
from O365.account import Account

from viadot.sources import Outlook
from viadot.exceptions import CredentialError

os.system("clear")

# o = Outlook(
#     mailbox_name="vevoszolgalat@velux.com",
#     start_date="2023-02-12",
#     end_date="2023-02-13",
# )

# o.get_all_mails_to_df()


@pytest.fixture
def var_dictionary():
    variables = {
        "mailbox_name": "random@random.com",
        "start_date": "2023-01-12",
        "end_date": "2023-01-13",
    }

    return variables


@pytest.mark.basics
def test_outlook_credentials(var_dictionary):
    o = Outlook(mailbox_name=var_dictionary["mailbox_name"])

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
            credentials=[9],
        )


@mock.patch(
    "viadot.sources.outlook.Outlook._get_messages_from_mailbox", return_value=("a", 1)
)
@pytest.mark.to_df
def test_outlook_to_df(mock_api_response, var_dictionary):
    o = Outlook(
        mailbox_name=var_dictionary["mailbox_name"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )

    df = o.get_all_mails_to_df()
    assert isinstance(df, pd.DataFrame)


@pytest.mark.to_csv
def test_outlook_to_csv(var_dictionary):
    o = Outlook(
        mailbox_name=var_dictionary["mailbox_name"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )

    file_name = (
        var_dictionary["mailbox_name"].split("@")[0].replace(".", "_").replace("-", "_")
    )
    o.to_csv(pd.read_csv(f"{file_name}.csv", sep="\t"))
    assert os.path.exists(f"{file_name}.csv")
