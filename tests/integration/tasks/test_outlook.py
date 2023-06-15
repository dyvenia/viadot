from unittest import mock

import pandas as pd
import pytest

from viadot.config import local_config
from viadot.sources import Outlook
from viadot.tasks import OutlookToDF

outlook_env_vars = local_config.get("OUTLOOK")


def to_df():
    test_df = pd.DataFrame(
        data={"country": ["italy", "germany", "spain"], "sales": [100, 50, 80]}
    )

    assert isinstance(test_df, pd.DataFrame)

    return test_df


@pytest.mark.init
def test_outlook_to_df():
    outlook = Outlook(
        mailbox_name=outlook_env_vars["mail_example"],
        credentials=outlook_env_vars,
        start_date="2022-04-28",
        end_date="2022-04-29",
    )
    df = outlook.get_all_mails_to_df()

    assert isinstance(df, pd.DataFrame)


@pytest.mark.init
@pytest.mark.dependency(depends=["test_outlook_to_df"])
def test_outlook_credentials():
    outlook_env_vars = local_config.get("OUTLOOK")
    outlook = Outlook(
        mailbox_name=outlook_env_vars["mail_example"], credentials=outlook_env_vars
    )

    assert isinstance(outlook.credentials, dict)


@mock.patch("O365.Account", return_value=None)
@mock.patch("O365.Account.authenticate", return_value="trial1")
@mock.patch("O365.Account.mailbox", return_value="trial2")
@mock.patch("viadot.sources.Outlook.get_all_mails_to_df", return_calue=to_df)
@pytest.mark.task
def test_outlook_task(mock_account, mock_auth, mock_mail, mock_to_df):
    outlook_to_df = OutlookToDF(credentials=outlook_env_vars)
    df = outlook_to_df.run(
        mailbox_name=outlook_env_vars["mail_example"],
    )

    assert df != None
    mock_account.assert_called()
    mock_auth.assert_called()
    mock_mail.assert_called()
