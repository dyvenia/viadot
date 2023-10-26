import os
from unittest import mock
from datetime import date, timedelta
from prefect.tasks.secrets import PrefectSecret

import pandas as pd
import pytest

from viadot.flows import OutlookToADLS
from viadot.tasks import AzureDataLakeRemove

ADLS_CREDENTIAL_SECRET = PrefectSecret(
    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
).run()
ADLS_FILE_NAME = "test_outlook_to_adls.parquet"
ADLS_DIR_PATH = "raw/tests/"

start_date = date.today() - timedelta(days=1)
start_date = start_date.strftime("%Y-%m-%d")
end_date = date.today().strftime("%Y-%m-%d")

mailbox_list = [
    "romania.tehnic@velux.com",
]

DATA = {
    "sender": ["sender@mail.com"],
    "receivers": ["receiver@mail.com"],
}


def test_outlook_to_adls_flow_run():
    flow = OutlookToADLS(
        name="Test OutlookToADLS flow run",
        mailbox_list=mailbox_list,
        outbox_list=["Outbox", "Sent Items"],
        start_date=start_date,
        end_date=end_date,
        local_file_path=ADLS_FILE_NAME,
        adls_file_path=ADLS_DIR_PATH + ADLS_FILE_NAME,
        adls_sp_credentials_secret=ADLS_CREDENTIAL_SECRET,
        if_exists="replace",
        timeout=4400,
    )

    result = flow.run()
    assert result.is_successful()


def test_outlook_to_adls_run_flow_validate_fail():
    flow = OutlookToADLS(
        name="Test OutlookToADLS validate flow df fail",
        mailbox_list=mailbox_list,
        outbox_list=["Outbox", "Sent Items"],
        start_date=start_date,
        end_date=end_date,
        local_file_path=ADLS_FILE_NAME,
        adls_file_path=ADLS_DIR_PATH + ADLS_FILE_NAME,
        adls_sp_credentials_secret=ADLS_CREDENTIAL_SECRET,
        if_exists="replace",
        validation_df_dict={"column_list_to_match": ["test", "wrong", "columns"]},
        timeout=4400,
    )

    result = flow.run()
    assert result.is_failed()


@mock.patch(
    "viadot.tasks.OutlookToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_outlook_to_adls_run_flow_validate_success(mocked_task):
    flow = OutlookToADLS(
        name="Test OutlookToADLS validate flow df success",
        mailbox_list=mailbox_list,
        outbox_list=["Outbox", "Sent Items"],
        start_date=start_date,
        end_date=end_date,
        local_file_path=ADLS_FILE_NAME,
        adls_file_path=ADLS_DIR_PATH + ADLS_FILE_NAME,
        adls_sp_credentials_secret=ADLS_CREDENTIAL_SECRET,
        if_exists="replace",
        validation_df_dict={"column_list_to_match": ["sender", "receivers"]},
        timeout=4400,
    )

    result = flow.run()
    assert result.is_successful()

    os.remove("test_outlook_to_adls.parquet")
    os.remove("romania_tehnic.csv")
    os.remove("o365_token.txt")
    rm = AzureDataLakeRemove(
        path=ADLS_DIR_PATH + ADLS_FILE_NAME, vault_name="azuwevelcrkeyv001s"
    )
    rm.run(sp_credentials_secret=ADLS_CREDENTIAL_SECRET)
