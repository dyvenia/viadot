from prefect.tasks.secrets import PrefectSecret
from viadot.tasks.outlook import OutlookToDF


def test_outlook_to_df_task():
    task = OutlookToDF()
    credentials_secret = PrefectSecret("OUTLOOK_KEYVAULT").run()
    df = task.run(
        credentials=credentials_secret,
        mailbox_name=credentials_secret["mail_example"],
        start_date="2022-06-28",
        end_date="2022-06-29",
    )

    assert df.shape[1] == 9
    assert df.empty == False
