from prefect.tasks.secrets import PrefectSecret
from viadot.tasks.outlook import OutlookToDF
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
import json


def test_outlook_to_df_task():
    task = OutlookToDF()
    credentials_secret = PrefectSecret("OUTLOOK_KEYVAULT").run()
    vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()
    credentials_str = AzureKeyVaultSecret(
        credentials_secret, vault_name=vault_name
    ).run()
    credentials = json.loads(credentials_str)
    df = task.run(
        credentials=credentials,
        mailbox_name=credentials["mail_example"],
        start_date="2022-06-28",
        end_date="2022-06-29",
    )

    assert df.shape[1] == 9
    assert df.empty == False
