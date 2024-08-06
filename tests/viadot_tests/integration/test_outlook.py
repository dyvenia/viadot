import os

from viadot.orchestration.prefect.flows import outlook_to_adls

os.system("clear")

mail_box = "velux.bih@velux.com"


if "__main__" == __name__:
    outlook_to_adls(
        azure_key_vault_secret="outlook-access",
        mailbox_name=mail_box,
        start_date="2023-04-12",
        end_date="2023-04-13",
        adls_azure_key_vault_secret="app-azure-cr-datalakegen2",
        adls_path=f"raw/dyvenia_sandbox/genesys/{mail_box.split('@')[0].replace('.', '_').replace('-', '_')}.csv",
        adls_path_overwrite=True,
    )
