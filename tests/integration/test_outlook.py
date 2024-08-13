from viadot.config import get_source_config
from viadot.orchestration.prefect.flows import outlook_to_adls


def test_outlook_to_adls():
    mailbox = get_source_config("outlook").get("mailbox")
    state = outlook_to_adls(
        azure_key_vault_secret="outlook-access",  # noqa: S106
        mailbox_name=mailbox,
        start_date="2023-04-12",
        end_date="2023-04-13",
        adls_azure_key_vault_secret="app-azure-cr-datalakegen2",  # noqa: S106
        adls_path=f"raw/dyvenia_sandbox/genesys/{mailbox.split('@')[0].replace('.', '_').replace('-', '_')}.csv",
        adls_path_overwrite=True,
    )
    assert state.is_successful()
