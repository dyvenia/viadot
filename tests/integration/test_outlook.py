from viadot.config import get_source_config
from viadot.orchestration.prefect.flows import outlook_to_adls


def test_outlook_to_adls(adls_credentials_secret, outlook_config_key):
    mailbox = get_source_config("outlook").get("mailbox")
    state = outlook_to_adls(
        config_key=outlook_config_key,
        mailbox_name=mailbox,
        start_date="2023-04-12",
        end_date="2023-04-13",
        adls_azure_key_vault_secret=adls_credentials_secret,
        adls_path=f"raw/dyvenia_sandbox/genesys/{mailbox.split('@')[0].replace('.', '_').replace('-', '_')}.csv",
        adls_path_overwrite=True,
    )
    assert state.is_successful()
