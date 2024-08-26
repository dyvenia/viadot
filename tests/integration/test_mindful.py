from datetime import date, timedelta

from viadot.orchestration.prefect.flows import mindful_to_adls


start_date = date.today() - timedelta(days=2)
end_date = start_date + timedelta(days=1)
date_interval = [start_date, end_date]


def test_mindful_to_adls(mindful_config_key, adls_credentials_secret):
    state = mindful_to_adls(
        azure_key_vault_secret=mindful_config_key,
        endpoint="responses",
        date_interval=date_interval,
        adls_path="raw/dyvenia_sandbox/mindful",
        adls_azure_key_vault_secret=adls_credentials_secret,
        adls_path_overwrite=True,
    )
    assert state.is_successful()
