import os
from datetime import date, timedelta

from viadot.orchestration.prefect.flows import mindful_to_adls

os.system("clear")

start_date = date.today() - timedelta(days=2)
end_date = start_date + timedelta(days=1)
date_interval = [start_date, end_date]

if "__main__" == __name__:
    mindful_to_adls(
        azure_key_vault_secret="mindful",
        endpoint="responses",
        date_interval=date_interval,
        adls_path="raw/dyvenia_sandbox/mindful",
        adls_azure_key_vault_secret="app-azure-cr-datalakegen2",
        adls_path_overwrite=True,
    )
