import os

from viadot.orchestration.prefect.flows import hubspot_to_adls

os.system("clear")

if "__main__" == __name__:
    hubspot_to_adls(
        azure_key_vault_secret="hubspot-access",
        endpoint="hubdb/api/v2/tables/6009756/rows/draft",
        nrows=1000000,
        adls_azure_key_vault_secret="app-azure-cr-datalakegen2",
        adls_path="raw/dyvenia_sandbox/genesys/hubspot_rial.parquet",
        adls_path_overwrite=True,
    )
