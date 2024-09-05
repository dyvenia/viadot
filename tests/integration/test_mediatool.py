"""'test_mediatool.py'."""

import os

from viadot.orchestration.prefect.flows import mediatool_to_adls

os.system("clear")


organization_ids = [
    "03073189740550375",
    "075472954388631",
]

media_entries_columns = [
    "_id",
    "organizationId",
    "mediaTypeId",
    "vehicleId",
    "startDate",
    "endDate",
    "businessAreaId",
    "campaignObjectiveId",
    "campaignId",
    "netMediaCostChosenCurrency",
    "currencyId",
    "eurExchangeRate",
    "netMediaCostEur",
    "fee",
    "totalFeeOfNetMediaCostEur",
    "totalCostToClientChosenCurrency",
    "totalCostToClientEur",
    "nonBiddableMediaCostEur",
]

mediatool_to_adls(
    azure_key_vault_secret="mediatool-access",
    organization_ids=organization_ids,
    media_entries_columns=media_entries_columns,
    adls_path="raw/dyvenia_sandbox/mediatool/mediatool_trial.parquet",
    adls_azure_key_vault_secret="app-azure-cr-datalakegen2-dev",
    adls_path_overwrite=True,
)
