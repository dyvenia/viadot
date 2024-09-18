"""'test_mediatool.py'."""

from viadot.orchestration.prefect.flows import mediatool_to_adls

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


def test_genesys_to_adls(azure_key_vault_secret, adls_azure_key_vault_secret):
    state = mediatool_to_adls(
        azure_key_vault_secret=azure_key_vault_secret,
        organization_ids=organization_ids,
        media_entries_columns=media_entries_columns,
        adls_path="raw/dyvenia_sandbox/mediatool/mediatool_trial.parquet",
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_path_overwrite=True,
    )

    assert state.is_successful()
