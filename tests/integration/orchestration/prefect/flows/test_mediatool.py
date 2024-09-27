"""'test_mediatool.py'."""

from viadot.orchestration.prefect.flows import mediatool_to_adls


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


def test_genesys_to_adls(
    MEDIATOOL_ADLS_AZURE_KEY_VAULT_SECRET,
    MEDIATOOL_TESTS_ORG,
    MEDIATOOL_TEST_ADLS_PATH,
    ADLS_AZURE_KEY_VAULT_SECRET,
):
    state = mediatool_to_adls(
        azure_key_vault_secret=MEDIATOOL_ADLS_AZURE_KEY_VAULT_SECRET,
        organization_ids=MEDIATOOL_TESTS_ORG,
        media_entries_columns=media_entries_columns,
        adls_path=MEDIATOOL_TEST_ADLS_PATH,
        adls_azure_key_vault_secret=ADLS_AZURE_KEY_VAULT_SECRET,
        adls_path_overwrite=True,
    )

    assert state.is_successful()
