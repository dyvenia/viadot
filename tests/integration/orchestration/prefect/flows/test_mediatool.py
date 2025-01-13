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


def test_mediatool_to_adls(
    VIADOT_TEST_MEDIATOOL_ADLS_AZURE_KEY_VAULT_SECRET,
    VIADOT_TEST_MEDIATOOL_ORG,
    VIADOT_TEST_MEDIATOOL_ADLS_PATH,
    VIADOT_TEST_ADLS_AZURE_KEY_VAULT_SECRET,
):
    state = mediatool_to_adls(
        azure_key_vault_secret=VIADOT_TEST_MEDIATOOL_ADLS_AZURE_KEY_VAULT_SECRET,
        organization_ids=VIADOT_TEST_MEDIATOOL_ORG,
        media_entries_columns=media_entries_columns,
        adls_path=VIADOT_TEST_MEDIATOOL_ADLS_PATH,
        adls_azure_key_vault_secret=VIADOT_TEST_ADLS_AZURE_KEY_VAULT_SECRET,
        adls_path_overwrite=True,
    )

    assert state.is_successful()
