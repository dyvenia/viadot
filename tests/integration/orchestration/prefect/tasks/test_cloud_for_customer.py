"""Test task for pulling data from CloudForCustomers and loading into ADLS."""

from viadot.orchestration.prefect.flows import cloud_for_customers_to_adls


def test_cloud_for_customers_to_adls():
    state = cloud_for_customers_to_adls(
        report_url="https://my341115.crm.ondemand.com/sap/c4c/odata/ana_businessanalytics_analytics.svc/RPZ36A87743F65355C0B904A5QueryResults?$select=TDOC_PRIORITY",
        filter_params={"CBTD_REF_TYPE_CODE": "(%20eq%20%27118%27)"},
        adls_path="raw/c4c/ticket/leads_link/c4c_tickets_leads_link.parquet",
        overwrite=True,
        cloud_for_customers_credentials_secret="aia-c4c-prod",  # noqa: S106
        adls_credentials_secret="app-azure-cr-datalakegen2",  # noqa: S106
    )
    assert state.is_successful()
