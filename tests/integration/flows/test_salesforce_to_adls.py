from viadot.flows import SalesforceToADLS
from prefect.tasks.secrets import PrefectSecret
import os


def test_salesforce_to_adls():
    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
    ).run()

    flow = SalesforceToADLS(
        "test_salesforce_to_adls_run_flow",
        query="SELECT IsDeleted, FiscalYear FROM Opportunity LIMIT 50",
        adls_sp_credentials_secret=credentials_secret,
        adls_dir_path="raw/tests/test_salesforce.csv",
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_salesforce_to_adls_run_flow.parquet")
    os.remove("test_salesforce_to_adls_run_flow.json")
