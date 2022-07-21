import os

from prefect.tasks.secrets import PrefectSecret

from viadot.flows import SalesforceToADLS
from viadot.tasks import AzureDataLakeRemove

ADLS_FILE_NAME = "test_salesforce.parquet"
ADLS_DIR_PATH = "raw/tests/"


def test_salesforce_to_adls():

    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
    ).run()

    flow = SalesforceToADLS(
        "test_salesforce_to_adls_run_flow",
        query="SELECT IsDeleted, FiscalYear FROM Opportunity LIMIT 50",
        adls_sp_credentials_secret=credentials_secret,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
    )

    result = flow.run()
    assert result.is_successful()

    os.remove("test_salesforce_to_adls_run_flow.parquet")
    os.remove("test_salesforce_to_adls_run_flow.json")
    rm = AzureDataLakeRemove(
        path=ADLS_DIR_PATH + ADLS_FILE_NAME,
        vault_name="azuwevelcrkeyv001s",
    )
    rm.run(sp_credentials_secret=credentials_secret)
