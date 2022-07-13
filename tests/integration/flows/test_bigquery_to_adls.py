import os

import pendulum
from prefect.tasks.secrets import PrefectSecret

from viadot.flows import BigQueryToADLS
from viadot.tasks import AzureDataLakeRemove

ADLS_DIR_PATH = "raw/tests/"
ADLS_FILE_NAME = str(pendulum.now("utc")) + ".parquet"
BIGQ_CREDENTIAL_KEY = "BIGQUERY_TESTS"
ADLS_CREDENTIAL_SECRET = PrefectSecret(
    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
).run()


def test_bigquery_to_adls():
    flow_bigquery = BigQueryToADLS(
        name="Test BigQuery to ADLS extract",
        dataset_name="official_empty",
        table_name="space",
        adls_file_name=ADLS_FILE_NAME,
        credentials_key=BIGQ_CREDENTIAL_KEY,
        adls_dir_path=ADLS_DIR_PATH,
        adls_sp_credentials_secret=ADLS_CREDENTIAL_SECRET,
    )

    result = flow_bigquery.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])

    os.remove("test_bigquery_to_adls_extract.parquet")
    os.remove("test_bigquery_to_adls_extract.json")


def test_bigquery_to_adls_overwrite_true():
    flow_bigquery = BigQueryToADLS(
        name="Test BigQuery to ADLS overwrite true",
        dataset_name="official_empty",
        table_name="space",
        credentials_key=BIGQ_CREDENTIAL_KEY,
        adls_file_name=ADLS_FILE_NAME,
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_sp_credentials_secret=ADLS_CREDENTIAL_SECRET,
    )

    result = flow_bigquery.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])
    os.remove("test_bigquery_to_adls_overwrite_true.parquet")
    os.remove("test_bigquery_to_adls_overwrite_true.json")


def test_bigquery_to_adls_false():
    flow_bigquery = BigQueryToADLS(
        name="Test BigQuery to ADLS overwrite false",
        dataset_name="official_empty",
        table_name="space",
        adls_file_name=ADLS_FILE_NAME,
        overwrite_adls=False,
        credentials_key=BIGQ_CREDENTIAL_KEY,
        adls_dir_path=ADLS_DIR_PATH,
        adls_sp_credentials_secret=ADLS_CREDENTIAL_SECRET,
    )

    result = flow_bigquery.run()
    assert result.is_failed()
    os.remove("test_bigquery_to_adls_overwrite_false.parquet")
    os.remove("test_bigquery_to_adls_overwrite_false.json")
    rm = AzureDataLakeRemove(
        path=ADLS_DIR_PATH + ADLS_FILE_NAME, vault_name="azuwevelcrkeyv001s"
    )
    rm.run(sp_credentials_secret=ADLS_CREDENTIAL_SECRET)
