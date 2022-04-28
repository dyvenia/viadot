from viadot.flows import BigQueryToADLS
from prefect.tasks.secrets import PrefectSecret


def test_bigquery_to_adls():
    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
    ).run()

    flow_bigquery = BigQueryToADLS(
        name="BigQuery to ADLS",
        dataset_name="official_empty",
        table_name="space",
        credentials_key="BIGQUERY_TESTS",
        adls_dir_path="raw/tests",
        adls_sp_credentials_secret=credentials_secret,
    )

    result = flow_bigquery.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])
