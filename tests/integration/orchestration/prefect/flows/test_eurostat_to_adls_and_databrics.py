"""Test flows `test_eurostat_to_databricks` and `test_eurostat_to_adls`."""

from viadot.orchestration.prefect.flows import (
    eurostat_to_adls,
    eurostat_to_databricks,
)
from viadot.sources import AzureDataLake, Databricks

TEST_FILE_PATH = "raw/viadot_2_0_TEST_eurostat.parquet"


def test_eurostat_to_adls():
    """Function for testing uploading data from Eurostat to ADLS."""
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_FILE_PATH)

    eurostat_to_adls(
        dataset_code="ILC_DI04",
        adls_path=TEST_FILE_PATH,
        adls_credentials_secret="sp-adls-test",
    )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)


def test_eurostat_to_databricks():
    """Function for testing uploading data from Eurostat to Databrics."""
    test_schema = "test_viadot_schema"
    test_table = "eurostat_test"

    databricks = Databricks(config_key="databricks-qa-elt")

    assert not databricks._check_if_table_exists(schema=test_schema, table=test_table)

    eurostat_to_databricks(
        dataset_code="ILC_DI04",
        databricks_table=test_table,
        databricks_schema=test_schema,
        databricks_credentials_secret="databricks-qa-nesso-qa",
    )

    assert databricks._check_if_table_exists(schema=test_schema, table=test_table)

    databricks.drop_table(schema=test_schema, table=test_table)

    databricks.session.stop()
