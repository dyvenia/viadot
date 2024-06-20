from viadot.orchestration.prefect.flows import (
    eurostat_to_adls,
    eurostat_to_databricks,
)
from viadot.sources import AzureDataLake, Databricks

TEST_FILE_PATH = "raw/viadot_2_0_TEST_eurostat.parquet"


def test_eurostat_to_adls():
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
    TEST_SCHEMA = "test_viadot_schema"
    TEST_TABLE = "eurostat_test"

    databricks = Databricks(config_key="databricks-qa-elt")

    assert not databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    eurostat_to_databricks(
        dataset_code="ILC_DI04",
        databricks_table=TEST_TABLE,
        databricks_schema=TEST_SCHEMA,
        databricks_credentials_secret="databricks-qa-nesso-qa",
    )

    assert databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.session.stop()
