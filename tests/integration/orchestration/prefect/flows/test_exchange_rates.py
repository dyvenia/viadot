from viadot.orchestration.prefect.flows import (
    exchange_rates_to_adls,
    exchange_rates_to_databricks,
)
from viadot.sources import AzureDataLake, Databricks


TEST_SCHEMA = "test_viadot_schema"
TEST_TABLE = "test"


def test_exchange_rates_to_adls(
    TEST_FILE_PATH, exchange_rates_config_key, adls_credentials_secret
):
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_FILE_PATH)

    exchange_rates_to_adls(
        adls_path=TEST_FILE_PATH,
        exchange_rates_config_key=exchange_rates_config_key,
        adls_credentials_secret=adls_credentials_secret,
    )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)


def test_exchange_rates_to_databricks(
    exchange_rates_config_key, databricks_credentials_secret
):
    databricks = Databricks(config_key="databricks-qa-elt")
    assert not databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    exchange_rates_to_databricks(
        databricks_schema=TEST_SCHEMA,
        databricks_table=TEST_TABLE,
        exchange_rates_config_key=exchange_rates_config_key,
        databricks_credentials_secret=databricks_credentials_secret,
    )

    assert databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.session.stop()
