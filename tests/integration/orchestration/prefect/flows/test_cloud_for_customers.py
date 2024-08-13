from viadot.orchestration.prefect.flows import (
    cloud_for_customers_to_adls,
    cloud_for_customers_to_databricks,
)
from viadot.sources import AzureDataLake, Databricks


TEST_SCHEMA = "test_viadot_schema"
TEST_TABLE = "test"


def test_cloud_for_customers_to_adls(
    cloud_for_customers_url,
    TEST_FILE_PATH,
    c4c_credentials_secret,
    adls_credentials_secret,
):
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_FILE_PATH)

    cloud_for_customers_to_adls(
        cloud_for_customers_url=cloud_for_customers_url,
        adls_path=TEST_FILE_PATH,
        cloud_for_customers_credentials_secret=c4c_credentials_secret,
        adls_credentials_secret=adls_credentials_secret,
    )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)


def test_cloud_for_customers_to_databricks(
    cloud_for_customers_url, c4c_credentials_secret, databricks_credentials_secret
):
    databricks = Databricks(config_key="databricks-qa-elt")

    assert not databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    cloud_for_customers_to_databricks(
        cloud_for_customers_url=cloud_for_customers_url,
        databricks_table=TEST_TABLE,
        databricks_schema=TEST_SCHEMA,
        cloud_for_customers_credentials_secret=c4c_credentials_secret,
        databricks_credentials_secret=databricks_credentials_secret,
    )

    assert databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.session.stop()
