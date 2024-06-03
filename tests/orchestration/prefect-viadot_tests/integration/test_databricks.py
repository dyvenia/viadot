import contextlib

import pytest
from prefect import flow
from viadot.exceptions import TableDoesNotExist

try:
    from viadot.sources import Databricks

    _databricks_installed = True
except ImportError:
    _databricks_installed = False

if not _databricks_installed:
    pytest.skip("Databricks source not installed", allow_module_level=True)

from orchestration.prefect_viadot.tasks import df_to_databricks

TEST_SCHEMA = "test_viadot_schema"
TEST_TABLE = "test"


@pytest.fixture(scope="session", autouse=True)
def databricks() -> Databricks:
    databricks = Databricks(config_key="databricks-qa-elt")
    databricks.create_schema(TEST_SCHEMA)

    yield databricks

    with contextlib.suppress(TableDoesNotExist):
        databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    with contextlib.suppress(Exception):
        databricks.drop_schema(TEST_SCHEMA)

    databricks.session.stop()


def test_df_to_databricks(
    TEST_DF, databricks: Databricks, databricks_credentials_secret
):
    @flow
    def test_flow():
        return df_to_databricks(
            df=TEST_DF,
            schema=TEST_SCHEMA,
            table=TEST_TABLE,
            credentials_secret=databricks_credentials_secret,
        )

    assert not databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    test_flow()

    assert databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
