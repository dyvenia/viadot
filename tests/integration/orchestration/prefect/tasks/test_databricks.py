import contextlib

from prefect import flow
import pytest

from viadot.exceptions import TableDoesNotExistError
from viadot.utils import skip_test_on_missing_extra


try:
    from viadot.sources import Databricks
except ImportError:
    skip_test_on_missing_extra(source_name="Databricks", extra="databricks")

from viadot.orchestration.prefect.tasks import df_to_databricks


TEST_SCHEMA = "test_viadot_schema"
TEST_TABLE = "test"


@pytest.fixture(scope="session", autouse=True)
def databricks() -> Databricks:
    databricks = Databricks(config_key="databricks-qa-elt")
    databricks.create_schema(TEST_SCHEMA)

    yield databricks

    with contextlib.suppress(TableDoesNotExistError):
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
