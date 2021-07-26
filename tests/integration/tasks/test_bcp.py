import pytest
from prefect.engine.signals import FAIL

from viadot.tasks import BCPTask
from viadot.tasks.azure_sql import AzureSQLCreateTable, AzureSQLDBQuery

SCHEMA = "sandbox"
TABLE = "test_bcp"
FAIL_TABLE = "nonexistent_table"


@pytest.fixture(scope="function")
def test_table():
    create_table_task = AzureSQLCreateTable()
    create_table_task.run(
        schema=SCHEMA,
        table=TABLE,
        dtypes={"country": "VARCHAR(25)", "sales": "INT"},
        if_exists="replace",
    )
    yield
    sql_query_task = AzureSQLDBQuery()
    sql_query_task.run(f"DROP TABLE {SCHEMA}.{TABLE};")


def test_bcp(TEST_CSV_FILE_PATH, test_table):

    bcp_task = BCPTask()

    try:
        result = bcp_task.run(path=TEST_CSV_FILE_PATH, schema=SCHEMA, table=TABLE)
    except FAIL:
        result = False

    assert result is not False


def test_bcp_fail(TEST_CSV_FILE_PATH, test_table):

    bcp_task = BCPTask()

    with pytest.raises(FAIL):
        bcp_task.run(path=TEST_CSV_FILE_PATH, schema=SCHEMA, table=FAIL_TABLE)
