import os

import pytest
from prefect.engine.signals import FAIL

from viadot.tasks import BCPTask
from viadot.tasks.azure_sql import AzureSQLCreateTable, AzureSQLDBQuery

SCHEMA = "sandbox"
TABLE = "test_bcp"
FAIL_TABLE = "nonexistent_table"
ERROR_TABLE = "test_bcp_error"
ERROR_LOG_FILE = "log_file.log"


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


@pytest.fixture(scope="function")
def test_error_table():
    create_table_task = AzureSQLCreateTable()
    create_table_task.run(
        schema=SCHEMA,
        table=ERROR_TABLE,
        dtypes={"country": "INT", "sales": "INT"},
        if_exists="replace",
    )
    yield
    sql_query_task = AzureSQLDBQuery()
    sql_query_task.run(f"DROP TABLE  {SCHEMA}.{ERROR_TABLE};")


def test_bcp(TEST_CSV_FILE_PATH, test_table):

    bcp_task = BCPTask()

    try:
        result = bcp_task.run(
            path=TEST_CSV_FILE_PATH,
            schema=SCHEMA,
            table=TABLE,
            error_log_file_path=ERROR_LOG_FILE,
        )
    except FAIL:
        result = False

    assert result is not False
    os.remove(ERROR_LOG_FILE)


def test_bcp_fail(TEST_CSV_FILE_PATH, test_table):

    bcp_task = BCPTask()

    with pytest.raises(FAIL):
        bcp_task.run(path=TEST_CSV_FILE_PATH, schema=SCHEMA, table=FAIL_TABLE)


def test_bcp_log_error(TEST_CSV_FILE_PATH, test_error_table):
    bcp_task = BCPTask()
    bcp_task.run(
        path=TEST_CSV_FILE_PATH,
        schema=SCHEMA,
        table=ERROR_TABLE,
        error_log_file_path=ERROR_LOG_FILE,
        on_error="skip",
    )
    assert (
        os.path.exists(ERROR_LOG_FILE) is True and os.path.getsize(ERROR_LOG_FILE) != 0
    )
    os.remove(ERROR_LOG_FILE)
