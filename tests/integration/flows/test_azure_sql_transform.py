import pytest
import pyodbc

from viadot.flows import AzureSQLTransform
from viadot.tasks import AzureSQLDBQuery


SCHEMA = "sandbox"
TABLE = "test1"
FQN = f"{SCHEMA}.{TABLE}"

QUERY = """SELECT * FROM sandbox.test_if_failed;
    CREATE TABLE sandbox.test_if_failed (id INT, name VARCHAR(25));
    INSERT INTO sandbox.test_if_failed VALUES (1, 'Mike');
    DROP TABLE sandbox.test_if_failed;
   """


@pytest.fixture()
def TEST_TABLE():
    run_sql_task = AzureSQLDBQuery()
    run_sql_task.run(f"CREATE TABLE {FQN} (id INT, name VARCHAR(25))")
    run_sql_task.run(f"INSERT INTO {FQN} VALUES (1, 'Mike')")
    yield
    run_sql_task.run(f"DROP TABLE {FQN}")


def test_azure_sql_transform(TEST_TABLE):
    flow = AzureSQLTransform(
        name="test",
        query=f"SELECT * FROM {FQN}",
    )
    result = flow.run()
    task = list(result.result.keys())[0]
    actual_result = result.result[task].result[0]
    assert list(actual_result) == [1, "Mike"]


def test_azure_sql_transform_if_failed_skip(caplog):
    flow = AzureSQLTransform(name="test", query=QUERY, if_failed="skip")
    result = flow.run()
    assert result
    assert "Following query failed" in caplog.text


def test_azure_sql_transform_if_failed_break(caplog):
    flow = AzureSQLTransform(name="test", query=QUERY, if_failed="break")
    flow.run()
    assert "pyodbc.ProgrammingError" in caplog.text
