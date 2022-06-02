import pytest

from viadot.flows import AzureSQLTransform
from viadot.tasks import AzureSQLDBQuery

SCHEMA = "sandbox"
TABLE = "test"
FQN = f"{SCHEMA}.{TABLE}"


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
