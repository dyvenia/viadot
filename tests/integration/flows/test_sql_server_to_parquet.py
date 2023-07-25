import os

import pytest
from prefect import Flow

from viadot.flows import SQLServerToParquet
from viadot.tasks import SQLServerToDF
from viadot.tasks.sql_server import SQLServerQuery

SCHEMA = "sandbox"
TABLE = "test"
PATH = "test.parquet"


@pytest.fixture(scope="session")
def create_table():
    query_task = SQLServerQuery("AZURE_SQL")
    query_task.run(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}")
    query_task.run(f"CREATE TABLE {SCHEMA}.{TABLE} (Id INT, Name VARCHAR (10))")
    yield True


def test_sql_server_to_parquet_flow(create_table):
    flow = SQLServerToParquet(
        name="test_flow",
        sql_query=f"SELECT * FROM {SCHEMA}.{TABLE}",
        local_file_path=PATH,
        if_exists="fail",
        sqlserver_config_key="AZURE_SQL",
        timeout=3600,
    )
    flow.gen_flow()
    assert isinstance(flow, Flow)
    assert len(flow.tasks) == 3  # Number of tasks in the flow
    tasks = list(flow.tasks)

    assert isinstance(tasks[0], SQLServerToDF)
    flow.run()
    assert os.path.isfile(PATH) == True
    os.remove(PATH)
