import pytest
from viadot.flows.sql_server_transform import SQLServerTransform
from viadot.tasks.sql_server import SQLServerQuery

SCHEMA = "sandbox"
TABLE = "test_sql_server"


def test_sql_server_transform():
    query_task = SQLServerQuery("AZURE_SQL")
    query_task.run(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}")
    flow = SQLServerTransform(
        name="test flow sql transform",
        config_key="AZURE_SQL",
        query=f"CREATE TABLE {SCHEMA}.{TABLE} (Id INT, Name VARCHAR (10))",
    )
    result = flow.run()
    assert result.is_successful()
    query_task.run(f"DROP TABLE {SCHEMA}.{TABLE}")


def test_sql_server_transfor_fail():
    flow = SQLServerTransform(
        name="test flow sql transform",
        config_key="AZURE_SQL",
        query=f"CREATE TABLE {SCHEMA}.{TABLE}",
    )

    result = flow.run()
    assert result.is_failed()
