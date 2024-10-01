import pytest

from viadot.orchestration.prefect.tasks import (
    create_sql_server_table,
    sql_server_query,
    sql_server_to_df,
)
from viadot.sources import SQLServer


TABLE = "test"
SCHEMA = "sandbox"


@pytest.fixture
def sql_server():
    # Initialize the SQLServer instance with the test credentials.
    return SQLServer(config_key="sql_server")


def test_sql_server_to_df():
    df = sql_server_to_df(
        query="""
                SELECT t.name as table_name
                ,s.name as schema_name
                FROM sys.tables t
                JOIN sys.schemas s
                ON t.schema_id = s.schema_id""",
        credentials_secret="sql-server",  # noqa: S106
    )

    assert not df.empty


def test_create_sql_server_table(sql_server):
    dtypes = {
        "date": "DATE",
        "name": "VARCHAR(255)",
        "id": "VARCHAR(255)",
        "weather": "FLOAT(24)",
        "rain": "FLOAT(24)",
        "temp": "FLOAT(24)",
        "summary": "VARCHAR(255)",
    }
    create_sql_server_table(
        table=TABLE,
        schema=SCHEMA,
        dtypes=dtypes,
        if_exists="replace",
        credentials_secret="sql-server",  # noqa: S106
    )

    assert sql_server.exists(table=TABLE, schema=SCHEMA)

    sql_server_query(
        query=f"""DROP TABLE {SCHEMA}.{TABLE}""",
        credentials_secret="sql-server",  # noqa: S106
    )

    assert not sql_server.exists(table=TABLE, schema=SCHEMA)
