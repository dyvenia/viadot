import logging
import pandas as pd
import pytest
from viadot.exceptions import ValidationError

from viadot.tasks import AzureSQLCreateTable, AzureSQLDBQuery, ChangeColumnOrder

logger = logging.getLogger(__name__)

SCHEMA = "sandbox"
TABLE = "test"


def test_azure_sql_create_table():

    create_table_task = AzureSQLCreateTable()

    create_table_task.run(
        schema=SCHEMA,
        table=TABLE,
        dtypes={"id": "INT", "name": "VARCHAR(25)"},
        if_exists="replace",
    )


def test_azure_sql_run_sqldb_query_empty_result():

    sql_query_task = AzureSQLDBQuery()

    list_table_info_query = f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
            ON t.schema_id = s.schema_id
        WHERE s.name = '{SCHEMA}' AND t.name = '{TABLE}'
    """
    exists = bool(sql_query_task.run(list_table_info_query))
    assert exists

    result = sql_query_task.run(f"SELECT * FROM {SCHEMA}.{TABLE}")
    assert result == []


def test_azure_sql_run_insert_query():

    sql_query_task = AzureSQLDBQuery()

    sql_query_task.run(f"INSERT INTO {SCHEMA}.{TABLE} VALUES (1, 'Mike')")
    result = list(sql_query_task.run(f"SELECT * FROM {SCHEMA}.{TABLE}")[0])
    assert result == [1, "Mike"]


def test_azure_sql_run_drop_query():

    sql_query_task = AzureSQLDBQuery()

    result = sql_query_task.run(f"DROP TABLE {SCHEMA}.{TABLE}")
    assert result is True

    list_table_info_query = f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
            ON t.schema_id = s.schema_id
        WHERE s.name = '{SCHEMA}' AND t.name = '{TABLE}'
    """
    exists = bool(sql_query_task.run(list_table_info_query))
    assert not exists


def test_change_column_order_check_append(caplog):
    create_table_task = AzureSQLCreateTable()
    with caplog.at_level(logging.INFO):
        create_table_task.run(
            schema=SCHEMA,
            table=TABLE,
            dtypes={"id": "INT", "name": "VARCHAR(25)", "street": "VARCHAR(25)"},
            if_exists="replace",
        )
    assert "Successfully created table sandbox" in caplog.text

    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)

    check_column_order = ChangeColumnOrder()
    with pytest.raises(
        ValidationError,
        match=r"The columns differ in the SQL table and the file being loaded.",
    ):
        check_column_order.run(table=TABLE, if_exists="append", df=df)
