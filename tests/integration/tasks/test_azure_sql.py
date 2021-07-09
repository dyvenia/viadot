from viadot.tasks import AzureSQLCreateTable, RunAzureSQLDBQuery

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


def test_azure_sql_run_sqldb_query():
    run_sql_task = RunAzureSQLDBQuery()
    run_sql_task.run(f"INSERT INTO {SCHEMA}.{TABLE} VALUES (1, 'Mike')")
    result = list(run_sql_task.run(f"SELECT * FROM {SCHEMA}.{TABLE}")[0])
    assert result == [1, "Mike"]


# def test_azure_sql_drop():
#     drop_table_task = AzureSQLDropTable()
#     dropped = drop_table_task.run(schema=SCHEMA, table=TABLE)
#     assert dropped is True
