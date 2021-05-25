import pytest
import pandas as pd
from viadot.tasks.sqlite_tasks import SQLtoDF, Insert
import os

TABLE = 'test'
DB_PATH = '/home/viadot/tests/testfile_db.sqlite'
SQL_PATH_SELECT = '/home/viadot/tests/testfile_select.sql'
SQL_PATH_NOTSELECT = '/home/viadot/tests/testfile_notselect.sql'

@pytest.fixture(scope="session")
def load_table():
    load_table = Insert()
    yield load_table

@pytest.fixture(scope="session")
def create_test_sql_file_notselect():
    with open(SQL_PATH_NOTSELECT, 'w') as sql_file:
        sql_file.write("INSERT ...")
    yield
    os.remove(SQL_PATH_NOTSELECT)

@pytest.fixture(scope="session")
def create_test_sql_file_select():
    with open(SQL_PATH_SELECT, 'w') as sql_file:
        sql_file.write("SELECT * FROM test;")
    yield
    os.remove(SQL_PATH_SELECT)

@pytest.fixture(scope="session")
def sql_to_df_task_notselect(create_test_sql_file_notselect):
    sql_to_df_task_notselect = SQLtoDF(db_path=DB_PATH, sql_path=SQL_PATH_NOTSELECT)
    yield sql_to_df_task_notselect

@pytest.fixture(scope="session")
def sql_to_df_task_select(create_test_sql_file_select):
    sql_to_df_task_select = SQLtoDF(db_path=DB_PATH, sql_path=SQL_PATH_SELECT)
    yield sql_to_df_task_select

def test_query_select(sql_to_df_task_select, load_table):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    df_data = pd.DataFrame({"country": ['italy'], "sales": [100.]})
    table = load_table.run(table_name=TABLE,
                   schema=None,
                   dtypes=dtypes,
                   db_path=DB_PATH,
                   df=df_data,
                   if_exists="replace"
                   )
    table_from_query = sql_to_df_task_select.run()
    assert table == True
    assert not table_from_query.empty

def test_query_not_select(sql_to_df_task_notselect):
    table_from_query = sql_to_df_task_notselect.run()
    assert table_from_query.empty
