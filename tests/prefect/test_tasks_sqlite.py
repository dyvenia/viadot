import pytest
import pandas as pd
from viadot.tasks.sqlite_tasks import SQLtoDF, Insert
import os

TABLE = 'test'
DB_PATH = '/home/viadot/tests/testfile_db.sqlite'
SQL_PATH = '/home/viadot/tests/testfile.sql'

@pytest.fixture(scope="session")
def load_table():
    load_table = Insert()
    yield load_table

@pytest.fixture(scope="session")
def TEST_SQL_FILE_PATH():
    return "test_sql.sql"

@pytest.fixture(scope="session", autouse=True)
def create_test_sql_file(TEST_SQL_FILE_PATH):
    with open(TEST_SQL_FILE_PATH, 'w') as sql_file:
        sql_file.write("SELECT * FROM test;")
    yield sql_file
    os.remove(TEST_SQL_FILE_PATH)

@pytest.fixture(scope="session")
def sql_to_df_task(create_test_sql_file):
    sql_to_df_task = SQLtoDF(db_path=DB_PATH, sql_path=create_test_sql_file)  #NoneType
    #sql_to_df_task = SQLtoDF(db_path=DB_PATH, sql_path=TEST_SQL_FILE_PATH)
    yield sql_to_df_task

def test_create_table_from_df(load_table):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    df_data = pd.DataFrame({"country": ['italy'], "sales": [100.]})
    result = load_table.run(table_name=TABLE,
                   schema=None,
                   dtypes=dtypes,
                   db_path=DB_PATH,
                   df=df_data,
                   if_exists="replace"
                   )
    assert result == True

def test_not_select(sql_to_df_task):
    empty_df = pd.DataFrame()
    result = sql_to_df_task.run()
    assert result == empty_df
