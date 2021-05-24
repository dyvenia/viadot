import pytest
import pandas as pd
from viadot.tasks.sqlite_tasks import SQLtoDF, Insert

TABLE = 'test'
DB_PATH = '/home/viadot/tests/testfile_db.sqlite'
SQL_PATH = '/home/viadot/tests/testfile.sql'

@pytest.fixture(scope="session")
def load_table():
    load_table = Insert()
    yield load_table


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

@pytest.fixture(scope="session")
def run_sql():
    run_sql = SQLtoDF(db_path=DB_PATH, sql_path=SQL_PATH)
    yield run_sql

def test_not_select(run_sql):
    empty_df = pd.DataFrame()
    result = run_sql.run()

    assert result == empty_df
