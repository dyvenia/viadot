import pytest
import pandas
from viadot.tasks.sqlite_tasks import RunSQL, LoadDF
from collections import defaultdict
import pandas as pd
import os

TABLE = "test"
DB_PATH = ':memory:'
SQL_PATH = "home/viadot/tests/testfile.sql"

@pytest.fixture(scope="session")
def load_table():
    load_table = LoadDF()
    yield load_table


def test_create_table_from_df(load_table):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    data = defaultdict(list)
    data["country"].append('italy')
    data["sales"].append(100.0)
    df_data = pd.DataFrame(data)
    df = load_table.run(table_name=TABLE,
                   schema=None,
                   dtypes=dtypes,
                   db_path=DB_PATH,
                   df=df_data,
                   if_exists="replace"
                   )
    assert df == True

def test_run_sql():
    run_sql = RunSQL(db_path=DB_PATH, sql_path=SQL_PATH)
    yield run_sql
