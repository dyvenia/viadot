import os

import pandas as pd
import pytest

from viadot.tasks import SQLiteInsert, SQLiteSQLtoDF
from viadot.tasks.sqlite import SQLiteInsert, SQLiteQuery, SQLiteSQLtoDF

TABLE = "test_sqlite_task"
DB_PATH = "testfile.sqlite"
SQL_PATH_SELECT = "testfile_select.sql"
SQL_PATH_NOTSELECT = "testfile_notselect.sql"

sqlite_insert_task = SQLiteInsert(db_path=DB_PATH)
sql_to_df_task = SQLiteSQLtoDF(db_path=DB_PATH)
sqlite_query_task = SQLiteQuery(db_path=DB_PATH)


@pytest.fixture(scope="session")
def create_test_sql_file_select():
    with open(SQL_PATH_SELECT, "w") as sql_file:
        sql_file.write(f"SELECT * FROM {TABLE};")
    yield
    os.remove(SQL_PATH_SELECT)


@pytest.fixture(scope="session")
def create_test_sql_file_notselect():
    with open(SQL_PATH_NOTSELECT, "w") as sql_file:
        sql_file.write("INSERT ...")
    yield
    os.remove(SQL_PATH_NOTSELECT)


def test_query_select(create_test_sql_file_select):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    df_data = pd.DataFrame({"country": ["italy"], "sales": [100.0]})
    result = sqlite_insert_task.run(
        table_name=TABLE,
        dtypes=dtypes,
        df=df_data,
        if_exists="replace",
    )
    table_from_query = sql_to_df_task.run(sql_path=SQL_PATH_SELECT)
    assert result is True
    assert not table_from_query.empty


def test_query_not_select(create_test_sql_file_notselect):
    table_from_query = sql_to_df_task.run(sql_path=SQL_PATH_NOTSELECT)
    assert table_from_query.empty
