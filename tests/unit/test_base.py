import os
import pytest
import pandas as pd
import pyarrow as pa
from viadot.sources.base import SQL, Source
from viadot.signals import SKIP

from .test_credentials import get_credentials

CREDENTIALS = get_credentials("SQL_SOURCE_TEST")
TABLE = "test"
PATH = "t.csv"


class NotEmptySource(Source):
    def to_df(self, if_empty):
        df = pd.DataFrame.from_dict(
            data={"country": ["italy", "germany", "spain"], "sales": [100, 50, 80]}
        )
        return df


class EmptySource(Source):
    def to_df(self, if_empty):
        df = pd.DataFrame()
        if df.empty:
            self._handle_if_empty(if_empty)
        return df


def test_empty_source_skip():
    empty = EmptySource()
    result = empty.to_csv(path=PATH, if_empty="skip")
    assert result is False


def test_to_csv():
    src = NotEmptySource()
    res = src.to_csv(path="testbase.csv")
    assert res == True
    assert os.path.isfile("testbase.csv") == True
    os.remove("testbase.csv")


def test_to_arrow():
    src = NotEmptySource()
    res = src.to_arrow("testbase.arrow")
    assert isinstance(res, pa.Table) == True


def test_to_excel():
    src = NotEmptySource()
    res = src.to_excel(path="testbase.xlsx")
    assert res == True
    assert os.path.isfile("testbase.xlsx") == True
    os.remove("testbase.xlsx")


def test_handle_if_empty(caplog):
    src = EmptySource()
    src._handle_if_empty(if_empty="warn")
    assert "WARNING The query produced no data." in caplog.text
    with pytest.raises(ValueError):
        src._handle_if_empty(if_empty="fail")
    with pytest.raises(SKIP):
        src._handle_if_empty(if_empty="skip")


# def test_to_csv_append():
#     """Test whether `to_csv()` with the append option writes data of correct shape"""
#     driver = "/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so"
#     db_name = "testfile.sqlite"
#     server = "localhost"
#     source = SQL(
#         credentials=dict(driver=driver, db_name=db_name, server=server, user=None)
#     )

#     # Generate test table.
#     df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
#     source.create_table("test", dtypes={"a": "INT", "b": "INT"}, if_exists="replace")
#     source.insert_into(TABLE, df)

#     # Write the table to a CSV three times in `append` mode.
#     for i in range(3):
#         source.to_csv(path=PATH, query="SELECT * FROM test", if_exists="append")

#     # Read the CSV and validate no. of rows and columns.
#     out_df = pd.read_csv(PATH, sep="\t")

#     target_length = 3 * df.shape[0]
#     target_width = df.shape[0]

#     actual_length = out_df.shape[0]
#     actual_width = out_df.shape[1]

#     assert actual_length == target_length and actual_width == target_width

#     # Clean up.
#     os.remove(PATH)


# GitHub changes the string and makes the test fail
# def test_conn_str():
#     s = SQL(
#         driver=CREDENTIALS["driver"],
#         server=CREDENTIALS["server"],
#         db=CREDENTIALS["db_name"],
#         user=CREDENTIALS["user"],
#         pw=CREDENTIALS["password"],
#     )
#     assert (
#         s.conn_str
#         == "DRIVER=ODBC Driver 17 for SQL Server;SERVER=s123.database.windows.net;DATABASE=a-b-c;UID={my_user@example.com};PWD={a123;@4}"
#     )
