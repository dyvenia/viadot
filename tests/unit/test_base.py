import pandas as pd

from viadot.sources.base import Source

from .test_credentials import get_credentials

CREDENTIALS = get_credentials("SQL_SOURCE_TEST")


class EmptySource(Source):
    def to_df(self, if_empty):
        df = pd.DataFrame()
        if df.empty:
            self._handle_if_empty(if_empty)
        return df


def test_empty_source_skip():
    empty = EmptySource()
    result = empty.to_csv(path="t.csv", if_empty="skip")
    assert result is False


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
