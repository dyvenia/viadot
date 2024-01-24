import logging
import os

import pandas as pd
import pyarrow as pa
import pytest

from viadot.signals import SKIP
from viadot.sources.base import SQL, Source

from .test_credentials import get_credentials

logger = logging.getLogger(__name__)


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


def test_to_parquet(caplog):
    src = NotEmptySource()
    if os.path.isfile("testbase.parquet") == True:
        os.remove("testbase.parquet")
    with caplog.at_level(logging.INFO):
        src.to_parquet(path="testbase.parquet")
        assert "File not found." in caplog.text
    assert os.path.isfile("testbase.parquet") == True
    os.remove("testbase.parquet")


def test_handle_if_empty(caplog):
    src = EmptySource()
    src._handle_if_empty(if_empty="warn")
    assert "WARNING The query produced no data." in caplog.text
    with pytest.raises(ValueError):
        src._handle_if_empty(if_empty="fail")
    with pytest.raises(SKIP):
        src._handle_if_empty(if_empty="skip")
