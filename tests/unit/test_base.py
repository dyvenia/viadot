import pandas as pd

from viadot.sources.base import Source


class EmptySource(Source):
    def to_df(self):
        return pd.DataFrame()


def test_empty_source_skip():
    empty = EmptySource()
    result = empty.to_csv(path="t.csv", if_empty="skip")
    assert result is False
