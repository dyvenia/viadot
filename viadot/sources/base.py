import logging
import os
import pandas
from abc import abstractmethod
from typing import Any, Dict

import pyarrow as pa


class Source:
    def __init__(self, *args, credentials: Dict[str, Any] = None, **kwargs):
        self.credentials = credentials
        self.data: pa.Table = None
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def to_json(self):
        pass

    @abstractmethod
    def to_df():
        pass

    @abstractmethod
    def query():
        pass

    def to_arrow(self):
        df = self.to_df()
        table = pa.Table.from_pandas(df)
        return table

    def to_csv(self, path: str, if_exists="replace"):
        df = self.to_df()
        if if_exists == "append":
            if os.path.isfile(path):
                csv_df = pandas.read_csv(path)
                out_df = pandas.concat([csv_df, df])
            else:
                out_df = df
        elif if_exists == "replace":
            out_df = df
        out_df.to_csv(path, index=False)
        return True
