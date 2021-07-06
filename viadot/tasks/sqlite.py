import os
import sqlite3
from typing import Any, Dict

import pandas as pd
import prefect
from prefect import Task
from sqlalchemy import create_engine

from ..sources.sqlite import SQLite


class Insert(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(name="load_DF", *args, **kwargs)

    def run(
        self,
        table_name: str = None,
        schema: str = None,
        dtypes: Dict[str, Any] = None,
        db_path: str = None,
        df: pd.DataFrame = None,
        if_exists: str = "fail",
    ):

        sqlite = SQLite(driver="{SQLite}", server="localhost", db=db_path)
        sqlite.create_table(
            table=table_name, schema=schema, dtypes=dtypes, if_exists=if_exists
        )
        sqlite.insert_into(table=table_name, df=df)

        return True


class BulkInsert(Task):
    pass


class SQLtoDF(Task):
    def __init__(self, *args, db_path: str = None, sql_path: str = None, **kwargs):
        super().__init__(name="SQLtoDF", *args, **kwargs)
        self.sql_path = sql_path
        self.db_path = db_path

    def __call__(self):
        """Generate a DataFrame from SQL query"""

    def run(self):
        sqlite = SQLite(driver="{SQLite}", server="localhost", db=self.db_path)
        logger = prefect.context.get("logger")
        logger.info(f"Creating SQL query from: {self.sql_path}")
        with open(self.sql_path, "r") as queryfile:
            query = queryfile.read()

        df = sqlite.to_df(query)
        logger.info(f"Successfully creating SQL query from file.")

        return df
