from typing import Any, Dict

import pandas as pd
import prefect
from prefect import Task

from ..sources.sqlite import SQLite


class SQLiteInsert(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(name="load_df", *args, **kwargs)

    def run(
        self,
        table_name: str = None,
        schema: str = None,
        dtypes: Dict[str, Any] = None,
        db_path: str = None,
        df: pd.DataFrame = None,
        if_exists: str = "fail",
    ):

        sqlite = SQLite(credentials=dict(db_name=db_path))
        sqlite.create_table(
            table=table_name, schema=schema, dtypes=dtypes, if_exists=if_exists
        )
        sqlite.insert_into(table=table_name, df=df)

        return True


class SQLiteBulkInsert(Task):
    pass


class SQLiteSQLtoDF(Task):
    def __init__(self, *args, db_path: str = None, sql_path: str = None, **kwargs):
        super().__init__(name="sqlite_sql_to_df", *args, **kwargs)
        self.sql_path = sql_path
        self.db_path = db_path

    def __call__(self):
        """Generate a DataFrame from a SQLite SQL query"""

    def run(self):
        sqlite = SQLite(credentials=dict(db_name=self.db_path))
        logger = prefect.context.get("logger")
        logger.info(f"Loading a DataFrame from {self.sql_path}")
        with open(self.sql_path, "r") as queryfile:
            query = queryfile.read()

        df = sqlite.to_df(query)
        logger.info(f"DataFrame was successfully loaded.")

        return df
