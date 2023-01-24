from logging import log
from typing import Any, Dict

import pandas as pd
import prefect
from pendulum import instance
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources.sqlite import SQLite


class SQLiteInsert(Task):
    """
    Task for inserting data from a pandas DataFrame into SQLite.

    Args:
        db_path (str, optional): The path to the database to be used. Defaults to None.
        sql_path (str, optional): The path to the text file containing the query. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.

    """

    def __init__(
        self,
        df: pd.DataFrame = None,
        db_path: str = None,
        schema: str = None,
        table_name: str = None,
        if_exists: str = "fail",
        dtypes: Dict[str, Any] = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.db_path = db_path
        self.table_name = table_name
        self.df = df
        self.dtypes = dtypes
        self.schema = schema
        self.if_exists = if_exists

        super().__init__(name="sqlite_insert", timeout=timeout, *args, **kwargs)

    @defaults_from_attrs("df", "db_path", "schema", "table_name", "if_exists", "dtypes")
    def run(
        self,
        table_name: str = None,
        schema: str = None,
        dtypes: Dict[str, Any] = None,
        db_path: str = None,
        df: pd.DataFrame = None,
        if_exists: str = "skip",
    ):

        sqlite = SQLite(credentials=dict(db_name=db_path))
        sqlite.create_table(
            table=table_name, schema=schema, dtypes=dtypes, if_exists=if_exists
        )
        logger = prefect.context.get("logger")
        if isinstance(df, pd.DataFrame) == False:
            logger.warning("Object is not a pandas DataFrame")
        elif df.empty:
            logger.warning("DataFrame is empty")
        else:
            sqlite.insert_into(table=table_name, df=df)

        return True


class SQLiteSQLtoDF(Task):
    """
    Task for downloading data from the SQLite to a pandas DataFrame.

    SQLite will create a new database in the directory specified by the 'db_path' parameter.

    Args:
        db_path (str, optional): The path to the database to be used. Defaults to None.
        sql_path (str, optional): The path to the text file containing the query. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.

    """

    def __init__(
        self,
        db_path: str = None,
        sql_path: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.db_path = db_path
        self.sql_path = sql_path

        super().__init__(name="sqlite_sql_to_df", timeout=timeout, *args, **kwargs)

    def __call__(self):
        """Generate a DataFrame from a SQLite SQL query"""

    @defaults_from_attrs("db_path", "sql_path")
    def run(self, sql_path: str = None, db_path: str = None) -> pd.DataFrame:

        sqlite = SQLite(credentials=dict(db_name=db_path))

        logger = prefect.context.get("logger")
        logger.info(f"Loading a DataFrame from {sql_path}...")

        with open(sql_path, "r") as queryfile:
            query = queryfile.read()

        df = sqlite.to_df(query)
        logger.info(f"DataFrame was successfully loaded.")

        return df


class SQLiteQuery(Task):
    """
    Task for running an SQLite query.

    Args:
        query (str, optional): The query to execute on the database. Defaults to None.
        db_path (str, optional): The path to the database to be used. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.

    """

    def __init__(
        self,
        query: str = None,
        db_path: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.query = query
        self.db_path = db_path
        super().__init__(name="sqlite_query", timeout=timeout, *args, **kwargs)

    def __call__(self):
        """Run an SQL query on SQLite"""

    @defaults_from_attrs("query", "db_path")
    def run(self, query: str = None, db_path: str = None):
        sqlite = SQLite(credentials=dict(db_name=db_path))

        # run the query and fetch the results if it's a select
        result = sqlite.run(query=query)

        self.logger.info(f"Successfully ran the query.")

        return result
