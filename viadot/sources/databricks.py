from typing import Any, Dict, Literal, Union

import pandas as pd
import pyspark.sql.dataframe as spark
from delta.tables import *
from pyspark import SparkConf, SparkContext

from ..config import get_source_credentials
from ..exceptions import TableDoesNotExist
from ..utils import build_merge_query
from .base import Source


class Databricks(Source):
    """
    A class for pulling and manipulating data on Databricks.

    Documentation for Databricks is located at: https://docs.microsoft.com/en-us/azure/databricks/

    Parameters
    ----------
    credentials : Dict[str, Any], optional
        Credentials containing Databricks connection configuration
        (`host`, `token`, `org_id`, and `cluster_id`).
    config_key (str, optional): The key in the viadot config holding relevant credentials.
    """

    DEFAULT_SCHEMA = "default"
    DEFAULT_CLUSTER_PORT = "15001"

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        self.credentials = credentials or get_source_credentials(config_key)
        self._session = None

    def _create_spark_session(self):
        """
        Establish a connection to the Databricks cluster.

        Returns:
            SparkSession: A configured SparkSession object.
        """
        default_spark = SparkSession.builder.getOrCreate()
        config = SparkConf()

        # Copy all the configuration values from the current Spark Context.
        default_config_vals = default_spark.sparkContext.getConf().getAll()

        for (key, value) in default_config_vals:
            config.set(key, value)

        config.set(
            "spark.databricks.service.clusterId", self.credentials.get("cluster_id")
        )
        config.set(
            "spark.databricks.service.port",
            self.credentials.get("port", Databricks.DEFAULT_CLUSTER_PORT),
        )
        # stop the spark session context in order to create a new one with the required cluster_id, else we
        # will still use the current cluster_id for execution

        default_spark.stop()
        context = SparkContext(conf=config)
        session = SparkSession(context)
        new_spark = session.builder.config(conf=config).getOrCreate()
        return new_spark

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            session = self._create_spark_session()
            self._session = session
            return session
        return self._session

    def to_df(self, query: str, if_empty: str = "fail") -> pd.DataFrame:
        """
        Execute a query and return a Pandas DataFrame.

        Args:
            query (str): The query to execute
            if_empty (str, optional): What to do if the query returns no data. Defaults to "fail".

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        table_data = databricks.to_df("SELECT * FROM schema.table_1")
        ```
        Returns:
            pd.DataFrame: A Pandas DataFrame containing the requested table's data.
        """
        if query.upper().startswith("SELECT"):
            df = self.run(query, fetch_type="pandas")
            if df.empty:
                self._handle_if_empty(if_empty=if_empty)
        else:
            df = pd.DataFrame()
        return df

    def _pandas_df_to_spark_df(self, df: pd.DataFrame) -> spark.DataFrame:
        """
        Convert a Pandas DataFrame to a Spark DataFrame.

        Args:
            df (pd.DataFrame): The Pandas DataFrame to be converted to a Spark DataFrame.

        Example:
        ```python
        from viadot.sources import Databricks
        import pandas as pd
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        spark_df = databricks._pandas_df_to_spark_df(df)
        ```
        Returns:
            spark.DataFrame: The resulting Spark DataFrame.
        """
        spark_df = self.session.createDataFrame(df)
        return spark_df

    def _spark_df_to_pandas_df(self, spark_df: spark.DataFrame) -> pd.DataFrame:
        """
        Convert a Spark DataFrame to a Pandas DataFrame.

        Args:
            df (spark.DataFrame): The Spark DataFrame to be converted to a Pandas DataFrame.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        spark_df = databricks._pandas_df_to_spark_df(df)
        pandas_df = databricks._spark_df_to_pandas_df(spark_df)
        ```
        Returns:
            pd.DataFrame: The resulting Pandas DataFrame.
        """
        return spark_df.toPandas()

    def run(
        self, query: str, fetch_type: Literal["spark", "pandas"] = "spark"
    ) -> Union[spark.DataFrame, pd.DataFrame, bool]:
        """
        Execute an SQL query.

        Args:
            query (str): The query to execute.
            fetch_type (Literal, optional): How to return the data: either
            in the default Spark DataFrame format or as a Pandas DataFrame. Defaults to "spark".

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        query_result = databricks.run("SELECT * FROM schema.table_1")
        ```
        Returns:
            Union[spark.DataFrame, pd.DataFrame, bool]: Either the result set of a query or,
            in case of DDL/DML queries, a boolean describing whether
            the query was excuted successfuly.
        """
        if fetch_type not in ["spark", "pandas"]:
            raise ValueError(
                "Only the values 'spark', 'pandas' are allowed for 'fetch_type'"
            )

        query_clean = query.upper().strip()
        query_keywords = ["SELECT", "SHOW", "PRAGMA", "DESCRIBE"]

        query_result = self.session.sql(query)

        if any(query_clean.startswith(word) for word in query_keywords):
            if fetch_type == "spark":
                result = query_result
            else:
                result = self._spark_df_to_pandas_df(query_result)
        else:
            result = True

        return result

    def _check_if_table_exists(self, table: str, schema: str = None) -> bool:
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        does_exist = self.session._jsparkSession.catalog().tableExists(schema, table)
        return does_exist

    def create_table_from_pandas(
        self, table: str, df: pd.DataFrame, schema: str = None, if_empty="warn"
    ) -> pd.DataFrame:
        """
        Write a new table using a given Pandas DataFrame.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the new table to be created.
            df (pd.DataFrame): DataFrame to be written as a table.
            if_empty (str, optional): What to do if the query returns no data. Defaults to "warn".

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        new_table = databricks.create_table_from_pandas(schema="schema", table="table_1", df=df)
        ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}"
        if not self._check_if_table_exists(schema=schema, table=table):
            data = self._pandas_df_to_spark_df(df)
            data.createOrReplaceTempView("new_table")

            self.run(f"CREATE TABLE {fqn} USING DELTA AS SELECT * FROM new_table;")

            self.logger.info(f"Table {fqn} created.")

            result = self.to_df(f"SELECT * FROM {fqn}")
            if result.empty:
                self._handle_if_empty(if_empty)

        else:
            self.logger.info(f"Table {fqn} already exists.")

    def drop_table(self, table: str, schema: str = None) -> None:
        """
        Delete an existing table.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the new table to be created.

        Raises:
            TableDoesNotExist: If the table does not exist.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.drop_table(schema="schema", table="table_1")
        ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}"
        if self._check_if_table_exists(schema, table):
            self.run(f"DROP TABLE {fqn}")
            self.logger.info(f"Table {fqn} has been deleted successfully.")
        else:
            raise TableDoesNotExist(fqn=fqn)

    def _append(self, schema: str, table: str, df: pd.DataFrame):
        fqn = f"{schema}.{table}"
        spark_df = self._pandas_df_to_spark_df(df)
        spark_df.write.format("delta").mode("append").saveAsTable(fqn)
        self.logger.info(f"Table {fqn} has been appended successfully.")

    def _full_refresh(self, schema: str, table: str, df: pd.DataFrame):
        """
        Overwrite an existing table with data from a Pandas DataFrame.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the new table to be created.
            df (pd.DataFrame): DataFrame to be used to overwrite the table.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        databricks.insert_into(schema="raw", table="c4c_test4", df=df, if_exists="replace")
        ```
        """
        fqn = f"{schema}.{table}"
        data = self._pandas_df_to_spark_df(df)
        data.write.format("delta").mode("overwrite").saveAsTable(fqn)
        self.logger.info(f"Table {fqn} has been full-refreshed successfully")

    def _upsert(
        self,
        schema: str,
        table: str,
        primary_key: str,
        df: pd.DataFrame,
    ):
        spark_df = self._pandas_df_to_spark_df(df)
        merge_query = build_merge_query(
            stg_schema="",
            stg_table="",
            schema=schema,
            table=table,
            primary_key=primary_key,
            source=self,
            df=spark_df,
        )
        result = self.run(merge_query)

        self.logger.info("Data upserted successfully.")

        return result

    def insert_into(
        self,
        table: str,
        df: pd.DataFrame,
        schema: str = None,
        primary_key: str = "",
        if_exists: Literal["replace", "append", "update", "fail"] = "fail",
    ):
        """
        Insert data into a table.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the new table to be created.
            df (pd.DataFrame): DataFrame with the data to be inserted into the table.
            primary_key (str, Optional): Needed only when updating the data, the primary key on which the data will be joined.
            if_exists (str, Optional): Which operation to run with the data. Allowed operations are: replace, append, update and fail.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        databricks.insert_into(schema="raw", table="c4c_test4", df=df, primary_key="pk", if_exists="update")
        ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        exists = self._check_if_table_exists(schema=schema, table=table)
        if exists:
            if if_exists == "replace":
                self._full_refresh(schema, table, df)
                result = True
            elif if_exists == "append":
                self._append(schema, table, df)
                result = True
            elif if_exists == "update":
                self._upsert(schema, table, primary_key, df)
                result = True
            elif if_exists == "fail":
                raise ValueError(
                    "The table already exists and 'if_exists' is set to 'fail'."
                )
            else:
                raise ValueError(
                    "Only the values 'replace', 'append', 'update', 'fail' are allowed for 'if_exists'"
                )
        else:
            raise ValueError("Table does not exist")
        return result

    def create_schema(self, schema_name: str):
        """
        Create a schema for storing tables.

        Args:
            schema_name (str): Name of the new schema to be created.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.create_schema("schema_1")
        ```
        """
        self.run(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self.logger.info(f"Schema {schema_name} created.")

    def drop_schema(self, schema_name: str):
        """
        Delete a schema.

        Args:
            schema_name (str): Name of the schema to be deleted.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.drop_schema("schema_1")
        ```
        """
        self.run(f"DROP SCHEMA {schema_name}")
        self.logger.info(f"Schema {schema_name} deleted.")

    def discover_schema(self, table: str, schema: str = None) -> dict:
        """
        Return a table's schema.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the new table to be created.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.discover_schema(schema="schema", table="table")
        ```
        Returns:
            schema (dict): A dictionary containing the schema details of the table.
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}"
        result = self.run(f"DESCRIBE {fqn}", fetch_type="pandas")

        # Delete the last 3 records, which are boilerplate for Delta Tables
        col_names = result["col_name"].values.tolist()
        col_names = col_names[:-3]

        data_types = result["data_type"].values.tolist()
        data_types = data_types[:-3]

        schema = dict(zip(col_names, data_types))

        return schema

    def get_table_version(self, table: str, schema: str = None) -> int:
        """
        Get the provided table's version number.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the table to rollback.

        Returns:
            version_number (int): The table's version number
        ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}"

        # Get the info regarding the current version of the delta table
        history = self.run(f"DESCRIBE HISTORY {fqn}", "pandas")

        # Extract the current version number
        version_number = history["version"].iat[0]
        return int(version_number)

    def rollback(self, table: str, version_number: int, schema: str = None) -> bool:
        """
        Rollback the table to a previous version.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the table to rollback.
            version_number (int): Number of the table's version to rollback to.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()

        schema = "test"
        table = "table_1"

        version_number = databricks.get_table_version(schema=schema, table=table)

        # Perform changes on the table, in this example we are appending to the table
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        databricks.insert_into(schema=schema, table=table, df=df, if_exists="append")

        databricks.rollback(schema=schema, table=table, version_number=version_number)
        ```
        Returns:
            result (bool): A boolean indicating the success of the rollback.
        """

        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}"

        # Retrieve the data from the previous table
        old_table = self.to_df(f"SELECT * FROM {fqn}@v{version_number}")

        # Perform full-refresh and overwrite the table with the new data
        result = self.insert_into(
            schema=schema, table=table, df=old_table, if_exists="replace"
        )

        self.logger.info(
            f"Rollback for table {fqn} to version #{version_number} completed."
        )

        return result
