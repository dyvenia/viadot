from viadot.config import local_config
from viadot.sources.base import Source
from delta.tables import *
import pandas as pd
from pyspark import SparkConf, SparkContext
import pyspark.sql.dataframe as spark
from pyspark.sql.dataframe import DataFrame
from prefect.utilities import logging
from typing import Literal, Union

import viadot.utils


logger = logging.get_logger(__name__)


class Databricks(Source):

    defaults_key = "DEV"
    env = ""
    session = None

    def __init__(self, env: str = "DEV", *args, **kwargs):
        self.env = env
        org_id = (
            local_config.get("DATABRICKS", {})
            .get(Databricks.defaults_key, {})
            .get("org_id")
        )
        port = (
            local_config.get("DATABRICKS", {})
            .get(Databricks.defaults_key, {})
            .get("port")
        )
        default_credentials = {
            "spark.databricks.service.org_id": org_id,
            "spark.databricks.service.port": port,
        }
        self.credentials = default_credentials or local_config.get(
            "DATABRICKS", {}
        ).get(env)
        self.connect()

    def _get_spark_session(self, env: str = None):
        env = env or self.env
        default_spark = SparkSession.builder.getOrCreate()
        new_config = SparkConf()
        # copy all the configuration values from the current Spark Context
        default_config_vals = default_spark.sparkContext.getConf().getAll()
        for (k, v) in default_config_vals:
            new_config.set(k, v)
        new_config.set(
            "spark.databricks.service.clusterId", self.credentials.get("cluster_id")
        )
        new_config.set("spark.databricks.service.port", self.credentials.get("port"))
        # stop the spark session context in order to create a new one with the required cluster_id, else we
        # will still use the current cluster_id for execution
        default_spark.stop()
        context = SparkContext(conf=new_config)
        session = SparkSession(context)
        new_spark = session.builder.config(conf=new_config).getOrCreate()
        return new_spark

    def connect(self):
        if self.env == "QA":
            self.session = SparkSession.builder.getOrCreate()
        else:
            self.session = self._get_spark_session(self.env)

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
            df = self.run(query, fetch_type="dataframe")
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
        self, query: str, fetch_type: Literal["record", "dataframe"] = "record"
    ) -> Union[spark.DataFrame, pd.DataFrame, bool]:
        """
        Execute an SQL query.

        Args:
            query (str): The query to execute.
            fetch_type (Literal[, optional): How to return the data: either
            in the default record format or as a Pandas DataFrame. Defaults to "record".

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
        allowed_fetch_type_values = ["record", "dataframe"]
        if fetch_type not in allowed_fetch_type_values:
            raise ValueError(
                f"Only the values {allowed_fetch_type_values} are allowed for 'fetch_type'"
            )

        query_clean = query.upper().strip()
        query_keywords = ["SELECT", "SHOW", "PRAGMA", "DESCRIBE"]

        query_result = self.session.sql(query)

        if any(query_clean.startswith(word) for word in query_keywords):
            if fetch_type == "record":
                result = query_result
            else:
                result = self._spark_df_to_pandas_df(query_result)
        else:
            result = True

        return result

    def _check_if_table_exists(self, table_name: str) -> bool:
        split_name = table_name.split(".")
        does_exist = self.session._jsparkSession.catalog().tableExists(
            split_name[0], split_name[1]
        )
        return does_exist

    def create_table_from_pandas(
        self, table_name: str, df: pd.DataFrame, if_empty="warn"
    ) -> pd.DataFrame:
        """
        Write a new table using a given Pandas DataFrame.

        Args:
            table_name (str): Name of the new table to be created.
            df (pd.DataFrame): DataFrame to be written as a table.
            if_empty (str, optional): What to do if the query returns no data. Defaults to "warn".

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        new_table = databricks.create_table_from_pandas("schema.table_1", df)
        ```
        """
        if not self._check_if_table_exists(table_name):
            data = self._pandas_df_to_spark_df(df)
            data.createOrReplaceTempView("new_table")

            self.run(
                f"CREATE TABLE {table_name} USING DELTA AS SELECT * FROM new_table;"
            )

            logger.info(f"Table {table_name} created.")

            result = self.to_df(f"SELECT * FROM {table_name}")
            if result.empty:
                self._handle_if_empty(if_empty)

        else:
            logger.info(f"Table {table_name} already exists.")

    def drop_table(self, table_name: str):
        """
        Delete an existing table.

        Args:
            table_name (str): Name of the table to be overwritten.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()]
        databricks.drop_table("schema.table_1")
        ```
        """
        if self._check_if_table_exists(table_name):
            self.run(f"DROP TABLE {table_name}")
            logger.info(f"Table {table_name} deleted.")
        else:
            logger.info(f"Table {table_name} does not exist.")

    def _append(self, table_name: str, df: pd.DataFrame):
        if self._check_if_table_exists(table_name):
            spark_df = self._pandas_df_to_spark_df(df)
            spark_df.write.format("delta").mode("append").saveAsTable(table_name)
            logger.info(f"Table {table_name} appended successfully.")
        else:
            logger.info(f"Table {table_name} does not exist.")

    def _full_refresh(self, table_name: str, df: pd.DataFrame):
        """
        Overwrite an existing table with data from a Pandas DataFrame.

        Args:
            table_name (str): Name of the table to be overwritten.
            df (pd.DataFrame): DataFrame to be used to overwrite the table.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        databricks.insert_into(table_name = "raw.c4c_test4", df = df, if_exists = "replace")
        ```
        """
        if self._check_if_table_exists(table_name):
            data = self._pandas_df_to_spark_df(df)
            data.write.format("delta").mode("overwrite").saveAsTable(table_name)
            logger.info(f"Table {table_name} has been full-refreshed successfully")
        else:
            logger.info(f"Table {table_name} does not exist")

    def _upsert(
        self,
        table_name: str,
        primary_key: str,
        df: pd.DataFrame,
    ):
        if self._check_if_table_exists(table_name):
            split_name = table_name.split(".")
            spark_df = self._pandas_df_to_spark_df(df)
            merge_query = viadot.utils.build_merge_query(
                "", "", split_name[0], split_name[1], primary_key, self, spark_df
            )
            self.run(merge_query)
            result = True
            logger.info("Data upserted successfully.")
        else:
            logger.info(f"Table {table_name} does not exist")
            result = False
        return result

    def insert_into(
        self,
        table_name: str,
        df: pd.DataFrame,
        primary_key: str = "",
        if_exists: Literal["replace", "append", "update", "fail"] = "fail",
    ):
        """
        Insert data into a table.

        Args:
            table_name (str): Name of the table to which data will be inserted.
            df (pd.DataFrame): DataFrame with the data to be inserted into the table.
            primary_key (str, Optional): Needed only when updating the data, the primary key on which the data will be joined.
            if_exists (str, Optional): Which operation to run with the data. Allowed operations are: replace, append and update.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        databricks.insert_into(table_name="raw.c4c_test4", df=df, primary_key="pk", if_exists="update")
        ```
        """
        IF_EXISTS_ACCEPTED_VALUES = ["replace", "append", "update", "fail"]
        if if_exists not in IF_EXISTS_ACCEPTED_VALUES:
            raise ValueError()
        if self._check_if_table_exists(table_name) and if_exists == "fail":
            raise ValueError()
        if if_exists == "replace":
            self._full_refresh(table_name, df)
        elif if_exists == "append":
            self._append(table_name, df)
        else:
            self._upsert(table_name, primary_key, df)

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
        self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logger.info(f"Schema {schema_name} created.")

    def delete_schema(self, schema_name: str):
        """
        Delete a schema.

        Args:
            schema_name (str): Name of the schema to be deleted.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.delete_schema("schema_1")
        ```
        """
        self.session.sql(f"DROP SCHEMA {schema_name}")
        logger.info(f"Schema {schema_name} deleted.")

    # TODO: Change to return dict of col_name:dtype
    def discover_schema(self, table_name: str) -> dict:
        """
        Return a table's schema.

        Args:
            table_name (str): Name of the table whose schema will be printed.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.discover_schema("table_1")
        ```
        Returns:
            schema: A dictionary containing the schema details of the table.
        """
        result = self.run(f"DESCRIBE {table_name}", "dataframe")

        # Delete the last 3 records, which are boilerplate for Delta Tables
        names = result["col_name"].values.tolist()
        del names[-3:]

        data_types = result["data_type"].values.tolist()
        del data_types[-3:]

        schema = {names[i]: data_types[i] for i in range(len(names))}

        return schema
