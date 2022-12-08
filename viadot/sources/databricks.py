import json
import os
from typing import Literal, Union

import pandas as pd
import pyspark.sql.dataframe as spark
from delta.tables import *
from pydantic import BaseModel
from viadot.exceptions import CredentialError

from ..config import get_source_credentials
from ..exceptions import TableAlreadyExists, TableDoesNotExist
from ..utils import build_merge_query, df_snakecase_column_names
from .base import Source


class DatabricksCredentials(BaseModel):
    org_id: str  # Databricks Organization ID
    host: str  # The host address of the Databricks cluster.
    cluster_id: str  # The ID of the Databricks cluster to which to connect.
    port: str = 15001  # The port on which the cluster is exposed. By default '15001'.
    token: str  # The access token which will be used to connect to the cluster.


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
        credentials: DatabricksCredentials = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            raise CredentialError("Please specify the credentials.")
        DatabricksCredentials(**credentials)  # validate the credentials schema

        if not credentials.get("port"):
            credentials["port"] = Databricks.DEFAULT_CLUSTER_PORT

        super().__init__(*args, credentials=credentials, **kwargs)

        self._session = None

    def _create_spark_session(self):
        """
        Establish a connection to the Databricks cluster.

        Returns:
            SparkSession: A configured SparkSession object.
        """

        db_connect_config = dict(
            host=self.credentials.get("host"),
            token=self.credentials.get("token"),
            cluster_id=self.credentials.get("cluster_id"),
            org_id=self.credentials.get("org_id"),
            port=self.credentials.get("port"),
        )

        with open(os.path.expanduser("~/.databricks-connect"), "w") as f:
            json.dump(db_connect_config, f)

        spark = SparkSession.builder.getOrCreate()
        return spark

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            session = self._create_spark_session()
            self._session = session
            return session
        return self._session

    def to_df(
        self,
        query: str,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """
        Execute a query and return a Pandas DataFrame.

        Args:
            query (str): The query to execute
            if_empty (str, optional): What to do if the query returns no data.
                Defaults to 'warn'.

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
        return self.session._jsparkSession.catalog().tableExists(schema, table)

    def _check_if_schema_exists(self, schema: str) -> bool:
        return self.session._jsparkSession.catalog().databaseExists(schema)

    def create_table_from_pandas(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        if_exists: Literal["replace", "skip", "fail"] = "fail",
    ) -> bool:
        """
        Create a table using a pandas `DataFrame`.

        Args:
            df (pd.DataFrame): The `DataFrame` to be written as a table.
            table (str): Name of the table to be created.
            schema (str, optional): Name of the schema.
            if_empty (str, optional): What to do if the input `DataFrame` is empty.
                Defaults to 'warn'.
            if_exists (Literal, optional): What to do if the table already exists.
                Defaults to 'fail'.

        Example:
        ```python
        from viadot.sources import Databricks

        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)

        new_table = databricks.create_table_from_pandas(
            df=df, schema="viadot_test", table="test"
        )
        ```
        """

        if df.empty:
            self._handle_if_empty(if_empty)

        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}"
        success_message = f"Table {fqn} has been created successfully."

        if self._check_if_table_exists(schema=schema, table=table):
            if if_exists == "skip":
                self.logger.warning(f"Table {fqn} already exists.")
                result = False
            elif if_exists == "fail":
                raise TableAlreadyExists(fqn)
            elif if_exists == "replace":
                result = self._full_refresh(schema=schema, table=table, df=df)
            else:
                success_message = f"Table {fqn} has been overwritten successfully."
                result = True
        else:
            df = df_snakecase_column_names(df)
            sdf = self._pandas_df_to_spark_df(df)
            sdf.createOrReplaceTempView("tmp_view")

            result = self.run(
                f"CREATE TABLE {fqn} USING DELTA AS SELECT * FROM tmp_view;"
            )

        if result:
            self.logger.info(success_message)

        return result

    def drop_table(self, table: str, schema: str = None) -> bool:
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

        databricks.drop_table(schema="viadot_test", table="test")
        ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}"

        if self._check_if_table_exists(schema=schema, table=table):
            result = self.run(f"DROP TABLE {fqn}")
            self.logger.info(f"Table {fqn} has been deleted successfully.")
        else:
            raise TableDoesNotExist(fqn=fqn)

        return result

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

        databricks.insert_into( df=df, schema="viadot_test", table="test", mode="replace")
        ```
        """
        fqn = f"{schema}.{table}"
        data = self._pandas_df_to_spark_df(df)
        data.write.format("delta").mode("overwrite").saveAsTable(fqn)

        self.logger.info(f"Table {fqn} has been refreshed successfully.")

        return True

    def _upsert(
        self,
        df: pd.DataFrame,
        table: str,
        primary_key: str,
        schema: str = None,
    ):
        spark_df = self._pandas_df_to_spark_df(df)
        merge_query = build_merge_query(
            df=spark_df,
            schema=schema,
            table=table,
            primary_key=primary_key,
            source=self,
        )
        result = self.run(merge_query)

        self.logger.info("Data has been upserted successfully.")

        return result

    def insert_into(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
        primary_key: str = None,
        mode: Literal["replace", "append", "update"] = "append",
    ) -> None:
        """
        Insert data from a pandas `DataFrame` into a Delta table.

        Args:
            df (pd.DataFrame): DataFrame with the data to be inserted into the table.
            table (str): Name of the new table to be created.
            schema (str, Optional): Name of the schema.
            primary_key (str, Optional): The primary key on which the data will be joined.
                Required only when updating existing data.
            mode (str, Optional): Which operation to run with the data. Allowed operations
                are: 'replace', 'append', and 'update'. By default, 'append'.

        Example:
        ```python
        from viadot.sources import Databricks

        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)

        databricks.insert_into(
            df=df, schema="viadot_test", table="test", primary_key="pk", mode="update"
        )
        ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = f"{schema}.{table}" if schema else table

        exists = self._check_if_table_exists(schema=schema, table=table)
        if exists:
            if mode == "replace":
                self._full_refresh(df=df, schema=schema, table=table)
            elif mode == "append":
                self._append(df=df, schema=schema, table=table)
            elif mode == "update":
                self._upsert(df=df, schema=schema, table=table, primary_key=primary_key)
            else:
                raise ValueError(
                    "`mode` must be one of: 'replace', 'append', or 'update'."
                )
        else:
            raise ValueError(f"Table {fqn} does not exist.")

    def create_schema(self, schema_name: str) -> bool:
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
        result = self.run(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self.logger.info(f"Schema {schema_name} has been created successfully.")
        return result

    def drop_schema(self, schema_name: str) -> bool:
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
        result = self.run(f"DROP SCHEMA {schema_name}")
        self.logger.info(f"Schema {schema_name} deleted.")
        return result

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

        databricks.discover_schema(schema="viadot_test", table="test")
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
            version_number (int): The table's version number.
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
        Rollback a table to a previous version.

        Args:
            schema (str): Name of the schema.
            table (str): Name of the table to rollback.
            version_number (int): Number of the table's version to rollback to.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()

        schema = "viadot_test"
        table = "table_1"

        version_number = databricks.get_table_version(schema=schema, table=table)

        # Perform changes on the table, in this example we are appending to the table
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        databricks.insert_into(df=df, schema=schema, table=table)

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
            df=old_table, schema=schema, table=table, mode="replace"
        )

        self.logger.info(
            f"Rollback for table {fqn} to version #{version_number} completed."
        )

        return result
