import json
from viadot.config import local_config
from viadot.sources.base import Source
from delta.tables import *
import pandas as pd
import pyspark
from pyspark import SparkConf, SparkContext
import pyspark.sql.dataframe as spark
from pyspark.sql.dataframe import DataFrame


class Databricks(Source):

    defaults_key = "DEFAULTS"
    env = ""
    sc = None

    def __init__(self, env: str = "QA", *args, **kwargs):
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
        print(org_id)
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
            self.sc = SparkSession.builder.getOrCreate()
        else:
            self.sc = self._get_spark_session(self.env)

    def to_df(self, table_name: str, if_empty="fail"):
        """
        Read the contents of a given table and return it as a Pandas DataFrame

        Args:
            table_name (str): Name of an existing table.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        table_data = databricks.to_df("schema.table_1")
        ```
        Returns:
            pd.DataFrame: A Pandas DataFrame containing the requested table's data.
        """
        result = self.sc.sql("select * from " + table_name)
        df = self._spark_df_to_pandas_df(result)
        if df.empty:
            self._handle_if_empty(if_empty)
        return df

    def to_json(self, table_name: str, if_empty="fail"):
        """
        Read the contents of a given table and return it as a JSON string

        Args:
            table_name (str): Name of an existing table.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        table_data = databricks.to_json("schema.table_1")
        ```
        Returns:
            str: A JSON string containing the requested table's data.
        """
        result = self.sc.sql("select * from " + table_name)
        df = self._spark_df_to_pandas_df(result)
        if df.empty:
            self._handle_if_empty(if_empty)
        return df.to_json()

    def _pandas_df_to_spark_df(self, df: pd.DataFrame):
        """
        Convert Pandas DataFrame to Spark DataFrame.

        Args:
            df (pd.DataFrame): Pandas DataFrame to be converted to Spark DataFrame.

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
            spark.DataFrame: The converted Spark DataFrame.
        """
        spark_df = self.sc.createDataFrame(df)
        return spark_df

    def _spark_df_to_pandas_df(self, spark_df: spark.DataFrame):
        """
        Convert Spark DataFrame to Pandas DataFrame.

        Args:
            df (spark.DataFrame): Spark DataFrame to be converted to Pandas DataFrame.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        spark_df = databricks._pandas_df_to_spark_df(df)
        pandas_df = _spark_df_to_pandas_df(spark_df)
        ```
        Returns:
            pd.DataFrame: A Pandas DataFrame.
        """
        return spark_df.toPandas()

    def query(self, query: str, if_empty="warn"):
        """
        Execute an SQL query.

        Args:
            query (str): An SQL query.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        query_result = databricks.query("SELECT * FROM schema.table_1")
        ```
        Returns:
            pd.DataFrame: A Pandas DataFrame containing the query's results (if available).
        """
        result = self.sc.sql(query)
        df = self._spark_df_to_pandas_df(result)
        if df.empty:
            self._handle_if_empty(if_empty)
        return df

    def create_table(self, table_name: str, df: pd.DataFrame, if_empty="warn"):
        """
        Write a new table using a given Pandas DataFrame.

        Args:
            table_name (str): Name of the new table to be created.
            df (pd.DataFrame): DataFrame to be written as a table.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"id":"1", "name":"Joe"}]
        df = pd.DataFrame(list)
        new_table = databricks.create_table("schema.table_1", df)
        ```
        Returns:
            pd.DataFrame: A Pandas DataFrame containing the newly created table's data.
        """
        data = self._pandas_df_to_spark_df(df)
        data.createOrReplaceTempView("new_table")

        self.query(
            "CREATE TABLE " + table_name + " USING DELTA AS SELECT * FROM new_table;"
        )

        print("Table " + table_name + " created.")

        result = self.to_df(table_name)
        if result.empty:
            self._handle_if_empty(if_empty)
        return result

    def delete_table(self, table_name: str):
        """
        Delete an existing table.

        Args:
            table_name (str): Name of the table to be overwritten.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()]
        databricks.delete_table("schema.table_1")
        ```
        """
        self.sc.sql("DROP TABLE " + table_name)
        print("Table deleted.")

    def full_refresh(self, table_name: str, df: pd.DataFrame):
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
        new_table = databricks.full_refresh("schema.table_1", df)
        ```
        Returns:
            pd.DataFrame: A Pandas DataFrame containing the new data in the overwitten table.
        """
        data = self._pandas_df_to_spark_df(df)
        data.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return self.to_df(table_name)

    # TODO: add a method for extracting a usable JSON schema for the sets
    # TODO: Parse the matched_set as a not_matched_set
    def upsert(self, table_name: str, df: pd.DataFrame, matched_set: dict, pk: str):
        """
        Upsert the data to an existing table. If the record exists, the function updates the record in the table.
        If it doesn't exist, the function creates a new record in the table.
        Assume that the source table is called "main", and the data to be upserted will use a view called "updates".

        Args:
            table_name (str): Name of the table to perform the upsert operation on.
            df (pd.DataFrame): DataFrame to be used to upsert the table.
            matched_set (dict): A dictionary defining the changes that should take place in case a matching record is found.
            pk (str): The primary key that should be joined on.
        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        data = [{"Id": "KVSzUaILfQZXDb", "wrong": "Updated3!EHNYKjSZsiy", "Name": "Upsert-Black", "FirstName": "Updated", "LastName": "Carter2", "ContactEMail": "Adam.Carter@TurnerBlack.com", "MailingCity": "Updated!Jamesport"}]
        match = {"Id": "main.Id",
                "AccountId": "updates.AccountId",
                "Name": "updates.Name",
                "FirstName": "updates.FirstName",
                "LastName": "updates.LastName",
                "ContactEMail": "updates.ContactEMail",
                "MailingCity": "main.MailingCity"}

        upsert("raw.c4c_test4", data, match, "Id")
        ```
        """

        # Retrieve columns names and matched set conditions from matched_set
        match_conditions = ""
        columns = "("
        keys_list = list(matched_set.keys())

        for k, v in matched_set.items():
            if k == keys_list[-1]:
                match_conditions = match_conditions + str(k) + "=" + str(v)
                columns = columns + str(k) + ")"
            else:
                match_conditions = match_conditions + str(k) + "=" + str(v) + ", "
                columns = columns + str(k) + ","

        # Check the column names and types match
        ## Retrieve the table as a sc DataFrame
        delta_df = self.to_df(table_name)
        upsertData = self._pandas_df_to_spark_df(df)
        upsert_df = self._spark_df_to_pandas_df(upsertData)
        ## Sort columns alphabetically
        delta_df = delta_df.sort_index(axis=1)
        upsert_df = upsert_df.sort_index(axis=1)

        if (delta_df.columns == upsert_df.columns).all():
            # Upserts the values of a given dataframe/Delta table into an existing Delta table
            upsertData.createOrReplaceTempView("updates")

            self.query(
                "MERGE INTO "
                + table_name
                + " AS main USING updates ON main."
                + pk
                + " = updates."
                + pk
                + " WHEN MATCHED THEN UPDATE SET "
                + match_conditions
                + " WHEN NOT MATCHED THEN INSERT "
                + columns
                + " VALUES "
                + columns
            )

            print("Data upserted successfully.")
        else:
            print(
                "Columns mismatch. Please adhere to the column names and data types used in the source table."
            )

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
        self.sc.sql("CREATE SCHEMA IF NOT EXISTS " + schema_name)
        print("Schema " + schema_name + " created.")

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
        self.sc.sql("DROP SCHEMA " + schema_name)
        print("Schema " + schema_name + " deleted.")

    def print_schema(self, table_name: str):
        """
        Print the details of a table's data schema.

        Args:
            table_name (str): Name of the table whose schema will be printed.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.print_schema("table_1")
        ```
        """
        table = self.to_df(table_name)
        table = self._pandas_df_to_spark_df(table)
        print(table.print_schema())
