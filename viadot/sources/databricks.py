import json
from pandas import DataFrame
from viadot.config import local_config
from viadot.sources.base import Source
from delta.tables import *
import pandas as pd
import pyspark
from pyspark import SparkConf, SparkContext
import time
# from viadot.sources import Base
class Databricks(Source):

    defaults_key = "DEFAULTS"
    env = ""
    spark = None

    def __init__(self,  env: str = "QA", *args, **kwargs):
        self.env = env
        org_id = local_config.get("DATABRICKS", {}).get(Databricks.defaults_key, {}).get("org_id")
        port = local_config.get("DATABRICKS", {}).get(Databricks.defaults_key, {}).get("port")
        print(org_id)
        default_credentials = {
            "spark.databricks.service.org_id": org_id,
            "spark.databricks.service.port": port
        }
        self.credentials = default_credentials or local_config.get("DATABRICKS", {}).get(env)
        self.connect()
        
    def _get_spark_session(self, env: str = None):
        env = env or self.env
        default_spark = SparkSession.builder.getOrCreate()
        new_config = SparkConf()
        # copy all the configuration values from the current Spark Context
        default_config_vals = default_spark.sparkContext.getConf().getAll()
        for (k, v) in default_config_vals:
            new_config.set(k, v)
        new_config.set("spark.databricks.service.clusterId", self.credentials.get("cluster_id"))
        new_config.set("spark.databricks.service.port", self.credentials.get("port"))
        # stop the spark session context in order to create a new one with the required cluster_id, else we
        # will still use the current cluster_id for execution
        default_spark.stop()
        context = SparkContext(conf=new_config)
        session = SparkSession(context)
        new_spark = session.builder.config(conf=new_config).getOrCreate()
        return new_spark

    def set_custom_config(self, cluster_id:str, port: str = "15001"):
        default_spark = SparkSession.builder.getOrCreate()
        new_config = SparkConf()
        # copy all the configuration values from the current Spark Context
        default_config_vals = default_spark.sparkContext.getConf().getAll()
        for (k, v) in default_config_vals:
            new_config.set(k, v)
        new_config.set("spark.databricks.service.clusterId", cluster_id)
        new_config.set("spark.databricks.service.port", port)
        # stop the spark session context in order to create a new one with the required cluster_id, else we
        # will still use the current cluster_id for execution
        default_spark.stop()
        context = SparkContext(conf=new_config)
        session = SparkSession(context)
        new_spark = session.builder.config(conf=new_config).getOrCreate()
        return new_spark

    def connect(self):
        if self.env == "QA":
            self.spark = SparkSession.builder.getOrCreate()
        else:
            self.spark = self._get_spark_session(self.env)
    
    def to_df(self, list):
        return pd.DataFrame(list)
    
    def to_json(sparkDf: pyspark.sql.dataframe.DataFrame):
        return sparkDf.toJSON()
    
    def toSparkDF(self, df: DataFrame):
        """
        Convert Pandas DataFrame to Spark DataFrame

        Args:
            df (DataFrame): Pandas DataFrame to be converted to Spark DataFrame.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"some":"data"}]
        sparkDf = databricks.toSparkDF(list)
        ```
        Returns:
            pyspark.sql.dataframe.DataFrame: The converted Spark DataFrame
        """
        sparkDF = self.spark.createDataFrame(df)
        return sparkDF
    
    def fromSparkToPandas(self, sparkDf: pyspark.sql.dataframe.DataFrame):
        """
        Convert Spark DataFrame to Pandas DataFrame

        Args:
            df (pyspark.sql.dataframe.DataFrame): Spark DataFrame to be converted to Pandas DataFrame.

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        list = [{"some":"data"}]
        sparkDf = databricks.toSparkDF(df)
        pandasDf = fromSparkToPandas(sparkDf)
        ```
        Returns:
            pyspark.sql.dataframe.DataFrame: The converted Spark DataFrame
        """
        return sparkDf.toPandas()
    
    def query(self, query: str):
        """
        Execute an SQL query

        Args:
            query (str): An SQL query

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        queryResult = databricks.query("SELECT * FROM schema.table_1")
        ```
        Returns:
            DataFrame: A Pandas DataFrame containing the query's results (if available)
        """
        result = self.spark.sql(query)
        return self.fromSparkToPandas(result)
    
    def readTable(self, table_name: str):
        """
        Read the contents of a given table

        Args:
            table_name (str): Name of an existing table

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        tableData = databricks.readTable("schema.table_1")
        ```
        Returns:
            DataFrame: A Pandas DataFrame containing the requested table's data
        """
        result = self.spark.sql("select * from "+ table_name)
        return self.fromSparkToPandas(result)

    def createTable(self, table_name: str, df: DataFrame):
        """
        Write a new table using a given Pandas DataFrame

        Args:
            table_name (str): Name of the new table to be created
            df (DataFrame): DataFrame to be written as a table

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        df = [{"id":"1", "name":"Joe"}]
        new_table = databricks.createTable("schema.table_1", df)
        ```
        Returns:
            DataFrame: A Pandas DataFrame containing the newly created table's data
        """
        data = self.toSparkDF(df)
        data.createOrReplaceTempView("new_table")

        self.query("CREATE TABLE " + table_name +" USING DELTA AS SELECT * FROM new_table;")

        print("Table " +table_name+ " created.")
        return self.readTable(table_name)    

    def deleteTable(self, table_name: str):
        """
        Delete an existing table

        Args:
            table_name (str): Name of the table to be overwritten
            df (DataFrame): DataFrame to be used to overwrite the table

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()]
        databricks.deleteTable("schema.table_1")
        ```
        """
        self.spark.sql("DROP TABLE "+ table_name)
        print("Table deleted.")

    def full_refresh(self, table_name: str, df: DataFrame):
        """
        Overwrite an existing table with data from a Pandas DataFrame

        Args:
            table_name (str): Name of the table to be overwritten
            df (DataFrame): DataFrame to be used to overwrite the table

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        df = [{"id":"1", "name":"Joe"}]
        new_table = databricks.full_refresh("schema.table_1", df)
        ```
        Returns:
            DataFrame: A Pandas DataFrame containing the new data in the overwitten table
        """
        data = self.toSparkDF(df)
        data.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return self.readTable(table_name)
        
    # TODO: add a method for extracting a usable JSON schema for the sets
    # TODO: Parse the matchedSet as a notMatchedSet
    def upsert(self, table_name: str, df: DataFrame, matchedSet: dict, notMatchedSet: dict, pk: str):
        """
        Upsert the data to an existing table. If the record exists, the function updates the record in the table.
        If it doesn't exist, the function creates a new record in the table

        Args:
            table_name (str): Name of the table to perform the upsert operation on
            df (DataFrame): DataFrame to be used to upsert the table
            matchedSet (dict): A dictionary defining the changes that should take place in case a matching record is found
            notMatchedSet(dict): A dictionary defining the fields that will be added in case no records exist
            pk (str): The primary key that should be joined on
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

        notMatch = {
                "Id": "updates.Id",
                "AccountId": "updates.AccountId",
                "Name": "updates.Name",
                "FirstName": "updates.FirstName",
                "LastName": "updates.LastName",
                "ContactEMail": "updates.ContactEMail",
                "MailingCity": "updates.MailingCity"
                }
        upsert("raw.c4c_test4", data, match, notMatch, "Id")
        ```
        """
        # Parse the dictionaries into a string readable by SQL
        # Parsing the matched dictionary
        matchedString = json.dumps(matchedSet)
        matchedString = matchedString.replace('"', '')
        matchedString = matchedString.replace('{', '')
        matchedString = matchedString.replace('}', '')
        matchedString = matchedString.replace('}', '')
        matchedString = matchedString.replace(':', '=')

        # Parsing not-matched keys
        notMatchedKeys = str(notMatchedSet.keys())
        notMatchedKeys = notMatchedKeys.replace('dict_keys([', '')
        notMatchedKeys = notMatchedKeys.replace("'", "")
        notMatchedKeys = notMatchedKeys.replace('])', '')

        # Parsing not-matched values
        notMatchedValues = str(notMatchedSet.values())
        notMatchedValues = notMatchedValues.replace('dict_values([', '')
        notMatchedValues = notMatchedValues.replace("'", "")
        notMatchedValues = notMatchedValues.replace('])', '')

        # Check the column names and types match
        ## Retrieve the table as a Spark DataFrame
        deltaDf = self.readTable(table_name)
        upsertData = self.spark.createDataFrame(df)
        upsertDf = self.fromSparkToPandas(upsertData)
        ## Sort columns alphabetically
        deltaDf = deltaDf.sort_index(axis=1)
        upsertDf = upsertDf.sort_index(axis=1)

        if (deltaDf.columns == upsertDf.columns).all():
            # Upserts the values of a given dataframe/Delta table into an existing Delta table
            upsertData.createOrReplaceTempView("updates")

            self.query("MERGE INTO " + table_name + " AS main USING updates ON main." + pk +" = updates." + pk +
            " WHEN MATCHED THEN UPDATE SET " + matchedString + " WHEN NOT MATCHED THEN INSERT (" + notMatchedKeys
            + ") VALUES (" + notMatchedValues + ")")

            print("Data upserted successfully.")
        else:
            print("Columns mismatch. Please adhere to the column names and data types used in the source table.")
    
    def createSchema(self, schemaName: str):
        """
        Create a schema for storing tables

        Args:
            schemaName (str): Name of the new schema to be created

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.createSchema("schema_1")
        ```
        """
        self.spark.sql("CREATE SCHEMA " + schemaName)
        print("Schema "+ schemaName + " created.")

    def deleteSchema(self, schemaName: str):
        """
        Delete a schema

        Args:
            schemaName (str): Name of the schema to be deleted

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.deleteSchema("schema_1")
        ```
        """
        self.spark.sql("DROP SCHEMA " + schemaName)
        print("Schema "+ schemaName + " deleted.")

    def printSchema(self, table_name: str):
        """
        Print the details of a table's data schema

        Args:
            table_name (str): Name of the table whose schema will be printed

        Example:
        ```python
        from viadot.sources import Databricks
        databricks = Databricks()
        databricks.printSchema("table_1")
        ```
        """
        table = self.readTable(table_name)
        table = self.toSparkDF(table)
        print(table.printSchema())