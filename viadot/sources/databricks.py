from viadot.config import local_config
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.dataframe as spark
from typing import Literal, Union, Tuple, List
from typing import Any, Dict
from viadot.sources.base import Source
from viadot.utils import get_fqn, build_merge_query

Record = Tuple[Any]


class Databricks(Source):
    """
    Download and manipulate data on Databricks.

    For in-depth information about Azure Databricks, see
    https://docs.microsoft.com/en-us/azure/databricks/.

    Args:
        env (str, optional): The name of the Databricks environment to use. By default, "DEV".

    credentials : Dict[str, Any], optional
        Credentials containing Databricks connection configuration and the following credentials:
            - spark.databricks.service.address
            - spark.databricks.service.token
            - spark.databricks.service.clusterId
            - spark.databricks.service.orgId
            - spark.databricks.service.port

    Alternatively, the credentials can be specified in the standard `viadot` `credentials.json`.
    """

    DEFAULT_SCHEMA = "default"

    def __init__(
        self,
        env: Literal["DEV", "QA", "PROD"],
        credentials: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):
        self.env = env
        self.credentials = credentials or local_config.get("DATABRICKS", {}).get(env)

        self.connect()

        super().__init__(*args, credentials=credentials, **kwargs)

    def connect(self) -> None:
        """
        Connect to a Databricks cluster.

        Currently, for optimization purposes, we only use `_create_spark_session()`
        when needed, and that is when the cluster specified in `credentials.json`
        does not match the one specified in `.databricks-connect`.

        In other words, below code assumes that the QA cluster specified in `credentials.json`
        is the same as the one specified in the `.databricks-connect` file.
        For more information, see the comments in `_create_spark_session()`.
        """
        if self.env == "QA":
            self.session = SparkSession.builder.getOrCreate()
        else:
            self.session = self._create_spark_session(self.env)

    def _create_spark_session(self, env: str = "DEV") -> SparkSession:
        """
        Create a connection to a Databricks cluster.
        The logic is quite complicated due to the fact that Dabricks only allows specifying
        one cluster config, therefore by default, there is no way to eg. handle environments.

        In order to work around this, we utilize the existing config and overwrite required values
        (such as `cluster_id`) and create the Spark session by hand, rather than depending on the
        `.databricks-connect` file.

        Args:
            env (str, optional): The name of the Databricks environment to use. by default "DEV"

        Returns:
            SparkSession: A configured SparkSession object used for connecting to the Databricks
            cluster.
        """
        env = env or self.env
        default_spark = SparkSession.builder.getOrCreate()
        config = SparkConf()

        # Copy all the configuration values from the current Spark Context.
        default_config_vals = default_spark.sparkContext.getConf().getAll()

        for (key, value) in default_config_vals:
            config.set(key, value)

        config.set(
            "spark.databricks.service.clusterId", self.credentials.get("cluster_id")
        )
        config.set("spark.databricks.service.port", self.credentials.get("port"))

        # Stop the Spark session we used to get existing config so that we can
        # create a new session using the new config.
        default_spark.stop()
        context = SparkContext(conf=config)
        session = SparkSession(context)
        new_spark = session.builder.config(conf=config).getOrCreate()
        return new_spark

    def to_df(self, query: str, if_empty: str = "fail") -> pd.DataFrame:
        """
        Execute a query and return a Pandas DataFrame.

        Args:
            query (str): The query to execute.
            if_empty (str, optional): What to do if the query returns no data. Defaults to "fail".

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            table_data = databricks.to_df("SELECT * FROM test_schema.test_table")
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
            df (pd.DataFrame): The Pandas DataFrame to be converted.

        Example:
            ```python
            from viadot.sources import Databricks
            import pandas as pd

            databricks = Databricks()

            data = [{"id": "1", "name": "Joe"}]
            df = pd.DataFrame(data)
            sdf = databricks._pandas_df_to_spark_df(df)
            ```
        Returns:
            spark.DataFrame: The resulting Spark DataFrame.
        """
        return self.session.createDataFrame(df)

    def _spark_df_to_pandas_df(self, sdf: spark.DataFrame) -> pd.DataFrame:
        """
        Convert a Spark DataFrame to a Pandas DataFrame.

        Args:
            df (spark.DataFrame): The Spark DataFrame to be converted.

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            data = [{"id": "1", "name": "Joe"}]
            df = pd.DataFrame(data)
            sdf = databricks._pandas_df_to_spark_df(df)
            df = databricks._spark_df_to_pandas_df(sdf)
            ```

        Returns:
            pd.DataFrame: The resulting Pandas DataFrame.
        """
        return sdf.toPandas()

    def run(
        self, query: str, fetch_type: Literal["spark", "pandas", "records"] = "records"
    ) -> Union[spark.DataFrame, pd.DataFrame, List[Record], bool]:
        """
        Execute an SQL query on a Databricks cluster.

        Args:
            query (str): The query to execute.
            fetch_type (Literal, optional): How to return the data: either
            a Spark DataFrame, a Pandas DataFrame, or records. Defaults to "records".

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            records = databricks.run("SELECT * FROM test_schema.test_table")
            ```

        Returns:
            Union[spark.DataFrame, pd.DataFrame, bool]: Either the result set of the query
            or, in case of DDL/DML queries, a boolean describing whether the query was
            excuted successfuly.
        """
        if fetch_type not in ["spark", "pandas", "records"]:
            raise ValueError(
                "Only the values 'spark', 'pandas', and 'records' are allowed for 'fetch_type'."
            )

        query_clean = query.upper().strip()
        query_keywords = ["SELECT", "SHOW", "PRAGMA", "DESCRIBE", "ANALYZE"]

        query_result = self.session.sql(query)

        if any(query_clean.startswith(word) for word in query_keywords):

            sdf = query_result

            if fetch_type == "spark":
                result = sdf
            elif fetch_type == "pandas":
                result = self._spark_df_to_pandas_df(sdf)
            else:
                result_numpy = self._spark_df_to_pandas_df(sdf).to_records()
                result = list(result_numpy)
        else:
            result = True

        return result

    def _check_if_table_exists(self, table: str, schema: str = None) -> bool:

        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        exists = self.session._jsparkSession.catalog().tableExists(schema, table)
        return exists

    def _check_if_schema_exists(self, schema: str) -> bool:
        exists = self.session._jsparkSession.catalog().databaseExists(schema)
        return exists

    def create_table_from_pandas(
        self,
        table: str,
        df: pd.DataFrame,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "skip"] = "fail",
    ) -> pd.DataFrame:
        """
        Create a Delta table from a Pandas DataFrame.

        Args:
            table (str): The name of the table to be created.
            df (pd.DataFrame): The pandas DataFrame to be used to create the table.
            schema (str, optional): The name of the schema where the table is located.
            if_exists (Literal, optional): What to do if the table already exists. Defaults to "fail".

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            data = [{"id": "1", "name": "Joe"}]
            df = pd.DataFrame(data)
            new_table = databricks.create_table_from_pandas(
                schema="test_schema", table="test_table", df=df
                )
            ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = get_fqn(schema=schema, table=table)

        exists = self._check_if_table_exists(schema=schema, table=table)

        if exists:
            if if_exists == "replace":
                self.run(f"DROP TABLE {fqn}")
            elif if_exists == "fail":
                raise ValueError(
                    f"Table '{fqn}' already exists and 'if_exists' is set to 'fail'."
                )
            elif if_exists == "append":
                inserted = self.insert_into(schema=schema, table=table, df=df)
                return inserted
            elif if_exists == "skip":
                self.logger.info(f"Table '{fqn}' already exists. Skipping...")
                return False

        sdf = self._pandas_df_to_spark_df(df)
        sdf.write.saveAsTable(fqn)

        self.logger.info(f"Table {fqn} has been created successfully.")

        return True

    def drop_table(self, table: str, schema: str = None, safe: bool = True) -> bool:
        """
        Delete an existing table.

        Args:
            table (str): Name of the new table to be created.
            schema (str, optional): The name of the schema where the table is located.
            safe (bool): Whether the `IF EXISTS` clause should be added.

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            databricks.drop_table(schema="test_schema", table="test_table")
            ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = get_fqn(schema=schema, table=table)

        if safe:
            query = f"DROP TABLE IF EXISTS {fqn}"
        else:
            query = f"DROP TABLE {fqn}"

        self.run(query)

        self.logger.info(f"Table {fqn} has been deleted successfully.")

        return True

    def _append(
        self,
        table: str,
        df: pd.DataFrame,
        schema: str = None,
    ) -> bool:
        fqn = get_fqn(schema=schema, table=table)
        sdf = self._pandas_df_to_spark_df(df)
        sdf.write.mode("append").saveAsTable(fqn)

        self.logger.info(f"Data has been successfully appended to table {fqn}.")

        return True

    def _full_refresh(
        self,
        table: str,
        df: pd.DataFrame,
        schema: str = None,
    ) -> bool:
        """
        Overwrite an existing table with data from a Pandas DataFrame.

        Args:
            table (str): The name of the table to be created.
            df (pd.DataFrame): DataFrame to be used to overwrite the table.
            schema (str, optional): The name of the schema where the table is located.

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            data = [{"id": "1", "name": "Joe"}]
            df = pd.DataFrame(data)
            databricks.insert_into(schema="test_schema", table="test_table", df=df, if_exists="replace")
            ```
        """
        fqn = get_fqn(schema=schema, table=table)
        sdf = self._pandas_df_to_spark_df(df)
        sdf.write.mode("overwrite").saveAsTable(fqn)

        self.logger.info(f"Table {fqn} has been created successfully.")

        return True

    def upsert(
        self,
        table: str,
        primary_key: str,
        df: pd.DataFrame,
        schema: str = None,
    ) -> bool:

        sdf = self._pandas_df_to_spark_df(df)
        temp_view_name = "stg"
        df.createOrReplaceTempView(temp_view_name)

        merge_query = build_merge_query(
            stg_schema=None,
            stg_table=temp_view_name,
            schema=schema,
            table=table,
            primary_key=primary_key,
            source=self,
            df=sdf,
        )
        result = self.run(merge_query)

        if result is True:
            fqn = get_fqn(schema=schema, table=table)
            self.logger.info(f"Data has been successfully upserted to '{fqn}'.")

        return result

    def insert_into(
        self,
        table: str,
        df: pd.DataFrame,
        schema: str = None,
        mode: Literal["replace", "append"] = "append",
    ) -> bool:
        """
        Insert data into a table.

        Args:
            table (str): The name of the table where the data should be inserted.
            df (pd.DataFrame): DataFrame with the data to be inserted into the table.
            schema (str, optional): The name of the schema where the table is located.
            mode (str, optional): Whether to replace existing data or append to it.
            Defaults to "append".

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            data = [{"id": "1", "name": "Joe"}]
            df = pd.DataFrame(data)
            databricks.insert_into(schema="test_schema", table="test_table", df=df)
            ```
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        exists = self._check_if_table_exists(schema=schema, table=table)

        if not exists:
            fqn = get_fqn(schema=schema, table=table)
            raise ValueError(f"Table '{fqn}' does not exist.")

        if mode == "replace":
            result = self._full_refresh(schema=schema, table=table, df=df)
        elif mode == "append":
            result = self._append(schema=schema, table=table, df=df)
        else:
            raise ValueError("Only 'replace' and 'append' modes are allowed.")

        return result

    def create_schema(
        self, schema: str, if_exists: Literal["skip", "fail"] = "skip"
    ) -> bool:
        """
        Create a schema.

        Args:
            schema (str): The name of the schema to be created.
            if_exists (Literal, optional): Whether to fail if the schema exists.

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            databricks.create_schema("test_sEchema")
            ```
        """
        if if_exists not in ["skip", "fail"]:
            raise ValueError(
                "Only the values 'skip' and 'fail' are allowed for 'if_exists'."
            )

        if if_exists == "skip":
            query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
        else:
            query = f"CREATE SCHEMA {schema}"

        result = self.run(query)
        if result is True:
            self.logger.info(f"Schema {schema} has been created successfully.")
        return result

    def drop_schema(
        self, schema: str, if_does_not_exist: Literal["skip", "fail"] = "skip"
    ) -> bool:
        """
        Delete a schema.

        Args:
            schema (str): The name of the schema to be deleted.
            if_does_not_exist (bool, optional): What to do if the schema does not exist.
            By default, "skip".

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            databricks.drop_schema("test_schema")
            ```
        """
        if if_does_not_exist not in ["skip", "fail"]:
            raise ValueError(
                "Only the values 'skip' and 'fail' are allowed for 'if_does_not_exist'."
            )

        if if_does_not_exist == "skip":
            query = f"DROP SCHEMA IF EXISTS {schema}"
        else:
            query = f"DROP SCHEMA {schema}"
        result = self.run(query)
        if result is True:
            self.logger.info(f"Schema {schema} has been deleted successfully.")
        return result

    def discover_schema(self, table: str, schema: str = None) -> Dict[str, str]:
        """
        Return the table's schema (column names and types).

        Args:
            table (str): Name of the new table to be created.
            schema (str, optional): The name of the schema where the table is located.

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            databricks.discover_schema(schema="test_schema", table="test_table")
            ```

        Returns:
            schema (Dict[str, str]): A dictionary representing the schema of the table.
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = get_fqn(schema=schema, table=table)
        cols_metadata: List[Record] = self.run(f"DESCRIBE {fqn}")
        schema = {col_metadata[1]: col_metadata[2] for col_metadata in cols_metadata}
        return schema

    def get_table_version(self, table: str, schema: str = None) -> int:
        """
        Retrieve table's version number.

        Args:
            table (str): The name of the table to rollback.
            schema (str, optional): The name of the schema where the table is located.

        Returns:
            version_number (int): The table's version number.
        """
        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        if not self._check_if_table_exists(schema=schema, table=table):
            return

        fqn = get_fqn(schema=schema, table=table)
        table_history: List[Tuple] = self.run(f"DESCRIBE HISTORY {fqn}")
        version_number_numpy = table_history[0][1]
        version_number = int(version_number_numpy)

        return version_number

    def rollback(self, table: str, version_number: int, schema: str = None) -> bool:
        """
        Rollback a table to the previous version.

        Args:
            table (str): The name of the table to rollback.
            version_number (int): The version to which to roll back.
            schema (str, optional): The name of the schema where the table is located.

        Example:
            ```python
            from viadot.sources import Databricks

            databricks = Databricks()

            schema = "test_schema"
            table = "test_table"

            version_number = databricks.get_table_version(schema=schema, table=table)

            # Perform changes on the table, in this example we are appending to the table
            data = [{"id": "1", "name": "Joe"}]
            df = pd.DataFrame(data)
            databricks.insert_into(schema=schema, table=table, df=df, if_exists="append")

            databricks.rollback(schema=schema, table=table, version_number=version_number)
            ```

        Returns:
            result (bool): Whether the operation succeeded.
        """

        if schema is None:
            schema = Databricks.DEFAULT_SCHEMA

        fqn = get_fqn(schema=schema, table=table)

        # Retrieve data from the previous table
        old_table_df = self.to_df(f"SELECT * FROM {fqn}@v{version_number}")

        # Overwrite the table with new data
        result = self.insert_into(
            schema=schema, table=table, df=old_table_df, if_exists="replace"
        )

        if result is True:
            self.logger.info(
                f"Rollback for table '{fqn}' to version #{version_number} has been completed successfully."
            )

        return result
