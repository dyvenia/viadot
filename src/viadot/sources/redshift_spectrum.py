"""Amazon Redshift Spectrum connector."""

from typing import Literal

import pandas as pd


try:
    import awswrangler as wr
    import boto3
    import redshift_connector
except ModuleNotFoundError as e:
    msg = "Missing required modules to use RedshiftSpectrum source."
    raise ImportError(msg) from e

from typing import Any

from pydantic import BaseModel, root_validator

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source


class RedshiftSpectrumCredentials(BaseModel):
    region_name: str  # The name of the AWS region.
    aws_access_key_id: str  # The AWS access key ID.
    aws_secret_access_key: str  # The AWS secret access key.
    profile_name: str | None = None  # The name of the IAM profile to use.
    endpoint_url: str | None = None  # The endpoint of the S3 service.

    # Below credentials are required only by some methods.
    #
    # The name of a AWS Secret holding credentials to a Redshift instance.
    # This AWS Secret has to include the following configuration:
    # host: Optional[str]
    # port: Optional[str] = "5439"
    # username: Optional[str]
    # password: Optional[str]
    # engine: Optional[str] = "redshift"
    # dbname: Optional[str]
    credentials_secret: str | None = None
    iam_role: str | None = None  # The IAM role to assume. Used by `create_schema()`.

    @root_validator(pre=True)
    def is_configured(cls, credentials: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
        """Validate the credentials configuration."""
        profile_name = credentials.get("profile_name")
        region_name = credentials.get("region_name")
        aws_access_key_id = credentials.get("aws_access_key_id")
        aws_secret_access_key = credentials.get("aws_secret_access_key")

        profile_credential = profile_name and region_name
        direct_credential = aws_access_key_id and aws_secret_access_key and region_name

        if not (profile_credential or direct_credential):
            msg = "Either `profile_name` and `region_name`, or `aws_access_key_id`,"
            msg += " `aws_secret_access_key`, and `region_name` must be specified."
            raise CredentialError(msg)
        return credentials


class RedshiftSpectrum(Source):
    def __init__(
        self,
        credentials: RedshiftSpectrumCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """A class for working with Amazon Redshift Spectrum.

        Note that internally, AWS SDK refers to schemas as "databases", as external
        schemas correspond to AWS Glue databases. However, to keep consistent naming
        with all other viadot sources, we use the word "schema" instead.

        Args:
        credentials (RedshiftSpectrumCredentials, optional): RedshiftSpectrumCredentials
            credentials. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.

        Examples:
        ```python
        from viadot.sources import RedshiftSpectrum

        with RedshiftSpectrum(config_key="redshift_spectrum") as redshift:
            redshift.get_schemas()
        ```
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(RedshiftSpectrumCredentials(**raw_creds))

        super().__init__(*args, credentials=validated_creds, **kwargs)

        if not self.credentials:
            self.logger.debug(
                "Credentials not specified. Falling back to `boto3` default credentials."
            )

        if self.credentials:
            endpoint_url = self.credentials.get("endpoint_url")
            if endpoint_url:
                wr.config.s3_endpoint_url = endpoint_url

        self._session = None
        self._con = None

    def __enter__(self):  # noqa: D105
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # noqa: D105, ANN001
        if self._con:
            self._con.close()
            self._con = None

    @property
    def session(self) -> boto3.session.Session:
        """A singleton-like property for initiating an AWS session with boto3.

        Note that this is not an actual session, so it does not need to be closed.
        """
        if not self._session:
            self._session = boto3.session.Session(
                region_name=self.credentials.get("region_name"),
                profile_name=self.credentials.get("profile_name"),
                aws_access_key_id=self.credentials.get("aws_access_key_id"),
                aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
            )
        return self._session

    @property
    def con(self) -> redshift_connector.Connection:
        """A singleton-like property for establishing a connection."""
        if not self._con:
            if self.credentials.get("credentials_secret"):
                self._con = wr.redshift.connect(
                    boto3_session=self.session,
                    timeout=10,
                    secret_id=self.credentials.get("credentials_secret"),
                )
            else:
                msg = "The `credentials_secret` config is required to connect to Redshift."
                raise ValueError(msg)
        return self._con

    def from_df(
        self,
        df: pd.DataFrame,
        to_path: str,
        schema: str,
        table: str,
        extension: Literal[".parquet", ".csv"] = ".parquet",
        if_exists: Literal["overwrite", "overwrite_partitions", "append"] = "overwrite",
        partition_cols: list[str] | None = None,
        sep: str = ",",
        **kwargs,
    ) -> None:
        """Upload a pandas `DataFrame` into a CSV or Parquet file.

        For a full list of available parameters, please refer to the official
        documentation:
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_parquet.html
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_csv.html

        Args:
            df (pd.DataFrame): Pandas `DataFrame`.
            to_path (str): Path to Amazon S3 folder where the table will be located. If
                needed, a bottom-level directory named f"{table}" is automatically
                created, so that files are always located in a folder named the same as
                the table.
            schema (str): The name of the schema.
            table (str): The name of the table to load the data into.
            extension (Literal[".parquet", ".csv"], optional): Required file type.
                Defaults to '.parquet'.
            if_exists (Literal["overwrite", "overwrite_partitions", "append"], optional
                ): 'overwrite' to recreate the table, 'overwrite_partitions' to only
                recreate the partitions, 'append' to append the data. Defaults to
                'overwrite'.
            partition_cols (List[str], optional): List of column names that will be used
                to create partitions. Only takes effect if dataset=True. Defaults to
                None.
            sep (str, optional): Field delimiter for the output file. Defaults to ','.
        """
        # Ensure files are in a directory named {table}.
        if not to_path.rstrip("/").endswith(table):
            to_path = to_path.rstrip("/") + "/" + table

        if extension == ".parquet":
            wr.s3.to_parquet(
                boto3_session=self.session,
                df=df,
                path=to_path,
                mode=if_exists,
                dataset=True,
                database=schema,
                table=table,
                partition_cols=partition_cols,
                **kwargs,
            )
        elif extension == ".csv":
            wr.s3.to_csv(
                boto3_session=self.session,
                df=df,
                path=to_path,
                dataset=True,
                database=schema,
                table=table,
                sep=sep,
                **kwargs,
            )
        else:
            msg = "Only CSV and parquet formats are supported."
            raise ValueError(msg)

    def to_df(
        self,
        schema: str,
        table: str,
        **kwargs,
    ) -> pd.DataFrame:
        """Read a table from an external schema into a pandas `DataFrame`.

        For a full list of available parameters, please refer to the official
        documentation:
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.read_parquet_table.html

        Args:
            schema (str): The name of the schema.
            table (str): The name of the table to load.
        """
        return wr.s3.read_parquet_table(
            boto3_session=self.session,
            database=schema,
            table=table,
            **kwargs,
        )

    def drop_table(
        self,
        schema: str,
        table: str,
        remove_files: bool = True,
    ) -> None:
        """Drop a table from a specified external schema.

        Drops a table, including related files from Amazon S3, if specified.

        Args:
            schema (str): The name of the schema.
            table (str): The name of the table to drop.
            remove_files (bool, optional): If True, Amazon S3 file related to the table
                will be removed. Defaults to True.
        """
        if remove_files:
            table_location = wr.catalog.get_table_location(
                boto3_session=self.session,
                database=schema,
                table=table,
            )
            wr.s3.delete_objects(boto3_session=self.session, path=table_location)

        wr.catalog.delete_table_if_exists(
            boto3_session=self.session, database=schema, table=table
        )

    def get_tables(
        self,
        schema: str,
    ) -> list[str]:
        """Returns a list of tables in a specified schema.

        Args:
            schema (str): The name of the schema.
        """
        get_tables_query = f"SELECT t.tablename FROM SVV_EXTERNAL_TABLES t WHERE t.schemaname = '{schema}'"  # noqa: S608
        with self.con.cursor() as cursor:
            tables_info = cursor.execute(get_tables_query).fetchall()
        return [table_info[0] for table_info in tables_info]

    def _check_if_table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists in a specified Redshift Spectrum external schema.

        Args:
            schema (str): The name of the schema.
            table (str): The name of the table to verify.

        Returns:
            bool: Whether the table exists.
        """
        return table in self.get_tables(schema=schema)

    def create_schema(
        self,
        schema: str,
    ) -> None:
        """Create an external schema in Amazon Redshift Spectrum.

        This involves two steps:
        - creating a Glue database
        - creating an external schema in Redshift, pointing to above Glue database

        Args:
            schema (str): The name of the schema.
        """
        self._create_glue_database(database=schema, exist_ok=True)
        create_schema_query = f"""
create external schema if not exists "{schema}" from data catalog
database '{schema}'
iam_role '{self.credentials.get("iam_role")}'
region '{self.credentials.get("region_name")}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
"""
        with self.con.cursor() as cursor:
            cursor.execute(create_schema_query)
            self.con.commit()

    def _create_glue_database(
        self,
        database: str,
        exist_ok: bool = False,
    ):
        """Create an AWS Glue database.

        Args:
            database (str): The name of the database.
            exist_ok (bool, optional): Whether to skip if the database already exists.
                If set to False, will throw `AlreadyExistsException`. Defaults to False.
        """
        wr.catalog.create_database(
            name=database,
            boto3_session=self.session,
            exist_ok=exist_ok,
        )

    def get_schemas(self) -> list[str]:
        """Returns a list of schemas in the current Redshift Spectrum database."""
        # External Redshift schemas
        get_schemas_query = "SELECT schemaname FROM SVV_EXTERNAL_SCHEMAS"
        with self.con.cursor() as cursor:
            schema_names: tuple[list] = cursor.execute(get_schemas_query).fetchall()
        external_schemas = [schema_name[0] for schema_name in schema_names]

        # Glue databases.
        schema_infos = wr.catalog.get_databases(boto3_session=self.session)
        glue_schemas = [schema_info["Name"] for schema_info in schema_infos]

        # An external Redshift schema is a Spectrum schema only if it's also a Glue
        # database.
        return [schema for schema in external_schemas if schema in glue_schemas]

    def _check_if_schema_exists(self, schema: str) -> bool:
        """Check if a schema exists in Amazon Redshift Spectrum.

        Args:
            schema (str): The name of the schema.

        Returns:
            bool: Whether the schema exists.
        """
        return schema in self.get_schemas()

    def _is_spectrum_schema(self, schema: str) -> bool:
        """Check if a Redshift schema is a Spectrum schema.

        Args:
            schema (str): The name of the schema.

        Returns:
            bool: Whether the schema is a Spectrum schema.
        """
        return self._check_if_schema_exists(schema)

    def drop_schema(self, schema: str, drop_glue_database: bool = False) -> None:
        """Drop a Spectrum schema. If specified, also drop the underlying Glue database.

        Args:
            schema (str): The name of the schema.
        """
        if not self._is_spectrum_schema(schema):
            msg = f"Schema {schema} is not a Spectrum schema."
            raise ValueError(msg)

        drop_external_schema_query = f"DROP SCHEMA IF EXISTS {schema}"
        with self.con.cursor() as cursor:
            cursor.execute(drop_external_schema_query)
            self.con.commit()

        if drop_glue_database:
            wr.catalog.delete_database(
                name=schema,
                boto3_session=self.session,
            )
