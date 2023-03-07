from typing import Any, Dict, List, Literal

import awswrangler as wr
import boto3
import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.sources.base import Source


class RedshiftSpectrumCredentials(BaseModel):
    profile_name: str  # The name of the IAM profile to use.
    region_name: str  # The name of the AWS region.
    aws_access_key_id: str
    aws_secret_access_key: str


class RedshiftSpectrum(Source):
    """
    A class for pulling data from and uploading to the Redshift Spectrum.

    Args:
        credentials (RedshiftSpectrumCredentials, optional): RedshiftSpectrumCredentials credentials.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """

    def __init__(
        self,
        credentials: RedshiftSpectrumCredentials = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        credentials = credentials or get_source_credentials(config_key) or {}

        super().__init__(*args, credentials=credentials, **kwargs)

        if not self.credentials:
            self.logger.debug(
                "Credentials not specified. Falling back to `boto3` default credentials."
            )

        self._session = None

    @property
    def session(self) -> boto3.session.Session:
        """A singleton-like property for initiating a session to the AWS."""
        if not self._session:
            self._session = boto3.session.Session(
                region_name=self.credentials.get("region_name"),
                profile_name=self.credentials.get("profile_name"),
                aws_access_key_id=self.credentials.get("aws_access_key_id"),
                aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
            )
        return self._session

    def ls(
        self,
        database: str = None,
        name_contains: str = None,
        search_text: str = None,
    ) -> List[str]:
        """
        Returns a list of tables in a Glue database.

        Args:
            database (str, optional): Database name.
            name_contains (str, optional): Match by a specific substring of the table
                name.
            search_text (str, optional): Select only tables with the given string in
                table's properties.
        """
        df = wr.catalog.tables(
            boto3_session=self.session,
            database=database,
            name_contains=name_contains,
            search_text=search_text,
        )

        return df

    def exists(self, database: str, table: str) -> bool:
        """
        Check if a table exists in Glue database.

        Args:
            database (str): AWS Glue catalog database name.
            table (str): AWS Glue catalog table name.

        Returns:
            bool: Whether the paths exists.
        """
        return wr.catalog.does_table_exist(
            boto3_session=self.session,
            database=database,
            table=table,
        )

    def drop_table(
        self,
        database: str,
        table: str,
        remove_files: bool = True,
    ) -> None:
        """
        Deletes table from AWS Glue database and related file from AWS S3, if specified.

        Args:
            database (str): AWS Glue catalog database name.
            table (str): AWS Glue catalog table name.
            remove_files (bool, optional): If True, AWS S3 file related to the table
                will be removed. Defaults to True.
        """
        if remove_files:
            table_location = wr.catalog.get_table_location(
                boto3_session=self.session,
                database=database,
                table=table,
            )
            wr.s3.delete_objects(boto3_session=self.session, path=table_location)

        wr.catalog.delete_table_if_exists(
            boto3_session=self.session, database=database, table=table
        )

    def from_df(
        self,
        df: pd.DataFrame,
        to_path: str,
        database: str,
        table: str,
        extension: str = ".parquet",
        if_exists: Literal["overwrite", "append"] = "overwrite",
        partition_cols: List[str] = None,
        index: bool = False,
        compression: str = None,
        sep: str = ",",
        description: str = "test",
    ) -> None:
        """
        Upload a pandas `DataFrame` to a csv or parquet file.

        Args:
            df (pd.DataFrame): Pandas DataFrame.
            to_path (str): Path to a S3 folder where the table will be located.
                Defaults to None.
            extension (str): Required file type. Accepted file formats are 'csv'
                and 'parquet'.
            database (str): AWS Glue catalog database name.
            table (str): AWS Glue catalog table name.
            partition_cols (List[str]): List of column names that will be used to
                create partitions. Only takes effect if dataset=True.
            if_exists (str, optional): 'overwrite' to recreate any possible existing
                table or 'append' to keep any possible existing table. Defaults to
                overwrite.
            index (bool, optional): Write row names (index). Defaults to False.
            compression (str, optional): Compression style (None, snappy, gzip, zstd).
            sep (str, optional): Field delimiter for the output file. Defaults to ','.
            description (str, optional): AWS Glue catalog table description.
        """

        if extension == ".parquet":
            wr.s3.to_parquet(
                boto3_session=self.session,
                df=df,
                path=to_path,
                mode=if_exists,
                index=index,
                compression=compression,
                dataset=True,
                database=database,
                table=table,
                partition_cols=partition_cols,
                description=description,
            )
        elif extension == ".csv":
            wr.s3.to_csv(
                boto3_session=self.session,
                df=df,
                path=to_path,
                dataset=True,
                database=database,
                table=table,
                index=index,
                sep=sep,
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")

    def to_df(
        self,
        database: str,
        table: str,
    ) -> pd.DataFrame:
        """
        Reads a Spectrum table to a pandas `DataFrame`.

        Args:
            database (str): AWS Glue catalog database name.
            table (str): AWS Glue catalog table name.
        """

        df = wr.s3.read_parquet_table(
            boto3_session=self.session,
            database=database,
            table=table,
        )

        return df
