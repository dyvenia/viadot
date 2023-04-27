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
    A class for pulling data from and uploading to the Amazon Redshift Spectrum.

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
        database: str,
        name_contains: str = None,
        search_text: str = None,
    ) -> List[str]:
        """
        Returns a list of tables in Amazon Redshift Spectrum database.

        Args:
            database (str): Database name.
            name_contains (str, optional): Match by a specific substring of the table
                name. Defaults to None.
            search_text (str, optional): Select only tables with the given string in
                table's properties. Defaults to None.
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
        Check if a table exists in Amazon Redshift Spectrum database.

        Args:
            database (str): Amazon Redshift Spectrum catalog database name.
            table (str): Amazon Redshift Spectrum table name.

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
        Deletes table from Amazon Redshift Spectrum database, including related file from Amazon S3, if specified.

        Args:
            database (str): Amazon Redshift Spectrum catalog name.
            table (str): Amazon Redshift Spectrum table name.
            remove_files (bool, optional): If True, Amazon S3 file related to the table
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
        extension: Literal[".parquet", ".csv"] = ".parquet",
        if_exists: Literal["overwrite", "overwrite_partitions", "append"] = "overwrite",
        partition_cols: List[str] = None,
        sep: str = ",",
        description: str = None,
        **kwargs,
    ) -> None:
        """
        Upload a pandas `DataFrame` to a csv or parquet file in Amazon Redshift Spectrum.
            For full list of available parameters please refer to the official documentation:
            https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_parquet.html
            https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_csv.html

        Args:
            df (pd.DataFrame): Pandas `DataFrame`.
            to_path (str): Path to Amazon S3 folder where the table will be located.
                Defaults to None.
            database (str): Amazon Redshift Spectrum catalog name.
            table (str): Amazon Redshift Spectrum table name.
            extension (Literal[".parquet", ".csv"], optional): Required file type. Defaults to '.parquet'.
            if_exists (Literal["overwrite", "overwrite_partitions", "append"], optional):
                'overwrite' to recreate table, 'overwrite_partitions' to recreate only partitions of the table,
                'append' to add data to the table. Defaults to 'overwrite'.
            partition_cols (List[str], optional): List of column names that will be used to
                create partitions. Only takes effect if dataset=True. Defaults to None.
            sep (str, optional): Field delimiter for the output file. Defaults to ','.
            description (str, optional): Amazon Redshift Spectrum table description. Defaults to None.
        """

        if extension == ".parquet":
            wr.s3.to_parquet(
                boto3_session=self.session,
                df=df,
                path=to_path,
                mode=if_exists,
                dataset=True,
                database=database,
                table=table,
                partition_cols=partition_cols,
                description=description,
                **kwargs,
            )
        elif extension == ".csv":
            wr.s3.to_csv(
                boto3_session=self.session,
                df=df,
                path=to_path,
                dataset=True,
                database=database,
                table=table,
                sep=sep,
                **kwargs,
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")

    def to_df(
        self,
        database: str,
        table: str,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Reads Amazon Redshift Spectrum table to a pandas `DataFrame`.
            For full list of available parameters please refer to the official documentation:
            https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.read_parquet_table.html

        Args:
            database (str): Amazon Redshift Spectrum catalog name.
            table (str): Amazon Redshift Spectrum table name.
        """

        df = wr.s3.read_parquet_table(
            boto3_session=self.session,
            database=database,
            table=table,
            **kwargs,
        )

        return df
