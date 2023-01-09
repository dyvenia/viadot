from typing import Dict, List, Literal

import awswrangler as wr
import boto3
import pandas as pd

from viadot.sources.base import Source


class AWSRedshiftSpectrum(Source):
    """
    A class for pulling data from and uploading to Spectrum.

    Args:
        profile_name (str, optional): The name of the AWS profile.
        aws_secret_access_key (str, optional): AWS secret access key
        aws_session_token (str, optional): AWS temporary session token
    """

    def __init__(
        self,
        profile_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        if profile_name:
            self.session = boto3.session.Session(profile_name=profile_name)
        elif aws_access_key_id and aws_secret_access_key:
            self.session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            self.session = boto3.session.Session()

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
        return wr.s3.does_table_exist(
            boto3_session=self.session,
            database=database,
            table=table,
        )

    def rm(
        self,
        database: str,
        table: str,
    ):
        """
        Deletes table from Glue database.

        Args:
            path (str): Path to a folder.
            database (str): AWS Glue catalog database name.
            table (str): AWS Glue catalog table name.
        """
        table_location = wr.catalog.get_table_location(
            boto3_session=self.session,
            database=database,
            table=table,
        )
        wr.catalog.delete_table_if_exists(database=database, table=table)

        wr.s3.delete_objects(path=table_location)

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
        columns_comments: Dict[str, str] = None,
    ):
        """
        Upload a pandas `DataFrame` to a csv or parquet file.

        Args:
            df (pd.DataFrame): Pandas DataFrame
            to_path (str): Path to a S3 folder where the table will be located. Defaults to None.
            extension (str): Required file type. Accepted file formats are 'csv' and 'parquet'.
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
            description (str, optional): Glue catalog table description.
            columns_comments (Dict[str,str], optional) - Glue catalog column names and
                the related comments.
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
                columns_comments=columns_comments,
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
        database: str = None,
        table: str = None,
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
