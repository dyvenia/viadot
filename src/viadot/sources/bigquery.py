"""
'bigquery.py'.

Structure for the BigQuery API connector.
This module provides functionalities for connecting to BigQuery API and download the
response. It includes the following features:
- Direct connection to BigQuery API.
- Introduce any downloaded data into a Pandas Data Frame.

Typical usage example:
    bigquery = BigQuery(credentials=credentials, config_key=config_key)
    bigquery.api_connection(
        query=query,
        dataset_name=dataset_name,
        table_name=table_name,
        date_column_name=date_column_name,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    data_frame = bigquery.to_df()

BigQuery Class Attributes:
    credentials (Optional[BigQueryCredentials], optional): BigQuery credentials.
        Defaults to None.
    config_key (Optional[str], optional): The key in the viadot config holding
        relevant credentials. Defaults to None.

Functions:
    api_connection(query, dataset_name, table_name, date_column_name, start_date,
        end_date, columns): Connect to BigQuery API and generate the response.
    to_df(if_empty): Generate a Pandas Data Frame with the data in the Response object
        and metadata

Classes:
    BigQueryCredentials: Checking for values in BigQuery credentials dictionary.
    BigQuery: Class implementing the BigQuery API.
"""  # noqa: D412

from typing import List, Optional

import numpy as np
import pandas as pd
import pandas_gbq
from colorama import Fore, Style
from google.oauth2 import service_account
from pandas_gbq.gbq import GenericGBQException
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import APIError, CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class BigQueryCredentials(BaseModel):
    """Checking for values in BigQuery credentials dictionary.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str


class BigQuery(Source):
    """
    Class to connect with Bigquery project and SQL tables.

    Documentation for this API is located at: https://cloud.google.com/bigquery/docs.
    """

    def __init__(
        self,
        *args,
        credentials: Optional[BigQueryCredentials] = None,
        config_key: Optional[str] = None,
        **kwargs,
    ):
        """
        Create an instance of the Mediatool class.

        Args:
            credentials (Optional[BigQueryCredentials], optional): BigQuery credentials.
                Defaults to None.
            config_key (Optional[str], optional): The key in the viadot config holding
                relevant credentials. Defaults to None.
        """
        credentials = credentials or get_source_credentials(config_key) or None
        if credentials is None:
            raise CredentialError("Missing credentials.")

        validated_creds = dict(BigQueryCredentials(**credentials))

        super().__init__(*args, credentials=validated_creds, **kwargs)

        credentials_service_account = (
            service_account.Credentials.from_service_account_info(credentials)
        )

        self.project_id = credentials["project_id"]

        pandas_gbq.context.credentials = credentials_service_account
        pandas_gbq.context.project = self.project_id

        self.df_data = None

    def _list_datasets(self) -> str:
        """
        Get datasets from BigQuery project.

        Returns:
            str: Custom query.
        """
        query = f"""SELECT schema_name
                FROM {self.project_id}.INFORMATION_SCHEMA.SCHEMATA
                """

        return query

    def _list_tables(self, dataset_name: str) -> str:
        """
        Get tables from BigQuery dataset. Dataset is required.

        Args:
            dataset_name (str): Dataset from Bigquery project.

        Returns:
            str: Custom query.
        """
        query = f"""SELECT table_name
                FROM {self.project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES
                """

        return query

    def _list_columns(self, dataset_name: str, table_name: str) -> np.ndarray:
        """
        Get columns from BigQuery table. Dataset name and Table name are required.

        Args:
            dataset_name (str): Dataset from Bigquery project.
            table_name (str): Table name from given dataset.

        Returns:
            np.ndarray: List of table names from the BigQuery dataset.
        """
        query = f"""SELECT column_name
                FROM {self.project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name="{table_name}"
                """
        df_columns = self._gbd(query)

        return df_columns["column_name"].values

    def _gbd(self, query: str) -> pd.DataFrame:
        """Connect to BigQuery API.

        Args:
            query (str): SQL query to querying data in BigQuery. Defaults to None.

        Raises:
            APIError: Error with BigQuery API connection.

        Returns:
            pd.DataFrame: BigQuery response data.
        """
        try:
            data = pandas_gbq.read_gbq(query)
        except GenericGBQException as message:
            raise APIError(message) from message

        return data

    def api_connection(
        self,
        query: Optional[str] = None,
        dataset_name: Optional[str] = None,
        table_name: Optional[str] = None,
        date_column_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: List[str] = [""],
    ) -> None:
        """Connect to BigQuery API and generate the response.

        Args:
            query (Optional[str]): SQL query to querying data in BigQuery.
                Defaults to None.
                Espetial queries:
                -----------------
                If the words "tables" or "datasets" are passed in this parameter all
                tables and data sets will be returned as special internal queries.
            dataset_name (Optional[str], optional): Dataset name. Defaults to None.
            table_name (Optional[str], optional): Table name. Defaults to None.
            date_column_name (Optional[str], optional): The user can provide the name of
                the date. If the user-specified column does not exist, all data will be
                retrieved from the table. Defaults to None.
            start_date (Optional[str], optional): Parameter to pass start date e.g.
                "2022-01-01". Defaults to None.
            end_date (Optional[str], optional): Parameter to pass end date e.g.
                "2022-01-01". Defaults to None.
            columns (List[str], optional): List of columns from given table name.
                Defaults to [""].
        """
        if query == "tables":
            query = self._list_tables(dataset_name=dataset_name)

        elif query == "datasets":
            query = self._list_datasets()

        else:
            if date_column_name or columns:
                table_columns = self._list_columns(
                    dataset_name=dataset_name, table_name=table_name
                )

            if not set(columns).issubset(set(table_columns)):
                print(
                    f"{Fore.YELLOW}WARNING{Style.RESET_ALL}: "
                    + "Some of the columns provided are either, not in the table or "
                    + "the list is empty. Downloading all the data instead."
                )
                columns = "*"
            else:
                columns = ", ".join(columns)

            if date_column_name:
                if (
                    not set([date_column_name]).issubset(set(table_columns))
                    or start_date is None
                    or end_date is None
                ):
                    print(
                        f"{Fore.YELLOW}WARNING{Style.RESET_ALL}: "
                        + f"'{date_column_name}' column is not recognized, "
                        + f"or either `start_date`: {start_date} or `end_date`: "
                        + f"{end_date} is None.\n"
                        + "Downloading all the data instead."
                    )
                    query = None
                else:
                    print(f"Filtering data from date {start_date} to {end_date}")
                    query = f"""
                        SELECT {columns}
                            FROM `{self.project_id}.{dataset_name}.{table_name}`
                            WHERE {date_column_name} BETWEEN
                                PARSE_DATE("%Y-%m-%d", "{start_date}") AND
                                PARSE_DATE("%Y-%m-%d", "{end_date}")
                            ORDER BY {date_column_name} DESC
                    """

            if query is None:
                query = (
                    f"SELECT {columns} "
                    + f"FROM `{self.project_id}.{dataset_name}.{table_name}`"
                )

        self.df_data = self._gbd(query)

        print(f"Downloaded the data from the table name: '{table_name}'.")

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
    ) -> pd.DataFrame:
        """
        Response from the API queried BigQuery table and transforms it into DataFrame.

        Args:
            if_empty (str, optional): if_empty param is checking params passed to
                the function. Defaults to "warn".

        Returns:
            pd.DataFrame: Table of the data carried in the response.
        """
        if self.df_data.empty:
            self._handle_if_empty(if_empty)

        return self.df_data
