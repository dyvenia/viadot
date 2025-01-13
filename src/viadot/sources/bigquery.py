"""'bigquery.py'."""

from typing import Literal

from google.oauth2 import service_account
import numpy as np
import pandas as pd
import pandas_gbq
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
    auth_uri: str = "https://accounts.google.com/o/oauth2/auth"
    token_uri: str = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url: str = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url: str


class BigQuery(Source):
    """Class to connect with Bigquery project and SQL tables.

    Documentation for this API is located at: https://cloud.google.com/bigquery/docs.
    """

    def __init__(
        self,
        *args,
        credentials: BigQueryCredentials | None = None,
        config_key: str | None = None,
        **kwargs,
    ):
        """Create an instance of the BigQuery class.

        Args:
            credentials (BigQueryCredentials, optional): BigQuery credentials.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding
                relevant credentials. Defaults to None.
        """
        credentials = credentials or get_source_credentials(config_key) or None
        if credentials is None:
            message = "Missing credentials."
            raise CredentialError(message)

        validated_creds = dict(BigQueryCredentials(**credentials))

        super().__init__(*args, credentials=validated_creds, **kwargs)

        credentials_service_account = (
            service_account.Credentials.from_service_account_info(credentials)
        )

        self.project_id = credentials["project_id"]

        pandas_gbq.context.credentials = credentials_service_account
        pandas_gbq.context.project = self.project_id

    def _get_list_datasets_query(self) -> str:
        """Get datasets from BigQuery project.

        Returns:
            str: Custom query.
        """
        return f"""SELECT schema_name
                FROM {self.project_id}.INFORMATION_SCHEMA.SCHEMATA
                """  # noqa: S608

    def _get_list_tables_query(self, dataset_name: str) -> str:
        """Get tables from BigQuery dataset. Dataset is required.

        Args:
            dataset_name (str): Dataset from Bigquery project.

        Returns:
            str: Custom query.
        """
        return f"""SELECT table_name
                FROM {self.project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES
                """  # noqa: S608

    def _list_columns(self, dataset_name: str, table_name: str) -> np.ndarray:
        """Get columns from BigQuery table. Dataset name and Table name are required.

        Args:
            dataset_name (str): Dataset from Bigquery project.
            table_name (str): Table name from given dataset.

        Returns:
            np.ndarray: List of table names from the BigQuery dataset.
        """
        query = f"""SELECT column_name
                FROM {self.project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name="{table_name}"
                """  # noqa: S608
        df_columns = self._get_google_bigquery_data(query)

        return df_columns["column_name"].values

    def _get_google_bigquery_data(self, query: str) -> pd.DataFrame:
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

    @add_viadot_metadata_columns
    def to_df(
        self,
        query: str | None = None,
        dataset_name: str | None = None,
        table_name: str | None = None,
        date_column_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """Generate a DataFrame from the API response of a queried BigQuery table.

        Args:
            query (str): SQL query to querying data in BigQuery.
                Defaults to None.
                Espetial queries:
                -----------------
                If the words "tables" or "datasets" are passed in this parameter all
                tables and data sets will be returned as special internal queries.
            dataset_name (str, optional): Dataset name. Defaults to None.
            table_name (str, optional): Table name. Defaults to None.
            date_column_name (str, optional): The user can provide the name of the date.
                If the user-specified column does not exist, all data will be retrieved
                from the table. Defaults to None.
            start_date (str, optional): Parameter to pass start date e.g.
                "2022-01-01". Defaults to None.
            end_date (str, optional): Parameter to pass end date e.g.
                "2022-01-01". Defaults to None.
            columns (list[str], optional): List of columns from given table name.
                Defaults to None.
            if_empty (Literal[warn, skip, fail], optional): What to do if there is no
                data. Defaults to "warn".

        Returns:
            pd.DataFrame: DataFrame with the data.
        """
        if query == "tables":
            query = self._get_list_tables_query(dataset_name=dataset_name)

        elif query == "datasets":
            query = self._get_list_datasets_query()

        elif (
            query is not None and "select" in query.lower() and "from" in query.lower()
        ):
            self.logger.info("query has been provided!")
        else:
            if date_column_name or columns:
                table_columns = self._list_columns(
                    dataset_name=dataset_name, table_name=table_name
                )

            if columns is None or not set(columns).issubset(set(table_columns)):
                self.logger.warning(
                    "Some of the columns provided are either, not in the table or "
                    + "the list is empty. Downloading all the data instead."
                )
                columns = "*"
            else:
                columns = ", ".join(columns)

            if date_column_name:
                if (
                    not set([date_column_name]).issubset(set(table_columns))  # noqa: C405
                    or start_date is None
                    or end_date is None
                ):
                    self.logger.warning(
                        f"'{date_column_name}' column is not recognized, "
                        + f"or either `start_date`: {start_date} or `end_date`: "
                        + f"{end_date} is None.\n"
                        + "Downloading all the data instead."
                    )
                    query = None
                else:
                    self.logger.info(
                        f"Filtering data from date {start_date} to {end_date}"
                    )
                    query = f"""
                        SELECT {columns}
                            FROM `{self.project_id}.{dataset_name}.{table_name}`
                            WHERE {date_column_name} BETWEEN
                                PARSE_DATE("%Y-%m-%d", "{start_date}") AND
                                PARSE_DATE("%Y-%m-%d", "{end_date}")
                            ORDER BY {date_column_name} DESC
                    """  # noqa: S608

            if query is None:
                query = (
                    f"SELECT {columns} "
                    + f"FROM `{self.project_id}.{dataset_name}.{table_name}`"
                )

        df = self._get_google_bigquery_data(query)

        if df.empty:
            self._handle_if_empty(if_empty)

        self.logger.info(f"Downloaded the data from the table name: '{table_name}'.")

        return df
