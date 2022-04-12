from typing import List
import pandas_gbq
import pandas as pd
from google.oauth2 import service_account

from ..config import local_config
from ..exceptions import CredentialError, DBDataAccessError
from .base import Source


class BigQuery(Source):
    """
    Class to connect with Bigquery project and tables.

    Note that credentials used for authentication can be generated only for User Principal
    who has access to specific BigQuery project.
    """

    def __init__(self, credentials_key: str = None, *args, **kwargs):
        """
        Create an instance of BigQuery class.

        Args:
            credentials_key (str): Credential key to dictionary where details are stored. Credentials can be generated as key
            for User Principal inside a BigQuery project. Defaults to None.

        Raises:
            CredentialError: In case credentials cannot be found.
        """
        self.credentials_raw = local_config.get(credentials_key)
        if self.credentials_raw is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=self.credentials_raw, **kwargs)

        credentials = service_account.Credentials.from_service_account_info(
            self.credentials_raw
        )
        self.client = pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = self.credentials_raw["project_id"]

    def list_datasets(self) -> List[str]:
        """
        Get datasets from BigQuery project.

        Returns:
            List[str]: List of datasets from BigQuery project.
        """
        query = f"""SELECT schema_name 
                FROM {self.get_project_id()}.INFORMATION_SCHEMA.SCHEMATA
                """
        df = self.query_to_df(query)
        return df["schema_name"].values

    def list_tables(self, dataset: str) -> List[str]:
        """
        Get tables from BigQuery dataset. Dataset is required.

        Args:
            dataset (str): Dataset from Bigquery project.

        Returns:
            List[str]: List of tables from BigQuery dataset.
        """
        query = f"""SELECT table_name 
                FROM {self.get_project_id()}.{dataset}.INFORMATION_SCHEMA.TABLES
                """
        df = self.query_to_df(query)
        return df["table_name"].values

    def list_columns(self, dataset: str, table: str) -> List[str]:
        """
        Get columns from BigQuery table. Dataset name and Table name are required.

        Args:
            dataset (str): Dataset from Bigquery project.
            table (str): Table name from given dataset.
        Returns:
            List[str]: List of tables from BigQuery dataset
        """
        query = f"""SELECT column_name
                FROM {self.get_project_id()}.{dataset}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name="{table}"
                """
        df = self.query_to_df(query)
        return df["column_name"].values

    def get_project_id(self) -> str:
        """
        Get project id from json file generated for specific project.

        Returns:
            str: Project name.
        """
        return self.credentials_raw["project_id"]

    def query_to_df(self, query: str) -> pd.DataFrame:
        """
        Query throught Bigquery table.

        Args:
            query (str): SQL-Like Query to return data values.

        Raises:
            DBDataAccessError: When dataset name or table name are incorrect.

        Returns:
            pd.DataFrame: Query result.
        """
        try:
            return pandas_gbq.read_gbq(query)
        except:
            raise DBDataAccessError
