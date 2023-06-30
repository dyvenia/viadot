import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from pydantic import BaseModel

from typing import List

from ..config import get_source_credentials
from ..exceptions import CredentialError, DBDataAccessError
from .base import Source
from ..utils import add_viadot_metadata_columns


class BigQueryCredentials(BaseModel):
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
    Description:
    Class to connect with Bigquery project and SQL tables.

    Note that credentials used for authentication can be generated only for User
    Principal who has access to specific BigQuery project.

    Documentation for this API is located at: https://cloud.google.com/bigquery/docs.

    Args:
    config_key (str, optional): The key in the viadot config holding credentials.
        Defaults to None.
    credentials Dict[str, Any], optional: Credentials for API connection configuration
        (`api_key` and `user`).

    Raises:
        CredentialError: In case the credentials cannot be found/ inncorect credentials.
    """

    def __init__(
        self,
        config_key: str = None,
        credentials: BigQueryCredentials = None,
        *args,
        **kwargs,
    ):
        credentials = credentials or get_source_credentials(config_key)

        if credentials == None:
            raise CredentialError("Credentials not found.")
        
        if not (
            credentials.get("type")
            and credentials.get("project_id")
            and credentials.get("private_key_id")
            and credentials.get("private_key")
            and credentials.get("client_email")
            and credentials.get("client_id")
            and credentials.get("auth_uri")
            and credentials.get("token_uri")
            and credentials.get("auth_provider_x509_cert_url")
            and credentials.get("client_x509_cert_url")
        ):
            raise CredentialError(
                """'type', 'project_id', 'private_key_id', 'private_key',
                    'client_email', 'client_id', 'auth_uri', 'token_uri',
                    'auth_provider_x509_cert_url', 'client_x509_cert_url'
                    credentials are required."""
            )
        validated_creds = dict(BigQueryCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        credentials_service_account = (
            service_account.Credentials.from_service_account_info(credentials)
        )
        pandas_gbq.context.credentials = credentials_service_account

        pandas_gbq.context.project = self.credentials["project_id"]


    def get_project_id(self) -> str:
        """
        Description:
        Get project_id from credentials generated for specific project.

        Args:
            None.

        Returns:
            str: Project name.
        """

        return self.credentials["project_id"]

    def list_datasets(self) -> List[str]:
        """
        Description:
        Get datasets from BigQuery project.

        Args:
            None.

        Returns:
            List[str]: List of datasets from BigQuery project.
        """

        query = f"""SELECT schema_name 
                FROM {self.get_project_id()}.INFORMATION_SCHEMA.SCHEMATA
                """
        df = self.to_df(query)
        return df["schema_name"].values

    def list_tables(self, dataset_name: str) -> List[str]:
        """
        Description:
        Get tables from BigQuery dataset. Dataset is required.

        Args:
            dataset_name (str): Dataset from Bigquery project.

        Returns:
            List[str]: List of tables from BigQuery dataset.
        """

        query = f"""SELECT table_name 
                FROM {self.get_project_id()}.{dataset_name}.INFORMATION_SCHEMA.TABLES
                """
        df = self.to_df(query)
        return df["table_name"].values

    def list_columns(self, dataset_name: str, table_name: str) -> List[str]:
        """
        Description:
        Get columns from BigQuery table. Dataset name and Table name are required.

        Args:
            dataset_name (str): Dataset from Bigquery project.
            table_name (str): Table name from given dataset.

        Returns:
            List[str]: List of table names from the BigQuery dataset.
        """

        query = f"""SELECT column_name
                FROM {self.get_project_id()}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name="{table_name}"
                """
        df = self.to_df(query)
        return df["column_name"].values

    @add_viadot_metadata_columns
    def to_df(
        self,
        query: str = None,
        dataset_name: str = None,
        table_name: str = None,
        columns: List[str] = None,
        if_empty: str = "fail",
    ) -> pd.DataFrame:
        """
        Description:
           Get the response from the API queried BigQuery table and transforms it 
           into DataFrame.

        Args:
            query (str, optional): SQL-Like Query to return data values. 
                Defaults to None.
            dataset_name (str): Dataset from Bigquery project. Defaults to None.
            table_name (str): Table name from given dataset. Defaults to None.
            columns (List[str], optional): List of columns from given table name.
                Defaults to None.
            if_empty (str, optional): if_empty param is checking params passed to
                the function. Defaults to "fail".
            
        Returns:
            pd.DataFrame: Table of the data carried in the response.

        Raises:
            DBDataAccessError: When dataset_name, table_name, columns 
                or query params are incorrect.
        """

        if columns is None: columns = "*"
        
        query_custom = f"""
                SELECT {columns}
                FROM {dataset_name}.{table_name}
                """
        
        query = query or query_custom
        
        try:
            df = pandas_gbq.read_gbq(query)
        except:
            raise DBDataAccessError
        if df.empty:
            self._handle_if_empty(if_empty)

        return df