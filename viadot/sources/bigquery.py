from pydoc import cli
from typing import List
from black import json
from .base import Source
from google.cloud import bigquery
from ..config import local_config
from ..exceptions import CredentialError

import os


class BigQuery(Source):
    """
    Class to connect with Bigquery Project and tables.

    Note that credentials used for authentication can be generated for User Principal
    who have access to specific BigQuery project.
    """

    def __init__(
        self, credentials_key: str = None, credentials: dict = None, *args, **kwargs
    ):
        """
        Create an instance of BigQuery class.

        Args:
            credentials_key (str): Credential key to dictionary where details are stored. Defaults to None.
            credentials (dict, optional): Credentials dictionary - credentials can be generate as key
            for User Principal inside a BigQuery project. Defaults to None.

        Raises:
            CredentialError: In case credentials cannot be found.
        """
        self.path_json = "credentials_json.json"
        DEFAULT_CREDENTIALS = local_config.get(credentials_key)
        credentials = credentials or DEFAULT_CREDENTIALS
        if credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

        with open(self.path_json, "w") as json_file:
            json.dump(credentials, json_file)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.path_json
        self.client = bigquery.Client()
        os.remove(self.path_json)

    def list_projects(self) -> str:
        """
        Get project name from BigQuery.

        Returns:
            str: Project name
        """
        return self.client.project

    def list_datasets(self) -> List[str]:
        """
        Get datasets from BigQuery project.

        Returns:
            List[str]: List of datasets from BigQuery project.
        """
        list_datasets = list(self.client.list_datasets())
        datasets_name = [dataset.dataset_id for dataset in list_datasets]
        return datasets_name

    def list_tables(self, dataset: str = None) -> List[str]:
        """
        Get tables from BigQuery dataset.

        Args:
            dataset (str, optional): Dataset from Bigquery project. Defaults to None.

        Returns:
            List[str]: List of tables from BigQuery dataset
        """
        tables = self.client.list_tables(dataset)
        tables_name = [table.table_id for table in tables]
        return tables_name

    def query(self, query: str = None) -> bigquery.job.query.QueryJob:
        """
        Query throught Bigquery table.

        Args:
            query (str, optional): String with query. Defaults to None.

        Returns:
            bigquery.job.query.QueryJob: Query result.
        """
        return self.client.query(query)
