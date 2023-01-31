import json
from typing import Any, Dict, List

import pandas as pd
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from ..exceptions import DBDataAccessError
from ..sources import BigQuery
from .azure_key_vault import AzureKeyVaultSecret

logger = logging.get_logger()


class BigQueryToDF(Task):
    """
    The task for querying BigQuery and saving data as the data frame.
    """

    def __init__(
        self,
        dataset_name: str = None,
        table_name: str = None,
        start_date: str = None,
        end_date: str = None,
        date_column_name: str = "date",
        credentials_key: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Initialize BigQueryToDF object. For querying on database - dataset and table name is required.
        The name of the project is taken from the config/credential json file so there is no need to enter its name.

        There are 3 cases:
            If start_date and end_date are not None - all data from the start date to the end date will be retrieved.
            If start_date and end_date are left as default (None) - the data is pulled till "yesterday" (current date -1)
            If the column that looks like a date does not exist in the table, get all the data from the table.

        Args:
            dataset_name (str, optional): Dataset name. Defaults to None.
            table_name (str, optional): Table name. Defaults to None.
            date_column_name (str, optional): The query is based on a date, the user can provide the name
            of the date columnn if it is different than "date". If the user-specified column does not exist,
            all data will be retrieved from the table. Defaults to "date".
            start_date (str, optional): A query parameter to pass start date e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A query parameter to pass end date e.g. "2022-01-01". Defaults to None.
            credentials_key (str, optional): Credential key to dictionary where details are stored (local config).
            credentials can be generated as key for User Principal inside a BigQuery project. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret for Bigquery project. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.start_date = start_date
        self.end_date = end_date
        self.date_column_name = date_column_name
        self.credentials_key = credentials_key
        self.credentials_secret = credentials_secret
        self.vault_name = vault_name

        super().__init__(
            name="bigquery_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download BigQuery data to a DF"""
        super().__call__(self)

    @defaults_from_attrs(
        "dataset_name",
        "table_name",
        "date_column_name",
        "start_date",
        "end_date",
        "credentials_key",
        "credentials_secret",
        "vault_name",
    )
    def run(
        self,
        dataset_name: str = None,
        table_name: str = None,
        date_column_name: str = "date",
        start_date: str = None,
        end_date: str = None,
        credentials_key: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        credentials = None
        if credentials_secret:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials = json.loads(credentials_str)

        bigquery = BigQuery(credentials_key=credentials_key, credentials=credentials)
        project = bigquery.get_project_id()
        try:
            table_columns = bigquery.list_columns(
                dataset_name=dataset_name, table_name=table_name
            )
            if date_column_name not in table_columns:
                logger.warning(
                    f"'{date_column_name}' column is not recognized. Downloading all the data from '{table_name}'."
                )
                query = f"SELECT * FROM `{project}.{dataset_name}.{table_name}`"
                df = bigquery.query_to_df(query)
            else:
                if start_date is not None and end_date is not None:
                    query = f"""SELECT * FROM `{project}.{dataset_name}.{table_name}` 
                    where {date_column_name} between PARSE_DATE("%Y-%m-%d", "{start_date}") and PARSE_DATE("%Y-%m-%d", "{end_date}") 
                    order by {date_column_name} desc"""
                else:
                    query = f"""SELECT * FROM `{project}.{dataset_name}.{table_name}` 
                    where {date_column_name} < CURRENT_DATE() 
                    order by {date_column_name} desc"""

                df = bigquery.query_to_df(query)
                logger.info(
                    f"Downloaded the data from the '{table_name}' table into the data frame."
                )

        except DBDataAccessError:
            logger.warning(
                f"Table name '{table_name}' or dataset '{dataset_name}' not recognized. Returning empty data frame."
            )
            df = pd.DataFrame()
        return df
