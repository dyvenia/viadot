import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities import logging
from prefect.tasks.secrets import PrefectSecret

from ..exceptions import DBDataAccessError
from ..sources import BigQuery

logger = logging.get_logger()


class BigQueryToDF(Task):
    """
    The task for querying BigQuery and saving data as the data frame.
    """

    def __init__(
        self,
        dataset: str = None,
        table: str = None,
        start_date: str = None,
        end_date: str = None,
        date_column_name: str = "date",
        credentials_key: str = "BIGQUERY",
        *args,
        **kwargs,
    ):
        """
        Initialize BigQueryToDF object. For querying on database - dataset and table name is required.
        The project name is taken from config/credential json file.

        There are 3 cases:
            If start_date and end_date are not None - all data from the start date to the end date will be retrieved.
            If start_date and end_date are left as default (None) - the data is pulled till "yesterday" (current date -1)
            If the column that looks like a date does not exist in the table, get all the data from the table.

        Args:
            dataset (str, optional): Dataset name. Defaults to None.
            table (str, optional): Table name. Defaults to None.
            date_column_name (str, optional): The query is based on a date, the user can provide the name
            of the date columnn if it is different than "date". If the user-specified column does not exist,
            all data will be retrieved from the table. Defaults to "date".
            start_date (str, optional): A query parameter to pass start date e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A query parameter to pass end date e.g. "2022-01-01". Defaults to None.
            credentials_key (str, optional): Credential key to dictionary where details are stored.
        """
        self.credentials_key = credentials_key
        self.dataset = dataset
        self.table = table
        self.start_date = start_date
        self.end_date = end_date
        self.date_column_name = date_column_name

        super().__init__(
            name="bigquery_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download BigQuery data to a DF"""
        super().__call__(self)

    @defaults_from_attrs(
        "dataset",
        "table",
        "date_column_name",
        "start_date",
        "end_date",
        "credentials_key",
    )
    def run(
        self,
        dataset: str = None,
        table: str = None,
        date_column_name: str = "date",
        start_date: str = None,
        end_date: str = None,
        credentials_key: str = "BIGQUERY",
        **kwargs,
    ) -> None:
        bigquery = BigQuery(credentials_key=credentials_key)
        project = bigquery.get_project_id()
        try:
            table_columns = bigquery.list_columns(dataset=dataset, table=table)
            if date_column_name not in table_columns:
                logger.warning(
                    f"'{date_column_name}' column is not recognized. Downloading all the data from '{table}'."
                )
                query = f"SELECT * FROM `{project}.{dataset}.{table}`"
                df = bigquery.query_to_df(query)
            else:
                if start_date is not None and end_date is not None:
                    query = f"""SELECT * FROM `{dataset}.{table}` 
                    where {date_column_name} between PARSE_DATE("%Y-%m-%d", "{start_date}") and PARSE_DATE("%Y-%m-%d", "{end_date}") 
                    order by {date_column_name} desc"""
                else:
                    query = f"""SELECT * FROM `{project}.{dataset}.{table}` 
                    where {date_column_name} < CURRENT_DATE() 
                    order by {date_column_name} desc"""

                df = bigquery.query_to_df(query)
                logger.info(
                    f"Downloaded the data from the '{table}' table into the data frame."
                )

        except DBDataAccessError:
            logger.warning(
                f"Table name '{table}' or dataset '{dataset}' not recognized. Returning empty data frame."
            )
            df = pd.DataFrame()
        return df
