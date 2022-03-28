from viadot.exceptions import DBDataAccessError
from ..sources import BigQuery

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities import logging
from prefect.tasks.secrets import PrefectSecret

logger = logging.get_logger()


class BigQueryToDF(Task):
    """
    Task for quering BigQuery and save data as data frame.
    """

    def __init__(
        self,
        project: str = None,
        dataset: str = None,
        table: str = None,
        credentials_key: str = "BIGQUERY",
        credentials: dict = None,
        start_date: str = None,
        end_date: str = None,
        *args,
        **kwargs,
    ):
        """
        Initialize BigQueryToDF object.
        For querying on database dataset name and table name is needed.

        Args:
            project (str, optional): Project name. Defaults to None.
            dataset (str, optional): Dataset name. Defaults to None.
            table (str, optional): Table name . Defaults to None.
            credentials_key (str, optional): Credential key to dictionary where details are stored. Defaults to "BIGQUERY".
            credentials (dict, optional): Credentials dictionary - credentials can be generate as key
            for User Principal inside a BigQuery project. Defaults to None.
            start_date (str, optional): A query parameter to pass start date e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A query parameter to pass end date e.g. "2022-01-01". Defaults to None.
        """
        self.credentials_key = credentials_key
        self.credentials = credentials
        self.project = project
        self.dataset = dataset
        self.table = table
        self.start_date = start_date
        self.end_date = end_date

        super().__init__(
            name="bigquery_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download BigQuery data to a DF"""
        super().__call__(self)

    @defaults_from_attrs(
        "project",
        "dataset",
        "table",
        "credentials_key",
        "credentials",
        "start_date",
        "end_date",
    )
    def run(
        self,
        project: str = None,
        dataset: str = None,
        table: str = None,
        credentials_key: str = "BIGQUERY",
        credentials: dict = None,
        start_date: str = None,
        end_date: str = None,
        **kwargs,
    ) -> None:
        bigq = BigQuery(credentials_key=credentials_key, credentials=credentials)

        project = project or bigq.list_projects()
        dataset = dataset or bigq.list_datasets()
        table = table or bigq.list_tables(dataset)

        if (
            project in bigq.list_projects()
            and dataset in bigq.list_datasets()
            and table in bigq.list_tables(dataset)
        ):
            if start_date is not None and end_date is not None:
                query = f"""SELECT * FROM `{dataset}.{table}` 
                where date between PARSE_DATE("%Y-%m-%d", "{start_date}") and PARSE_DATE("%Y-%m-%d", "{end_date}") 
                order by date desc"""
            else:
                query = f"""SELECT * FROM `{dataset}.{table}` where date < CURRENT_DATE() order by date desc"""

            query_job = bigq.query(query)
            df = query_job.result().to_dataframe()

        else:
            raise DBDataAccessError("Wrong dataset name or table name!")

        return df
