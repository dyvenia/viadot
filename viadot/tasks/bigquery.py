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
        credentials_key: str = "BIGQUERY",
        credentials: dict = None,
        *args,
        **kwargs,
    ):
        """
        Initialize BigQueryToDF task.

        Args:
            credentials_key (str, optional): Credential key to dictionary where details are stored. Defaults to "BIGQUERY".
            credentials (dict, optional): Credentials dictionary - credentials can be generate as key
            for User Principal inside a BigQuery project. Defaults to None.
        """
        self.credentials_key = credentials_key

        super().__init__(
            name="bigquery_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download BigQuery data to a DF"""
        super().__call__(self)

    @defaults_from_attrs("credentials_key")
    def run(
        self,
        project: str = None,
        dataset: str = None,
        table: str = None,
        credentials_key: str = "BIGQUERY",
        credentials: dict = None,
        **kwargs,
    ) -> None:
        bigq = BigQuery(credentials_key=credentials_key, credentials=credentials)

        project = project or bigq.list_projects()
        dataset = dataset or bigq.list_datasets()[0]
        table = table or bigq.list_tables(dataset)[0]
        query = f"""SELECT * FROM `{dataset}.{table}` where date < '2022-03-22' order by date desc"""

        query_job = bigq.query(query)
        df = query_job.result().to_dataframe()

        return df
