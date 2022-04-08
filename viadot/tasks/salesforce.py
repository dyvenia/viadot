from datetime import timedelta
from typing import Any, Dict

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Salesforce


class SalesforceUpsert(Task):
    """
    Task for upserting a pandas DataFrame to Salesforce.

    Args:
    TODO
    """

    def __init__(
        self,
        table: str = None,
        external_id: str = None,
        domain: str = "test",
        client_id: str = "viadot",
        credentials: Dict[str, Any] = None,
        env: str = "DEV",
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.table = table
        self.external_id = external_id
        self.domain = domain
        self.client_id = client_id
        self.credentials = credentials
        self.env = env

        super().__init__(
            name="salesforce_upsert",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Upserting data to Salesforce"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "table",
        "external_id",
        "domain",
        "client_id",
        "credentials",
        "env",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        df: pd.DataFrame = None,
        table: str = None,
        external_id: str = None,
        domain: str = None,
        client_id: str = None,
        credentials: Dict[str, Any] = None,
        env: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> None:
        salesforce = Salesforce(
            credentials=credentials, env=env, domain=domain, client_id=client_id
        )
        self.logger.info(f"Upserting {df.shape[0]} rows to Salesforce...")
        salesforce.upsert(df=df, table=table, external_id=external_id)
        self.logger.info(f"Successfully upserted {df.shape[0]} rows to Salesforce.")
