import json
import os
from datetime import timedelta
from typing import List
from ..config import local_config
from typing import Any, Dict, List

import pandas as pd
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Salesforce


class SalesforceUpsert(Task):
    """
    Task for upserting data to Salesforce.

    Args:
    """

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        domain: str = None,
        client_id: str = None,
        dict: Dict[str, Any] = None,
        table: str = None,
        env: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.credentials = credentials
        self.domain = domain
        self.client_id = client_id

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
        "credentials",
        "domain",
        "client_id",
        "dict",
        "table",
        "env",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        credentials: Dict[str, Any] = None,
        domain: str = None,
        client_id: str = None,
        dict: Dict[str, Any] = None,
        table: str = None,
        env: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
    ) -> None:

        if not credentials:
            credentials = local_config.get("SALESFORCE").get(env)

        salesforce = Salesforce(
            credentials=credentials, domain=domain, client_id=client_id
        )
        self.logger.info(f"Upserting {len(dict)} rows to Salesforce...")
        salesforce.upsert(dict=dict, table=table)
        self.logger.info(f"Successfully upserted to Salesforce.")
