import copy
import json
import os
from datetime import timedelta
from typing import List, Any, Literal, Dict

import pandas as pd
import prefect
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from ..exceptions import ValidationError
from ..sources import VeluxClub
from .azure_key_vault import AzureKeyVaultSecret

logger = logging.get_logger()


class VeluxClubToDF(Task):
    """_summary_

    Args:
        Task (_type_): _description_
    """

    def __init__(
        self,
        source: Literal["jobs", "product", "company", "survey"],
        credentials: Dict[str, Any] = None,
        from_date: str = "2022-03-22",
        to_date: str = "",
        if_empty: str = "warn",
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 3600,
        report_name: str = "velux_club_to_df",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """_summary_

        Args:
            source (Literal["jobs", "product", "company", "survey"]): _description_
            credentials (Dict[str, Any], optional): _description_. Defaults to None.
            from_date (str, optional): _description_. Defaults to '2022-03-22'.
            to_date (str, optional): _description_. Defaults to ''.
            if_empty (str, optional): _description_. Defaults to "warn".
            retry_delay (timedelta, optional): _description_. Defaults to timedelta(seconds=10).
            timeout (int, optional): _description_. Defaults to 3600.
            report_name (str, optional): _description_. Defaults to 'velux_club_to_df'.
        """

        self.logger = prefect.context.get("logger")
        self.source = source
        self.credentials = credentials
        self.from_date = from_date
        self.to_date = to_date
        self.if_empty = if_empty
        self.retry_delay = retry_delay
        self.report_name = report_name

        super().__init__(
            name=self.report_name,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)

    def run(self):
        vc_obj = VeluxClub(
            source=self.source, from_date=self.from_date, to_date=self.to_date
        )

        r = vc_obj.get_api_body()

        vc_df = vc_obj.to_df(r)

        return vc_df
