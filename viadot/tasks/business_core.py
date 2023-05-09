import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional
from xml.etree.ElementTree import fromstring

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import BusinessCore


class BusinessCoreToParquet(Task):
    def __init__(
        self,
        path: str,
        url: str,
        filters_dict: Dict[str, Any] = {
            "BucketCount": None,
            "BucketNo": None,
            "FromDate": None,
            "ToDate": None,
        },
        verify: bool = True,
        credentials: Dict[str, Any] = None,
        config_key: str = "BusinessCore",
        if_empty: str = "warn",
        timeout=3600,
        *args,
        **kwargs,
    ) -> pd.DataFrame:

        self.url = url
        self.path = path
        self.credentials = credentials
        self.config_key = config_key
        self.filters_dict = filters_dict
        self.verify = verify
        self.if_empty = if_empty
        super().__init__(
            name="business_core_to_parquet",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Load Business Core data to Parquet"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "url", "path", "credentials", "config_key", "filters_dict", "verify", "if_empty"
    )
    def run(
        self,
        path: str = None,
        url: str = None,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        filters_dict: str = None,
        verify: bool = True,
        if_empty: str = None,
    ):
        bc = BusinessCore(
            url=url,
            credentials=credentials,
            config_key=config_key,
            filters_dict=filters_dict,
            verify=verify,
        )
        return bc.to_parquet(path=path, if_empty=if_empty)
