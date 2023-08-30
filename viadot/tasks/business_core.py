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
        if_empty: str = "skip",
        timeout=3600,
        *args,
        **kwargs,
    ):
        """Task for downloading  data from Business Core API to a Parquet file.

        Args:
            path (str, required): Path where to save a Parquet file.
            url (str, required): Base url to a view in Business Core API.
            filters_dict (Dict[str, Any], optional): Filters in form of dictionary. Available filters: 'BucketCount',
                'BucketNo', 'FromDate', 'ToDate'.  Defaults to {"BucketCount": None,"BucketNo": None,"FromDate": None,
                "ToDate": None,}.
            verify (bool, optional): Whether or not verify certificates while connecting to an API. Defaults to True.
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: username,
                password. Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored. Defaults to "BusinessCore".
            if_empty (str, optional): What to do if output DataFrame is empty. Defaults to "skip".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

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
        """Run method for BusinessCoreToParquet task. Saves data from Business Core API to Parquet file.

        Args:
            path (str, optional): Path where to save a Parquet file. Defaults to None.
            url (str, optional): Base url to a view in Business Core API. Defaults to None.
            filters_dict (Dict[str, Any], optional): Filters in form of dictionary. Available filters: 'BucketCount',
                'BucketNo', 'FromDate', 'ToDate'.  Defaults to None.
            verify (bool, optional): Whether or not verify certificates while connecting to an API. Defaults to True.
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: username,
                password. Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored. Defaults to None.
            if_empty (str, optional): What to do if output DataFrame is empty. Defaults to None.


        """
        bc = BusinessCore(
            url=url,
            credentials=credentials,
            config_key=config_key,
            filters_dict=filters_dict,
            verify=verify,
        )
        return bc.to_parquet(path=path, if_empty=if_empty)
