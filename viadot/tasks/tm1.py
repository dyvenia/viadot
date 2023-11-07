import pandas as pd

from prefect import Task
from typing import Any, Dict
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import TM1

class TM1ToDF(Task):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = "TM1",
        mdx_query: str = None,
        cube: str = None,
        view: str = None,
        limit: int = None,
        private: bool = False,
        verify: bool = False,
        if_empty: str = "skip",
        timeout=3600,
        *args,
        **kwargs,
    ):
        """
        Task for downloading data from TM1 view to pandas DataFrame.

        Args:
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: username,
                password, address, port. Defaults to None.
            config_key (str, optional): Credential key to dictionary where credentials are stored. Defaults to "TM1".
            mdx_query (str, optional): MDX select query needed to download the data. Defaults to None.
            cube (str, optional): Cube name from which data will be downloaded. Defaults to None.
            view (str, optional): View name from which data will be downloaded. Defaults to None.
            limit (str, optional): How many rows should be extracted. If None all the avaiable rows will
                be downloaded. Defaults to None.
            private (bool, optional): Whether or not data download shoulb be private. Defaults to False.
            verify (bool, optional): Whether or not verify SSL certificates while. Defaults to False.
            if_empty (Literal["warn", "fail", "skip"], optional): What to do if output DataFrame is empty. Defaults to "skip".

        """
        self.credentials = credentials
        self.config_key = config_key
        self.mdx_query = mdx_query
        self.cube = cube
        self.view = view
        self.limit = limit
        self.private = private
        self.verify = verify
        self.if_empty = if_empty

        super().__init__(
            name="tm1_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Load TM1 data to pandas DataFrame"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "credentials",
        "config_key",
        "mdx_query",
        "cube",
        "view",
        "limit",
        "private",
        "verify",
        "if_empty",
    )
    def run(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        mdx_query: str = None,
        cube: str = None,
        view: str = None,
        limit: int = None,
        private: bool = None,
        verify: bool = None,
        if_empty: str = None,
    ) -> pd.DataFrame:
        """
        Run method for TM1ToDF class.

        Args:
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: username,
                password, address, port. Defaults to None.
            config_key (str, optional): Credential key to dictionary where credentials are stored. Defaults to None.
            mdx_query (str, optional): MDX select query needed to download the data. Defaults to None.
            cube (str, optional): Cube name from which data will be downloaded. Defaults to None.
            view (str, optional): View name from which data will be downloaded. Defaults to None.
            limit (str, optional): How many rows should be extracted. If None all the avaiable rows will
                be downloaded. Defaults to None.
            private (bool, optional): Whether or not data download shoulb be private. Defaults to None.
            verify (bool, optional): Whether or not verify SSL certificates while. Defaults to None.
            if_empty (Literal["warn", "fail", "skip"], optional): What to do if output DataFrame is empty. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame with data downloaded from TM1 view.

        """
        tm1 = TM1(
            credentials=credentials,
            config_key=config_key,
            mdx_query=mdx_query,
            cube=cube,
            view=view,
            limit=limit,
            private=private,
            verify=verify,
        )
        return tm1.to_df(if_empty=if_empty)
