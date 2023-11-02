from prefect import Task
from typing import Any, Dict
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import TM1


class TM1ToDF(Task):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = "TM1",
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
        self.credentials = credentials
        self.config_key = config_key
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
        cube: str = None,
        view: str = None,
        limit: int = None,
        private: bool = None,
        verify: bool = None,
        if_empty: str = None,
    ):
        tm1 = TM1(
            credentials=credentials,
            config_key=config_key,
            cube=cube,
            view=view,
            limit=limit,
            private=private,
            verify=verify,
        )
        return tm1.to_df(if_empty=if_empty)
