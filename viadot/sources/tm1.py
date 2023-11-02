import pandas as pd

from typing import Any, Dict, Literal
from TM1py.Services import TM1Service
from prefect.utilities import logging


from ..config import local_config
from ..exceptions import CredentialError
from .base import Source

logger = logging.get_logger(__name__)


class TM1(Source):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = "TM1",
        cube: str = None,
        view: str = None,
        limit: int = None,
        private: bool = False,
        verify: bool = False,
        *args,
        **kwargs,
    ):
        DEFAULT_CREDENTIALS = local_config.get(config_key)
        credentials = credentials or DEFAULT_CREDENTIALS

        required_credentials = ["address", "port", "username", "password"]
        if any([cred_key not in credentials for cred_key in required_credentials]):
            not_found = [c for c in required_credentials if c not in credentials]
            raise CredentialError(f"Missing credential(s): '{not_found}'.")

        self.config_key = config_key
        self.cube = cube
        self.view = view
        self.limit = limit
        self.private = private
        self.verify = verify

        super().__init__(*args, credentials=credentials, **kwargs)

    def get_connection(self) -> TM1Service:
        return TM1Service(
            address=self.credentials["address"],
            port=self.credentials["port"],
            user=self.credentials["username"],
            password=self.credentials["password"],
            ssl=self.verify,
        )

    def get_cubes_names(self) -> list:
        conn = self.get_connection
        return conn.cubes.get_all_names()

    def get_views_names(self) -> list:
        conn = self.get_connection
        return conn.views.get_all_names(self.cube)

    def to_df(self, if_empty: Literal["warn", "fail", "skip"] = "skip") -> pd.DataFrame:
        conn = self.get_connection()
        df = conn.cubes.cells.execute_view_dataframe(
            cube_name=self.cube,
            view_name=self.view,
            private=self.private,
            top=self.limit,
        )
        logger.info(
            f"Data was successfully transformed into DataFrame: {len(df.columns)} columns and {len(df)} rows."
        )
        if df.empty is True:
            self._handle_if_empty(if_empty)
        return df
