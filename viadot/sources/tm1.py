import pandas as pd

from typing import Any, Dict, Literal
from TM1py.Services import TM1Service
from prefect.utilities import logging


from ..config import local_config
from ..exceptions import CredentialError, ValidationError
from .base import Source

logger = logging.get_logger(__name__)


class TM1(Source):
    """
    Class for downloading data from TM1 Software using TM1py library
    """

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = "TM1",
        mdx_query: str = None,
        cube: str = None,
        view: str = None,
        dimension: str = None,
        hierarchy: str = None,
        limit: int = None,
        private: bool = False,
        verify: bool = False,
        *args,
        **kwargs,
    ):
        """
        Creating an instance of TM1 source class.

        Args:
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: username,
                password, address, port. Defaults to None.
            config_key (str, optional): Credential key to dictionary where credentials are stored. Defaults to "TM1".
            mdx_query (str, optional): MDX select query needed to download the data. Defaults to None.
            cube (str, optional): Cube name from which data will be downloaded. Defaults to None.
            view (str, optional): View name from which data will be downloaded. Defaults to None.
            dimension (str, optional): Diemension name. Defaults to None.
            hierarchy (str, optional): Hierarchy name. Defaults to None.
            limit (str, optional): How many rows should be extracted. If None all the avaiable rows will
                be downloaded. Defaults to None.
            private (bool, optional): Whether or not data download shoulb be private. Defaults to False.
            verify (bool, optional): Whether or not verify SSL certificates while. Defaults to False.


        Raises:
            CredentialError: When credentials are not found.
        """
        DEFAULT_CREDENTIALS = local_config.get(config_key)
        credentials = credentials or DEFAULT_CREDENTIALS

        required_credentials = ["address", "port", "username", "password"]
        if any([cred_key not in credentials for cred_key in required_credentials]):
            not_found = [c for c in required_credentials if c not in credentials]
            raise CredentialError(f"Missing credential(s): '{not_found}'.")

        self.config_key = config_key
        self.mdx_query = mdx_query
        self.cube = cube
        self.view = view
        self.dimension = dimension
        self.hierarchy = hierarchy
        self.limit = limit
        self.private = private
        self.verify = verify

        super().__init__(*args, credentials=credentials, **kwargs)

    def get_connection(self) -> TM1Service:
        """
        Start a connection to TM1 instance.

        Returns:
            TM1Service: Service instance if connection is succesfull.
        """
        return TM1Service(
            address=self.credentials["address"],
            port=self.credentials["port"],
            user=self.credentials["username"],
            password=self.credentials["password"],
            ssl=self.verify,
        )

    def get_cubes_names(self) -> list:
        """
        Get list of avaiable cubes in TM1 instance.

        Returns:
            list: List containing avaiable cubes names.

        """
        conn = self.get_connection()
        return conn.cubes.get_all_names()

    def get_views_names(self) -> list:
        """
        Get list of avaiable views in TM1 cube instance.

        Returns:
            list: List containing avaiable views names.

        """
        conn = self.get_connection()
        return conn.views.get_all_names(self.cube)

    def get_dimensions_names(self) -> list:
        """
        Get list of avaiable dimensions in TM1 instance.

        Returns:
            list: List containing avaiable dimensions names.

        """
        conn = self.get_connection()
        return conn.dimensions.get_all_names()

    def get_hierarchies_names(self) -> list:
        """
        Get list of avaiable hierarchies in TM1 dimension instance.

        Returns:
            list: List containing avaiable hierarchies names.

        """
        conn = self.get_connection()
        return conn.hierarchies.get_all_names(self.dimension)

    def get_available_elements(self) -> list:
        """
        Get list of avaiable elements in TM1 instance based on hierarchy and diemension.

        Returns:
            list: List containing avaiable elements names.

        """
        conn = self.get_connection()
        return conn.elements.get_element_names(
            dimension_name=self.dimension, hierarchy_name=self.hierarchy
        )

    def to_df(self, if_empty: Literal["warn", "fail", "skip"] = "skip") -> pd.DataFrame:
        """
        Function for downloading data from TM1 to pd.DataFrame.

        Args:
            if_empty (Literal["warn", "fail", "skip"], optional): What to do if output DataFrame is empty. Defaults to "skip".

        Returns:
            pd.DataFrame: DataFrame with data downloaded from TM1 view.

        Raises:
            ValidationError: When mdx and cube + view are not specified or when combination of both is specified.
        """
        conn = self.get_connection()

        if self.mdx_query is None and (self.cube is None or self.view is None):
            raise ValidationError("MDX query or cube and view are required.")
        elif self.mdx_query is not None and (self.cube is not None or self.view is not None):
            raise ValidationError("Specify only one: MDX query or cube and view.")
        elif self.cube is not None and self.view is not None:
            df = conn.cubes.cells.execute_view_dataframe(
                cube_name=self.cube,
                view_name=self.view,
                private=self.private,
                top=self.limit,
            )
        elif self.mdx_query is not None:
            df = conn.cubes.cells.execute_mdx_dataframe(self.mdx_query)

        logger.info(
            f"Data was successfully transformed into DataFrame: {len(df.columns)} columns and {len(df)} rows."
        )
        if df.empty is True:
            self._handle_if_empty(if_empty)
        return df
