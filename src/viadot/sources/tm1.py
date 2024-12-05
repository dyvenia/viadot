"""Source for connecting to TM1."""

from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, SecretStr
from TM1py.Services import TM1Service

from viadot.config import get_source_credentials
from viadot.exceptions import ValidationError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class TM1Credentials(BaseModel):
    """TM1 credentials.

    Uses simple authentication:
        - username: The user name to use for the connection.
        - password: The password to use for the connection.
        - address: The ip address to use for the connection.
        - port: The port to use for the connection.
    """

    username: str
    password: SecretStr
    address: str
    port: str


class TM1(Source):
    """TM1 connector."""

    def __init__(
        self,
        credentials: dict[str, Any] | None = None,
        config_key: str = "TM1",
        limit: int | None = None,
        private: bool = False,
        verify: bool = False,
        *args,
        **kwargs,
    ):
        """Create a TM1 connector instance.

        Args:
            credentials (dict[str, Any], optional): Credentials stored in a dictionary.
                Required credentials: username, password, address, port.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "TM1".
            mdx_query (str, optional): MDX select query needed to download the data.
                Defaults to None.
            cube (str, optional): Cube name from which data will be downloaded.
                Defaults to None.
            view (str, optional): View name from which data will be downloaded.
                Defaults to None.
            dimension (str, optional): Dimension name. Defaults to None.
            hierarchy (str, optional): Hierarchy name. Defaults to None.
            limit (str, optional): How many rows should be extracted.
                If None all the available rows will be downloaded. Defaults to None.
            private (bool, optional): Whether or not data download should be private.
                Defaults to False.
            verify (bool, optional): Whether or not verify SSL certificates.
                Defaults to False.

        Raises:
            CredentialError: When credentials are not found.

        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(TM1Credentials(**raw_creds))

        self.config_key = config_key
        self.limit = limit
        self.private = private
        self.verify = verify

        super().__init__(*args, credentials=validated_creds, **kwargs)

    def get_connection(self) -> TM1Service:
        """Start a connection to TM1 instance.

        Returns:
            TM1Service: Service instance if connection is successful.
        """
        return TM1Service(
            address=self.credentials.get("address"),
            port=self.credentials.get("port"),
            user=self.credentials.get("username"),
            password=self.credentials.get("password").get_secret_value(),
            ssl=self.verify,
        )

    def get_cubes_names(self) -> list:
        """Get list of available cubes in TM1 instance.

        Returns:
            list: List containing available cubes names.

        """
        conn = self.get_connection()
        return conn.cubes.get_all_names()

    def get_views_names(self, cube: str) -> list:
        """Get list of available views in TM1 cube instance.

        Args:
            cube (str): Cube name.

        Returns:
            list: List containing available views names.

        """
        conn = self.get_connection()
        return conn.views.get_all_names(cube)

    def get_dimensions_names(self) -> list:
        """Get list of available dimensions in TM1 instance.

        Returns:
            list: List containing available dimensions names.

        """
        conn = self.get_connection()
        return conn.dimensions.get_all_names()

    def get_hierarchies_names(self, dimension: str) -> list:
        """Get list of available hierarchies in TM1 dimension instance.

        Args:
            dimension (str): Dimension name.

        Returns:
            list: List containing available hierarchies names.

        """
        conn = self.get_connection()
        return conn.hierarchies.get_all_names(dimension)

    def get_available_elements(self, dimension: str, hierarchy: str) -> list:
        """Get list of available elements in TM1 instance.

        Elements are extracted based on hierarchy and dimension.

        Args:
            dimension (str): Dimension name.
            hierarchy (str): Hierarchy name.

        Returns:
            list: List containing available elements names.

        """
        if dimension is None or hierarchy is None:
            msg = "Missing dimension or hierarchy."
            raise ValidationError(msg)

        conn = self.get_connection()
        return conn.elements.get_element_names(
            dimension_name=dimension, hierarchy_name=hierarchy
        )

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: Literal["warn", "fail", "skip"] = "skip",
        mdx_query: str | None = None,
        cube: str | None = None,
        view: str | None = None,
    ) -> pd.DataFrame:
        """Download data into a pandas DataFrame.

            To download the data to the dataframe user needs to specify MDX query
                or combination of cube and view.

        Args:
            if_empty (Literal["warn", "fail", "skip"], optional): What to do if output
                DataFrame is empty. Defaults to "skip".
            mdx_query (str): MDX query. Defaults to None.
            cube (str): Cube name. Defaults to None.
            view (str): View name. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame with the data.

        Raises:
            ValidationError: When mdx and cube + view are not specified
                or when combination of both is specified.
        """
        conn = self.get_connection()

        if mdx_query is None and (cube is None or view is None):
            error_msg = "MDX query or cube and view are required."
            raise ValidationError(error_msg)
        if mdx_query is not None and (cube is not None or view is not None):
            error_msg = "Specify only one: MDX query or cube and view."
            raise ValidationError(error_msg)
        if cube is not None and view is not None:
            df = conn.cubes.cells.execute_view_dataframe(
                cube_name=cube,
                view_name=view,
                private=self.private,
                top=self.limit,
            )
        elif mdx_query is not None:
            df = conn.cubes.cells.execute_mdx_dataframe(mdx_query)

        self.logger.info(
            f"Data was successfully transformed into DataFrame: {len(df.columns)} columns and {len(df)} rows."
        )
        if df.empty:
            self._handle_if_empty(if_empty)
        return df
