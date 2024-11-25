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
        mdx_query: str | None = None,
        cube: str | None = None,
        view: str | None = None,
        dimension: str | None = None,
        hierarchy: str | None = None,
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
        self.validated_creds = dict(TM1Credentials(**raw_creds))

        self.config_key = config_key
        self.mdx_query = mdx_query
        self.cube = cube
        self.view = view
        self.dimension = dimension
        self.hierarchy = hierarchy
        self.limit = limit
        self.private = private
        self.verify = verify

        super().__init__(*args, credentials=self.validated_creds, **kwargs)

    def get_connection(self) -> TM1Service:
        """Start a connection to TM1 instance.

        Returns:
            TM1Service: Service instance if connection is successful.
        """
        return TM1Service(
            address=self.validated_creds.get("address"),
            port=self.validated_creds.get("port"),
            user=self.validated_creds.get("username"),
            password=self.validated_creds.get("password").get_secret_value(),
            ssl=self.verify,
        )

    def get_cubes_names(self) -> list:
        """Get list of available cubes in TM1 instance.

        Returns:
            list: List containing available cubes names.

        """
        conn = self.get_connection()
        return conn.cubes.get_all_names()

    def get_views_names(self) -> list:
        """Get list of available views in TM1 cube instance.

        Returns:
            list: List containing available views names.

        """
        if self.cube is None:
            msg = "Missing cube name."
            raise ValidationError(msg)
        conn = self.get_connection()
        return conn.views.get_all_names(self.cube)

    def get_dimensions_names(self) -> list:
        """Get list of available dimensions in TM1 instance.

        Returns:
            list: List containing available dimensions names.

        """
        conn = self.get_connection()
        return conn.dimensions.get_all_names()

    def get_hierarchies_names(self) -> list:
        """Get list of available hierarchies in TM1 dimension instance.

        Returns:
            list: List containing available hierarchies names.

        """
        if self.dimension is None:
            msg = "Missing dimension name."
            raise ValidationError(msg)

        conn = self.get_connection()
        return conn.hierarchies.get_all_names(self.dimension)

    def get_available_elements(self) -> list:
        """Get list of available elements in TM1 instance.

        Elements are extracted based on hierarchy and dimension.

        Returns:
            list: List containing available elements names.

        """
        if (self.dimension or self.hierarchy) is None:
            msg = "Missing dimension or hierarchy."
            raise ValidationError(msg)

        conn = self.get_connection()
        return conn.elements.get_element_names(
            dimension_name=self.dimension, hierarchy_name=self.hierarchy
        )

    @add_viadot_metadata_columns
    def to_df(self, if_empty: Literal["warn", "fail", "skip"] = "skip") -> pd.DataFrame:
        """Download data into a pandas DataFrame.

            To download the data to the dataframe user needs to specify MDX query
            or combination of cube and view.

        Args:
            if_empty (Literal["warn", "fail", "skip"], optional): What to do if output
            DataFrame is empty. Defaults to "skip".

        Returns:
            pd.DataFrame: DataFrame with the data.

        Raises:
            ValidationError: When mdx and cube + view are not specified
            or when combination of both is specified.
        """
        conn = self.get_connection()

        if self.mdx_query is None and (self.cube is None or self.view is None):
            error_msg = "MDX query or cube and view are required."
            raise ValidationError(error_msg)
        if self.mdx_query is not None and (
            self.cube is not None or self.view is not None
        ):
            error_msg="Specify only one: MDX query or cube and view."
            raise ValidationError(error_msg)
        if self.cube is not None and self.view is not None:
            df = conn.cubes.cells.execute_view_dataframe(
                cube_name=self.cube,
                view_name=self.view,
                private=self.private,
                top=self.limit,
            )
        elif self.mdx_query is not None:
            df = conn.cubes.cells.execute_mdx_dataframe(self.mdx_query)

        self.logger.info(
            f"Data was successfully transformed into DataFrame: {len(df.columns)} columns and {len(df)} rows."
        )
        if df.empty is True:
            self._handle_if_empty(if_empty)
        return df
