"""SAP BW  connector."""

import textwrap
from typing import Any

import pandas as pd
from pydantic import BaseModel
import pyrfc

from viadot.config import get_source_credentials
from viadot.exceptions import ValidationError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class SAPBWCredentials(BaseModel):
    """Checking for values in SAP BW credentials dictionary.

    Two key values are held in the SAP BW connector:
        - ashost: Indicates the host name or IP address of a specific SAP
            application server.
        - client: Specifies the SAP logon parameter client.
        - passwd: Indicates the SAP logon parameter password.
        - sysnr: Indicates the SAP system number—the 2-byte code that identifies the
            system on the host.
        - user: Indicates the SAP logon parameter user.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    ashost: str
    client: str
    passwd: str
    sysnr: str
    user: str


class SAPBW(Source):
    """Quering the SAP BW (SAP Business Warehouse) source using pyrfc library.

    Documentation to pyrfc can be found under:
        https://sap.github.io/PyRFC/pyrfc.html
    Documentation for SAP connection modules under:
        https://www.se80.co.uk/sap-function-modules/list/?index=rsr_mdx
    """

    def __init__(
        self,
        *args,
        credentials: SAPBWCredentials | None = None,
        config_key: str = "sap_bw",
        **kwargs,
    ):
        """Create an instance of SAP BW.

        Args:
            credentials (Optional[SAPBWCredentials], optional): SAP BW credentials.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "sap_bw".

        Examples:
            sap_bw = SAPBW(
                credentials=credentials,
                config_key=config_key,
            )
            sap_bw.api_connection(
                ...
            )
            data_frame = sap_bw.to_df()

        Raises:
            CredentialError: If credentials are not provided in local_config or
                directly as a parameter.
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(SAPBWCredentials(**raw_creds))

        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.query_output = None

    def _create_connection(self):
        """Create the connection with SAP BW.

        Returns:
            Connection: Connection to SAP.
        """
        return pyrfc.Connection(
            ashost=self.credentials.get("ashost"),
            sysnr=self.credentials.get("sysnr"),
            user=self.credentials.get("user"),
            passwd=self.credentials.get("passwd"),
            client=self.credentials.get("client"),
        )

    def api_connection(self, mdx_query: str) -> None:
        """Generate the SAP BW output dataset from MDX query.

        Args:
            mdx_query (str): The MDX query to be passed to connection.
        """
        conn = self._create_connection()

        query = textwrap.wrap(mdx_query, 75)
        properties = conn.call("RSR_MDX_CREATE_OBJECT", COMMAND_TEXT=query)

        datasetid = properties["DATASETID"]
        self.query_output = conn.call("RSR_MDX_GET_FLAT_DATA", DATASETID=datasetid)
        conn.close()

    def _apply_user_mapping(
        self,
        df: pd.DataFrame,
        mapping_dict: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Apply the column mapping defined by user for the output dataframe.

            DataFrame will be cut to selected columns - if any other columns need to be
        included in the output file, please add them to the mapping dictionary with
        original names.

        Args:
            df (pd.DataFrame): Input dataframe for the column mapping task.
            mapping_dict (dict[str, Any], optional): Dictionary with original and new
                column names. Defaults to None.

        Returns:
            pd.DataFrame: Output DataFrame with mapped columns.
        """
        self.logger.info("Applying user defined mapping for columns...")
        df = df[mapping_dict.keys()]
        df.columns = mapping_dict.values()

        self.logger.info("Successfully applied user mapping.")

        return df

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
        mapping_dict: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Convert the SAP BW output JSON data into a dataframe.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn".
            mapping_dict (dict[str, Any], optional): Dictionary with original and new
                column names. Defaults to None.

        Raises:
            ValidationError: Prints the original SAP error message in case of issues
                with MDX execution.

        Returns:
            pd.Dataframe: The response data as a pandas DataFrame, enriched
                with viadot metadata columns.
        """
        raw_data = {}

        if self.query_output["RETURN"]["MESSAGE"] == "":
            results = self.query_output["DATA"]
            for cell in results:
                if cell["ROW"] not in raw_data:
                    raw_data[cell["ROW"]] = {}
                if "].[" not in cell["DATA"]:
                    raw_data[cell["ROW"]][cell["COLUMN"]] = cell["DATA"]
            rows = [raw_data[row] for row in raw_data]
            cols = [x["DATA"] for x in self.query_output["HEADER"]]

            data_frame = pd.DataFrame(data=rows)
            data_frame.columns = cols

        else:
            data_frame = pd.DataFrame()
            raise ValidationError(self.query_output["RETURN"]["MESSAGE"])

        if mapping_dict:
            data_frame = self._apply_user_mapping(data_frame, mapping_dict)

        if data_frame.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info("Successfully downloaded data from the Mindful API.")

        return data_frame

    def get_available_columns(self, mdx_query: str) -> list[str]:
        """Generate list of all available columns in a SAP BW table.

        Args:
            mdx_query (str): The MDX query to be passed to connection.

        Returns:
            list[str]: List of all available columns in the source table.
        """
        conn = self._create_connection()
        query = textwrap.wrap(mdx_query, width=75)

        properties = conn.call("RSR_MDX_CREATE_STORED_OBJECT", COMMAND_TEXT=query)
        datasetid = properties["DATASETID"]

        if properties["RETURN"]["MESSAGE"] == "":
            get_axis_info = conn.call("RSR_MDX_GET_AXIS_INFO", DATASETID=datasetid)
            cols = get_axis_info["AXIS_DIMENSIONS"]

            all_available_columns = [x["DIM_UNAM"] for x in cols]
        else:
            all_available_columns = []
            self.logger.error(properties["RETURN"]["MESSAGE"])

        return all_available_columns
