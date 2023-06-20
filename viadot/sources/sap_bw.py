from typing import List
from viadot.sources.base import Source
from viadot.exceptions import CredentialError
from pyrfc import Connection
import textwrap


class SAPBW(Source):
    """
    A class for quering the SAP BW (SAP Business Warehouse) source using pyrfc library.
    Documentation to pyrfc can be found under: https://sap.github.io/PyRFC/pyrfc.html
    Documentation for SAP connection modules under: https://www.se80.co.uk/sap-function-modules/list/?index=rsr_mdx

    """

    def __init__(self, credentials: dict, *args, **kwargs):
        self.credentials = credentials
        if credentials is None:
            raise CredentialError("Missing credentials.")

        super().__init__(*args, credentials=self.credentials, **kwargs)

    def get_connection(self) -> Connection:
        """
        Function to create the connection with SAP BW.

        Returns:
            Connection: Connection to SAP.
        """
        return Connection(
            ashost=self.credentials.get("ashost"),
            sysnr=self.credentials.get("sysnr"),
            user=self.credentials.get("user"),
            passwd=self.credentials.get("passwd"),
            client=self.credentials.get("client"),
        )

    def get_all_available_columns(self, mdx_query: str = None) -> List:
        """
        Function to generate list of all available columns in the SAP table based on passed MDX query.

        Args:
            mdx_query (str, optional): The MDX query to be passed to connection. Defaults to [].

        Returns:
            List: List of all available columns in the source table.
        """
        conn = self.get_connection()
        query = textwrap.wrap(mdx_query, 75)
        properties = conn.call("RSR_MDX_CREATE_OBJECT", COMMAND_TEXT=query)
        datasetid = properties["DATASETID"]

        get_axis_info = conn.call(
            "RSR_MDX_GET_AXIS_INFO", DATASETID=datasetid
        )  # listing all of available columns and metrics
        cols = get_axis_info["AXIS_DIMENSIONS"]

        all_available_columns = [x["DIM_UNAM"] for x in cols]
        return all_available_columns

    def get_output_data(self, mdx_query: str = None) -> List:
        """
        Function to generate the SAP output dataset from MDX query.

        Args:
            mdx_query (str, optional): The MDX query to be passed to connection. Defaults to [].

        Returns:
            List: (query_output, columns): Set of query output in dictionary format and list of output columns to be passed to final dataframe.
        """
        conn = self.get_connection()
        query = textwrap.wrap(mdx_query, 75)
        properties = conn.call("RSR_MDX_CREATE_OBJECT", COMMAND_TEXT=query)
        datasetid = properties["DATASETID"]

        query_output = conn.call("RSR_MDX_GET_FLAT_DATA", DATASETID=datasetid)

        columns = conn.call("RSR_MDX_BXML_GET_INFO", DATASETID=datasetid)
        return [query_output, columns]
