import textwrap
from typing import List

from pyrfc import Connection

from viadot.exceptions import CredentialError, ValidationError
from viadot.sources.base import Source


class SAPBW(Source):
    """
    A class for quering the SAP BW (SAP Business Warehouse) source using pyrfc library.
    Documentation to pyrfc can be found under: https://sap.github.io/PyRFC/pyrfc.html
    Documentation for SAP connection modules under: https://www.se80.co.uk/sap-function-modules/list/?index=rsr_mdx

    """

    def __init__(self, credentials: dict, *args, **kwargs):
        """
        Create an instance of the SAPBW class.

        Args:
            credentials (dict): Credentials to connect with SAP BW containing ashost, sysnr, user, passwd, client.

        Raises:
            CredentialError: If provided credentials are incorrect.
        """
        if credentials is None:
            raise CredentialError("Missing credentials.")

        super().__init__(*args, credentials=credentials, **kwargs)

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

    def get_all_available_columns(self, mdx_query: str) -> List:
        """
        Function to generate list of all available columns in the SAP table based on passed MDX query.

        Args:
            mdx_query (str): The MDX query to be passed to connection.

        Returns:
            all_available_columns: List of all available columns in the source table.
        """

        conn = self.get_connection()
        query = textwrap.wrap(
            mdx_query, width=75
        )  # width = 75, to properly split mdx query into substrings passed to SAP object creation function

        properties = conn.call("RSR_MDX_CREATE_STORED_OBJECT", COMMAND_TEXT=query)
        datasetid = properties["DATASETID"]

        if properties["RETURN"]["MESSAGE"] == "":
            get_axis_info = conn.call(
                "RSR_MDX_GET_AXIS_INFO", DATASETID=datasetid
            )  # listing all of available columns and metrics
            cols = get_axis_info["AXIS_DIMENSIONS"]

            all_available_columns = [x["DIM_UNAM"] for x in cols]
        else:
            all_available_columns = []
            self.logger.error(properties["RETURN"]["MESSAGE"])

        return all_available_columns

    def get_output_data(self, mdx_query: str) -> dict:
        """
        Function to generate the SAP output dataset from MDX query.

        Args:
            mdx_query (str): The MDX query to be passed to connection.

        Returns:
            query_output: SAP output dictionary in JSON format that contains data rows and column headers.

            Example output:

                {
                "RETURN": {
                    "TYPE": "",
                    "ID": "",
                    "NUMBER": "000",
                    "MESSAGE": "",...
                },...
                "DATA": [
                    {
                    "COLUMN": 0,
                    "ROW": 0,
                    "DATA": "VELUX Deutschland GmbH",
                    "VALUE_DATA_TYPE": "CHAR",
                    "CELL_STATUS": ""
                    },...
                ],
                "HEADER": [
                    {
                    "COLUMN": 0,
                    "ROW": 0,
                    "DATA": "[0COMP_CODE].[LEVEL01].[DESCRIPTION]",
                    "VALUE_DATA_TYPE": "CHAR",
                    "CELL_STATUS": ""
                    },...
                ]
                }

        """
        conn = self.get_connection()
        query = textwrap.wrap(
            mdx_query, 75
        )  # width = 75, to properly split mdx query into substrings passed to SAP object creation function
        properties = conn.call("RSR_MDX_CREATE_OBJECT", COMMAND_TEXT=query)

        datasetid = properties["DATASETID"]
        query_output = conn.call("RSR_MDX_GET_FLAT_DATA", DATASETID=datasetid)

        return query_output
