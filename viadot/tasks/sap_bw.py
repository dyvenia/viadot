import pandas as pd


from typing import List, Dict, Any, Literal
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from viadot.exceptions import ValidationError
from viadot.sources import SAPBW
from viadot.task_utils import *

logger = logging.get_logger()


class SAPBWToDF(Task):
    def __init__(
        self,
        sapbw_credentials: dict = None,
        sapbw_credentials_key: str = "SAP",
        env: str = "BW",
        *args,
        **kwargs,
    ):
        """
        A task for quering the SAP BW (SAP Business Warehouse) source using pyrfc library.

        Args:
            sapbw_credentials (dict, optional): Credentials to SAP BW server. Defaults to None.
            sapbw_credentials_key (str, optional): Azure KV secret. Defaults to "SAP".
            env (str, optional): SAP environment. Defaults to "BW".
        """
        if sapbw_credentials is None:
            self.sapbw_credentials = credentials_loader.run(
                credentials_secret=sapbw_credentials_key
            )
            self.sapbw_credentials = self.sapbw_credentials[env]

        else:
            self.sapbw_credentials = sapbw_credentials

        super().__init__(
            name="sapbw_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download SAP BW data to a DF"""
        super().__call__(self)

    def get_columns(self, json_data: List = []) -> List:
        """
        Function to generate the column list to be passed as DataFrame headers.

        Args:
            json_data (List, optional): Input DataFrame and column list as list. Defaults to [].

        Returns:
            List: Columns list.
        """
        columns = json_data[1]
        df_cols = [x["DATA"] for x in columns["HEADER"]]
        return df_cols

    def apply_user_mapping(
        self, df: pd.DataFrame, mapping_dict: dict = {}
    ) -> pd.DataFrame:
        """
        Function to apply the column mapping defined by user for the output dataframe.
        DataFrame will be cut to selected columns - if any other columns need to be included in the output file,
        please add them to the mapping dictionary with original names.

        Args:
            df (pd.DataFrame): Input dataframe for the column mapping task.
            mapping_dict (dict, optional): Dictionary with original and new column names. Defaults to {}.

        Returns:
            pd.DataFrame: Output DataFrame with mapped columns.
        """
        self.logger.info("Applying user defined mapping for columns. Starting task...")
        df = df[mapping_dict.keys()]
        df.columns = mapping_dict.values()

        self.logger.info(f"Successfully applied user mapping.")

        return df

    def to_df(self, json_data: dict = {}) -> pd.DataFrame:
        """
        Function to convert the SAP output into a dataframe.

        Args:
            json_data (dict, optional): Output from teh SAP in json format. Defaults to {}.

        Returns:
            pd.DataFrame: Output dataframe.
        """
        rows = {}
        if "DATA" in json_data[0]:
            if len(json_data[0]["DATA"]) > 0:
                results = json_data[0]["DATA"]
                for cell in results:
                    if cell["ROW"] not in rows:
                        rows[cell["ROW"]] = {}
                    if "].[" not in cell["DATA"]:
                        rows[cell["ROW"]][cell["COLUMN"]] = cell["DATA"]

            else:
                issue_value = str(json_data[0]["RETURN"]["MESSAGE"])
                self.logger.warning(f"---------- {issue_value} --------")
        else:
            self.logger.warning("No data produced by the query.")

        rows = [rows[row] for row in rows]
        df = pd.DataFrame(rows)
        df.columns = self.get_columns(json_data)
        return df

    def run(self, mdx_query: str = None, mapping_dict: dict = {}) -> pd.DataFrame:
        """
        Task run method.

        Args:
            mdx_query (str, optional): MDX query to be passed to SAP BW. Defaults to None.
            mapping_dict (dict, optional): Mapping dictionary from user in json format. Defaults to {}.

        Returns:
            pd.DataFrame: Output DataFrame with applied column mapping.
        """
        sap = SAPBW(credentials=self.sapbw_credentials)

        data = sap.get_output_data(mdx_query)
        df = self.to_df(data)

        if mapping_dict:
            df = self.apply_user_mapping(df, mapping_dict)

        return df
