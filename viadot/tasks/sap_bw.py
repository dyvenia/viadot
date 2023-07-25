import pandas as pd
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
            ).get(env)

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
        self.logger.info("Applying user defined mapping for columns...")
        df = df[mapping_dict.keys()]
        df.columns = mapping_dict.values()

        self.logger.info(f"Successfully applied user mapping.")

        return df

    def to_df(self, query_output: dict) -> pd.DataFrame:
        """
        Function to convert the SAP output in JSON format into a dataframe.

        Args:
            query_output (dict): SAP output dictionary in JSON format that contains data rows and column headers.

        Raises:
            ValidationError: Prints the original SAP error message in case of issues with MDX execution.

        Returns:
            pd.DataFrame: Output dataframe.
        """
        raw_data = {}

        if query_output["RETURN"]["MESSAGE"] == "":
            results = query_output["DATA"]
            for cell in results:
                if cell["ROW"] not in raw_data:
                    raw_data[cell["ROW"]] = {}
                if "].[" not in cell["DATA"]:
                    raw_data[cell["ROW"]][cell["COLUMN"]] = cell["DATA"]
            rows = [raw_data[row] for row in raw_data]
            cols = [x["DATA"] for x in query_output["HEADER"]]
            df = pd.DataFrame(data=rows)
            df.columns = cols
        else:
            df = pd.DataFrame()
            raise ValidationError(query_output["RETURN"]["MESSAGE"])

        return df

    def run(self, mdx_query: str, mapping_dict: dict = {}) -> pd.DataFrame:
        """
        Task run method.

        Args:
            mdx_query (str): MDX query to be passed to SAP BW.
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
