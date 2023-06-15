from datetime import timedelta
from typing import List

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

try:
    from viadot.sources import SAPRFC, SAPRFCV2
except ImportError:
    raise


class SAPRFCToDF(Task):
    def __init__(
        self,
        query: str = None,
        sep: str = None,
        replacement: str = "-",
        func: str = None,
        rfc_total_col_width_character_limit: int = 400,
        credentials: dict = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        A task for querying SAP with SQL using the RFC protocol.

        Note that only a very limited subset of SQL is supported:
        - aliases
        - where clauses combined using the AND operator
        - limit & offset

        Unsupported:
        - aggregations
        - joins
        - subqueries
        - etc.

        Args:
            query (str, optional): The query to be executed with pyRFC.
            sep (str, optional): The separator to use when reading query results. If not provided,
            multiple options are automatically tried. Defaults to None.
            replacement (str, optional): In case of sep is on a columns, set up a new character to replace
                inside the string to avoid flow breakdowns. Defaults to "-".
            func (str, optional): SAP RFC function to use. Defaults to None.
            rfc_total_col_width_character_limit (int, optional): Number of characters by which query will be split in chunks
            in case of too many columns for RFC function. According to SAP documentation, the limit is
            512 characters. However, we observed SAP raising an exception even on a slightly lower number
            of characters, so we add a safety margin. Defaults to 400.
            credentials (dict, optional): The credentials to use to authenticate with SAP.
            By default, they're taken from the local viadot config.
        """
        self.query = query
        self.sep = sep
        self.replacement = replacement
        self.credentials = credentials
        self.func = func
        self.rfc_total_col_width_character_limit = rfc_total_col_width_character_limit

        super().__init__(
            name="sap_rfc_to_df",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "query",
        "sep",
        "replacement",
        "func",
        "rfc_total_col_width_character_limit",
        "credentials",
    )
    def run(
        self,
        query: str = None,
        sep: str = None,
        replacement: str = "-",
        credentials: dict = None,
        func: str = None,
        rfc_total_col_width_character_limit: int = None,
        rfc_unique_id: List[str] = None,
        alternative_version: bool = False,
    ) -> pd.DataFrame:
        """Task run method.

        Args:
            query (str, optional): The query to be executed with pyRFC.
            sep (str, optional): The separator to use when reading query results. If not provided,
            multiple options are automatically tried. Defaults to None.
            replacement (str, optional): In case of sep is on a columns, set up a new character to replace
                inside the string to avoid flow breakdowns. Defaults to "-".
            func (str, optional): SAP RFC function to use. Defaults to None.
            rfc_total_col_width_character_limit (int, optional): Number of characters by which query will be split in chunks
                in case of too many columns for RFC function. According to SAP documentation, the limit is
                512 characters. However, we observed SAP raising an exception even on a slightly lower number
                of characters, so we add a safety margin. Defaults to None.
            rfc_unique_id  (List[str], optional): Reference columns to merge chunks Data Frames. These columns must to be unique. If no columns are provided
                in this parameter, all data frame columns will by concatenated. Defaults to None.
                Example:
                --------
                SAPRFCToADLS(
                    ...
                    rfc_unique_id=["VBELN", "LPRIO"],
                    ...
                    )
            alternative_version (bool, optional): Enable the use version 2 in source. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame with SAP data.
        """
        if query is None:
            raise ValueError("Please provide the query.")

        if alternative_version is True:
            if rfc_unique_id:
                self.logger.warning(
                    "If the column/set of columns are not unique the table will be malformed."
                )
            sap = SAPRFCV2(
                sep=sep,
                replacement=replacement,
                credentials=credentials,
                func=func,
                rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
                rfc_unique_id=rfc_unique_id,
            )
        else:
            sap = SAPRFC(
                sep=sep,
                credentials=credentials,
                func=func,
                rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
            )
        sap.query(query)
        self.logger.info(f"Downloading data from SAP to a DataFrame...")
        self.logger.debug(f"Running query: \n{query}.")

        df = sap.to_df()

        self.logger.info(f"Data has been downloaded successfully.")
        return df
