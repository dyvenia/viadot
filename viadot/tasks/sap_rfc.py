from datetime import timedelta
from typing import List

import pandas as pd
import json
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from viadot.tasks import AzureKeyVaultSecret

try:
    from viadot.sources import SAPRFC, SAPRFCV2
except ImportError:
    raise
from prefect.utilities import logging

logger = logging.get_logger()


class SAPRFCToDF(Task):
    def __init__(
        self,
        query: str = None,
        sep: str = None,
        replacement: str = "-",
        func: str = None,
        rfc_total_col_width_character_limit: int = 400,
        sap_credentials: dict = None,
        sap_credentials_key: str = "SAP",
        env: str = "DEV",
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
            sap_credentials (dict, optional): The credentials to use to authenticate with SAP. By default, they're taken from the local viadot config.
            sap_credentials_key (str, optional): The key for sap credentials located in the local config or Azure Key Vault. Defaults to "SAP".
            env (str, optional): The key for sap_credentials_key pointing to the SAP environment. Defaults to "DEV".
            By default, they're taken from the local viadot config.
        """
        self.query = query
        self.sep = sep
        self.replacement = replacement
        self.sap_credentials = sap_credentials
        self.sap_credentials_key = sap_credentials_key
        self.env = env
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
        "sap_credentials",
    )
    def run(
        self,
        query: str,
        sep: str = None,
        replacement: str = "-",
        sap_credentials: dict = None,
        sap_credentials_key: str = "SAP",
        env: str = "DEV",
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
            sap_credentials (dict, optional): The credentials to use to authenticate with SAP. Defaults to None.
            sap_credentials_key (str, optional): The key for sap credentials located in the local config or Azure Key Vault. Defaults to "SAP".
            env (str, optional): The key for sap_credentials_key pointing to the SAP environment. Defaults to "DEV".
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

        if sap_credentials is None:
            try:
                credentials_str = AzureKeyVaultSecret(
                    secret=sap_credentials_key,
                ).run()
                sap_credentials = json.loads(credentials_str).get(env)
            except:
                logger.warning(
                    f"Getting credentials from Azure Key Vault was not possible. Either there is no key: {sap_credentials_key} or env: {env} or there is not Key Vault in your environment."
                )

        if alternative_version is True:
            if rfc_unique_id:
                self.logger.warning(
                    "If the column/set of columns are not unique the table will be malformed."
                )
            sap = SAPRFCV2(
                sep=sep,
                replacement=replacement,
                sap_credentials=sap_credentials,
                sap_credentials_key=sap_credentials_key,
                env=env,
                func=func,
                rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
                rfc_unique_id=rfc_unique_id,
            )
        else:
            sap = SAPRFC(
                sep=sep,
                sap_credentials=sap_credentials,
                sap_credentials_key=sap_credentials_key,
                env=env,
                func=func,
                rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
            )
        sap.query(query)
        self.logger.info(f"Downloading data from SAP to a DataFrame...")
        self.logger.debug(f"Running query: \n{query}.")

        df = sap.to_df()

        self.logger.info(f"Data has been downloaded successfully.")
        return df
