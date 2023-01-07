from datetime import timedelta

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

try:
    from ..sources import SAPRFC
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
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        query: str = None,
        sep: str = None,
        replacement: str = "-",
        credentials: dict = None,
        func: str = None,
        rfc_total_col_width_character_limit: int = None,
        rfc_reference_column: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
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
        """
        if query is None:
            raise ValueError("Please provide the query.")
        if rfc_reference_column:
            self.logger.warning(
                "Reference column added. Remember, this column MUST BE UNIQUE or the table will be malformed."
            )
        sap = SAPRFC(
            sep=sep,
            replacement=replacement,
            credentials=credentials,
            func=func,
            rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
            rfc_reference_column=rfc_reference_column,
        )
        sap.query(query)
        self.logger.info(f"Downloading data from SAP to a DataFrame...")
        self.logger.debug(f"Running query: \n{query}.")

        df = sap.to_df()

        self.logger.info(f"Data has been downloaded successfully.")
        return df
