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
        func: str = None,
        credentials: dict = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
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
            func (str, optional): SAP RFC function to use. Defaults to None.
            credentials (dict, optional): The credentials to use to authenticate with SAP.
            By default, they're taken from the local viadot config.
        """
        self.query = query
        self.sep = sep
        self.credentials = credentials
        self.func = func

        super().__init__(
            name="sap_rfc_to_df",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "query",
        "sep",
        "func",
        "credentials",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        query: str = None,
        sep: str = None,
        credentials: dict = None,
        func: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> pd.DataFrame:
        """Task run method.

        Args:
            query (str, optional): The query to be executed with pyRFC.
            sep (str, optional): The separator to use when reading query results. If not provided,
            multiple options are automatically tried. Defaults to None.
            func (str, optional): SAP RFC function to use. Defaults to None.
        """
        if query is None:
            raise ValueError("Please provide the query.")
        sap = SAPRFC(sep=sep, credentials=credentials, func=func)
        sap.query(query)
        self.logger.info(f"Downloading data from SAP to a DataFrame...")
        self.logger.debug(f"Running query: \n{query}.")

        df = sap.to_df()

        self.logger.info(f"Data has been downloaded successfully.")
        return df
