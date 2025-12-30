"""Source for ingesting data from European Central Bank (ECB) exchange rates API."""

from typing import Any, Literal

from defusedxml import ElementTree as ElementDE
import pandas as pd

from viadot.exceptions import APIError
from viadot.sources.base import Source
from viadot.utils import (
    add_viadot_metadata_columns,
    handle_api_response,
    validate,
)


class ECB(Source):
    """ECB source class for fetching exchange rates from European Central Bank API."""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Initialize ECB source.

        Connector allows to pull exchange rates data from ECB API and convert it to a
        pandas DataFrame. No credentials are required.

        Usage:
            ecb = ECB()
            df = ecb.to_df(url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml")

        Example:
            ecb = ECB()
            df = ecb.to_df(url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml")
        """
        super().__init__(*args, credentials=None, **kwargs)

    def fetch_data(self, url: str) -> str:
        """Connect to ECB API and fetch XML response data.

        The function sends a GET request to the ECB API endpoint.
        If the request is successful, the response XML is returned as a string.
        If the request fails, an APIError is raised with the error message.

        Args:
            url (str): The URL of the ECB API endpoint.

        Returns:
            str: The API response data as XML string.

        Raises:
            APIError: If the request fails.
        """
        if not url:
            msg = "url is required and cannot be empty"
            self.logger.exception(msg)
            raise ValueError(msg)

        try:
            response = handle_api_response(
                url=url,
                params=None,
                method="GET",
            )
        except Exception as e:
            msg = "Failed to fetch data from ECB API"
            self.logger.exception(msg)
            raise APIError(msg) from e

        self.logger.info("Successfully fetched data from ECB API.")
        return response.text

    def _parse_xml(self, xml_string: str) -> pd.DataFrame:
        """Parse XML string and extract exchange rates data.

        Args:
            xml_string (str): The XML response string from ECB API.

        Returns:
            pd.DataFrame: DataFrame containing time, currency, and rate columns.

        Raises:
            ValueError: If the XML cannot be parsed or has unexpected structure.
        """
        try:
            root = ElementDE.fromstring(xml_string)
        except Exception as e:
            msg = "Failed to parse XML response from ECB API"
            self.logger.exception(msg)
            raise ValueError(msg) from e

        # Define namespaces - ECB uses default namespace for Cube elements
        # We need to register the namespace or use the full namespace URL
        namespace = "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"

        # Find all Cube elements with currency and rate attributes
        data = []
        cube_elements = root.findall(f".//{{{namespace}}}Cube[@currency]")

        # Get the time from the parent Cube element
        time_cube = root.find(f".//{{{namespace}}}Cube[@time]")
        if time_cube is None:
            msg = "Could not find time attribute in XML response"
            self.logger.error(msg)
            raise ValueError(msg)

        time_value = time_cube.get("time")

        # Extract currency and rate from each Cube element
        for cube in cube_elements:
            currency = cube.get("currency")
            rate = cube.get("rate")

            if currency and rate:
                data.append(
                    {
                        "time": time_value,
                        "currency": currency,
                        "rate": float(rate),
                    }
                )

        if not data:
            self.logger.warning("No exchange rate data found in XML response.")
            return pd.DataFrame(columns=["time", "currency", "rate"])

        df = pd.DataFrame(data)
        self.logger.info(f"Successfully parsed {len(df)} exchange rates from ECB data.")
        return df

    @add_viadot_metadata_columns
    def to_df(
        self,
        url: str,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        tests: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Convert ECB exchange rates data to pandas DataFrame.

        Args:
            url (str): The URL of the ECB API endpoint.
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if no data
                is available. Defaults to "warn".
            tests (dict[str, Any], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from utils. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing exchange rates data with columns:
                - time: The date of the exchange rates
                - currency: The currency code (e.g., USD, GBP)
                - rate: The exchange rate against EUR

        Raises:
            ValueError: If no data has been fetched or if validation fails.
        """
        if not url:
            msg = "url is required and cannot be empty"
            raise ValueError(msg)

        self.logger.info("Converting ECB data to pandas DataFrame.")

        # Fetch XML data
        xml_data = self.fetch_data(url)

        # Parse XML to DataFrame
        df = self._parse_xml(xml_data)

        if df.empty:
            self.logger.warning("No exchange rates found in the response.")
            self._handle_if_empty(if_empty=if_empty)

        if tests:
            self.logger.info("Running data validation tests.")
            validate(df=df, tests=tests)

        self.logger.info(
            f"Successfully processed {len(df)} exchange rates from ECB data."
        )

        return df
