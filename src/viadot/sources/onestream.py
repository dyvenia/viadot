"""OneStream API connector for data extraction and management.

This module provides the OneStream class for interacting with OneStream API endpoints,
including Data Adapter queries, SQL queries, and Data Management sequence execution.
"""

from itertools import product
import json
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class OneStreamCredentials(BaseModel):
    """Check OneStream credentials dict.

    OneStream API Key token is required:
        - api_token: The unique authentication api token for the OneStream API.

    Args:
         BaseModel (pydantic.BaseModel): Base class for data validation.
    """

    api_token: str


class OneStream(Source):
    def __init__(
        self,
        base_url: str,
        application: str,
        config_key: str | None = None,
        credentials: OneStreamCredentials | None = None,
        params: dict[str, str] | None = None,
        *args,
        **kwargs,
    ):
        """Connector class to ingest data from the OneStream API endpoint..

        API Documentation:
        https://documentation.onestream.com/1388457/Content/REST%20API/OneStream%20WebAPI%20Endpoints.html

        Provides access to various OneStream API endpoints for data extraction and
        management. Each endpoint requires a specific function designed to handle its
        unique requirements.

        Args:
            base_url (str): OneStream base server URL.
            application (str): Name of the OneStream application to connect to.
            config_key (str, optional): Key in viadot config to fetch credentials.
                Defaults to None.
            credentials (OneStreamCredentials, optional): OneStream API credentials.
                Must contain 'api_token'. If not provided, will attempt to fetch from
                viadot config using config_key. Defaults to None.
            params (dict[str, str], optional): Additional API parameters to include
                in requests. Defaults to None, which sets {"api-version": "5.2.0"}.
            *args: Additional positional arguments passed to parent class.
            **kwargs: Additional keyword arguments passed to parent class.

        Note:
            The connector supports three main operations:
                1. Data Adapter queries: Fetch data using OneStream Data Adapters.
                2. SQL queries: Execute SQL queries against OneStream databases.
                3. Data Management: Run Data Management sequences.
        """
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(OneStreamCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)
        self.base_url = base_url
        self.application = application
        self.api_token = self.credentials.get("api_token")
        self.ssl_cert = False
        params_with_default_api_version = {"api-version": "5.2.0"}
        params_with_default_api_version.update(params)
        self.params = params_with_default_api_version

    def _send_api_request(
        self,
        endpoint: str,
        headers: dict[str, str],
        payload: str,
    ) -> requests.Response:
        """Sends a POST request to the specified API endpoint.

        Args:
            endpoint (str): The URL of the API endpoint.
            headers (dict[str, str]): HTTP headers to include in the request.
            payload (str): JSON-encoded string to send in the request body.

        Returns:
            requests.Response: The response object returned by the API.

        Raises:
            requests.exceptions.Timeout: If the request times out.
            requests.exceptions.ConnectionError: If connection fails.
            requests.exceptions.RequestException: For other request failures.
        """
        try:
            response = requests.post(
                url=endpoint,
                params=self.params,
                headers=headers,
                data=payload,
                timeout=(60, 3600),
            )

            response.raise_for_status()

        except requests.exceptions.Timeout:
            self.logger.exception(f"Request to {endpoint} timed out.")
            raise

        except requests.exceptions.ConnectionError:
            self.logger.exception(f"Connection error while accessing {endpoint}.")
            raise

        except requests.exceptions.RequestException:
            self.logger.exception("Request failed.")
            raise

        else:
            self.logger.info(f"API call to {endpoint} successful.")
            return response

    def _extract_data_from_response(
        self, response: requests.Response, adapter_response_key: str
    ) -> dict[str, Any]:
        """Extract records from the API response.

        Filters API response based on the adapter response key.

        Args:
            response (requests.Response): The HTTP response object from the API call.
            adapter_response_key (str): The key in the JSON response that contains
                the adapter's returned records.

        Returns:
            dict[str, Any]: The extracted records from the JSON response under
                the specified key.

        Raises:
            ValueError: If the API response is null or the key is not found.
        """
        json_response = response.json()

        if json_response is None:
            self.logger.error(
                "API call returned null response. Consider adding additional substition custom variables."
            )
            msg = "API response is null."
            raise ValueError(msg)
        return json_response[adapter_response_key]

    def _unpack_custom_subst_vars_to_string(
        self, custom_subst_vars: dict[str, Any]
    ) -> str:
        """Converts a dictionary into a comma-separated string of key=value pairs.

        Args:
            custom_subst_vars (dict[str, Any]): A dictionary mapping substitution
                variable names to lists of possible values.

        Returns:
            str: A key-value pair string formatted as 'key=value', separated by commas.
        """
        custom_subst_vars_list = [
            f"{key}={element}" for key, element in custom_subst_vars.items()
        ]
        return ",".join(custom_subst_vars_list)

    def _get_all_custom_subst_vars_combinations(
        self, custom_subst_vars: dict[str, list[Any]]
    ) -> list[dict[str, Any]]:
        """Generates a list of dictionaries of all combinations of custom variables.

        Args:
            custom_subst_vars (dict[str, list[Any]]): A dictionary where each key
                maps to a list of possible values for that variable. The
                cartesian product of these lists will be computed.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each representing
                a unique combination of custom variable values, where each
                dictionary has the same keys as the input but with single
                values selected from the corresponding lists.
        """
        return [
            dict(zip(custom_subst_vars.keys(), combination, strict=False))
            for combination in product(*custom_subst_vars.values())
        ]

    def _get_adapter_results_data(
        self,
        adapter_name: str,
        workspace_name: str,
        adapter_response_key: str,
        custom_subst_vars: dict[str, Any],
    ) -> dict[str, Any]:
        """Retrieve data from a specified Data Adapter (DA) in OneStream.

        Makes a direct API call to retrieve data from a Data Adapter in the
        specified workspace. The method constructs a POST request with the
        adapter details and custom substition variables, then extracts the relevant
        data from the response.

        Args:
            adapter_name (str): The name of the Data Adapter to query.
            workspace_name (str): The name of the workspace where the DA
                is located.
            adapter_response_key (str): The key in the JSON response that
                contains the adapter's returned data.
            custom_subst_vars (dict[str, Any]):A dictionary mapping substitution
                variable names to lists of possible values.

        Returns:
            dict[str, Any]: The extracted records from the adapter's
                response, keyed by the specified adapter_response_key.

        Raises:
            ValueError: If the API response is null or missing the
                specified response key.
            requests.exceptions.RequestException: If the API request fails
                or times out.
        """
        custom_subst_vars_str = self._unpack_custom_subst_vars_to_string(
            custom_subst_vars
        )
        endpoint = (
            self.base_url.rstrip("/") + "/api/DataProvider/GetAdoDataSetForAdapter"
        )

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
            "Accept-Encoding": "gzip, deflate, br",
        }
        payload = json.dumps(
            {
                "BaseWebServerUrl": self.base_url.rstrip("/") + "/OneStreamWeb",
                "ApplicationName": self.application,
                "WorkspaceName": workspace_name,
                "AdapterName": adapter_name,
                "ResultDataTableName": adapter_response_key,
                "CustomSubstVarsAsCommaSeparatedPairs": custom_subst_vars_str,
            }
        )

        response = self._send_api_request(endpoint, headers, payload)

        return self._extract_data_from_response(response, adapter_response_key)

    def _get_agg_adapter_endpoint_data(
        self,
        adapter_name: str,
        workspace_name: str = "MainWorkspace",
        adapter_response_key: str = "Results",
        custom_subst_vars: dict[str, list[Any]] | None = None,
    ) -> dict:
        """Retrieves and aggregates data from a OneStream Data Adapter (DA).

        This function constructs individual API requests for each
        substitution combination, and aggregates the results into a dictionary keyed by
        a string representation of the variable values.

        Args:
            adapter_name (str): The name of the Data Adapter to query.
            workspace_name (str, optional): The workspace name where the DA
                is located. Defaults to "MainWorkspace".
            adapter_response_key (str, optional): The key in the JSON
                response that contains the adapter's returned data.
                Defaults to "Results".
            custom_subst_vars (dict[str, list[Any]], optional):A dictionary mapping
                substitution variable names to lists of possible values.
                Defaults to None.

        Returns:
            dict: A dictionary where each key is a string of concatenated
                custom variable values (e.g., "Region - Product"), and each
                value is the corresponding data retrieved from the DA.
        """
        custom_subst_vars = custom_subst_vars or {}
        custom_subst_vars_list = self._get_all_custom_subst_vars_combinations(
            custom_subst_vars
        )

        agg_records = []
        for custom_subst_vars in custom_subst_vars_list:
            agg_records.append(
                self._get_adapter_results_data(
                    workspace_name=workspace_name,
                    adapter_response_key=adapter_response_key,
                    custom_subst_vars=custom_subst_vars,
                    adapter_name=adapter_name,
                )
            )

        return agg_records

    @add_viadot_metadata_columns
    def to_df(
        self,
        data: dict[str, list[dict[str, Any]]],
        if_empty: Literal["warn", "skip", "fail"],
    ) -> pd.DataFrame:
        """Convert a dictionary of JSON-like data slices into a Pandas DataFrame.

        Args:
            data (dict[str, list[dict[str, Any]]]): Dictionary where each key
                maps to a list of JSON-like records. Example:
                {
                    "Var1": [
                        {"col1": "xyz", "col2": 123},
                        {"col1": "45fg", "col2": ""}
                    ],
                    "Default": [
                        {"col1": "val3", "col2": 789}
                    ]
                }

        Returns:
            pd.DataFrame: A DataFrame containing all normalized records
                combined into a single table.
        """
        unpacked_data = [part for parts in data for part in parts]
        df = pd.DataFrame(unpacked_data)
        if df.empty:
            msg = "The response data is empty."
            self._handle_if_empty(if_empty, message=msg)
        return df

    def _run_sql(
        self,
        sql_query: str,
        db_location: str,
        results_table_name: str,
        external_db: str,
        custom_subst_vars: dict[str, list[Any]] | None = None,
    ) -> dict[str, Any]:
        """Execute an SQL query against the OneStream database using the API.

        Constructs and sends a POST request to the OneStream API endpoint
        with the provided SQL query parameters. The response is parsed to
        extract the results.

        Args:
            sql_query (str): SQL query to execute.
            db_location (str): Target database location within OneStream.
            results_table_name (str): Name of the results table.
            external_db (str): Name of external database connection.
            custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
                substitution variable names to lists of possible values.
                Defaults to None.

        Returns:
            dict[str, Any]: The extracted records from the specified
                results table.

        Raises:
            ValueError: If the API response is null or the results table
                is not found.
        """
        custom_subst_vars = custom_subst_vars or {}
        custom_subst_vars_str = self._unpack_custom_subst_vars_to_string(
            custom_subst_vars
        )

        endpoint = (
            self.base_url.rstrip("/") + "/api/DataProvider/GetAdoDataSetForSqlCommand"
        )

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
        }
        payload = json.dumps(
            {
                "BaseWebServerUrl": self.base_url.rstrip("/") + "/OneStreamWeb",
                "ApplicationName": self.application,
                "SqlQuery": sql_query,
                "ResultDataTableName": results_table_name,
                "DbLocation": db_location,
                "XFExternalDBConnectionName": external_db,
                "CustomSubstVarsAsCommaSeparatedPairs": custom_subst_vars_str,
            }
        )

        response = self._send_api_request(endpoint, headers, payload)

        return self._extract_data_from_response(response, results_table_name)

    def _get_agg_sql_data(
        self,
        custom_subst_vars: dict | None = None,
        sql_query: str = "",
        db_location: str = "Application",
        results_table_name: str = "Results",
        external_db: str = "",
    ) -> dict:
        """Retrieves and aggregates data from a SQL query in OneStream application.

        This function constructs and executes SQL queries using the provided parameters
        and custom variable combinations.It is typically used to retrieve
        configuration-level data such as metadata, security groups, or settings.

        Args:
            sql_query (str): The SQL query to execute.
            db_location (str, optional): The database context in which to run the query.
                Try "Framework" for system level config data. Defaults to "Application".
            results_table_name (str, optional): The key in the JSON response
                containing the desired data. Defaults to "Results".
            external_db (str, optional): The name of an external database.
                Defaults to an empty string.
            custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
                substitution variable names to lists of possible values.
                Defaults to None.

        Returns:
            dict: A dictionary where each key is a string of concatenated custom
                variable values (e.g., "Entity - Scenario"), and each value is
                    the corresponding data retrieved from the SQL query.
        """
        custom_subst_vars = custom_subst_vars or {}

        custom_subst_vars_list = self._get_all_custom_subst_vars_combinations(
            custom_subst_vars
        )

        agg_records = []

        for custom_subst_var in custom_subst_vars_list:
            agg_records.append(
                self._run_sql(
                    sql_query=sql_query,
                    db_location=db_location,
                    results_table_name=results_table_name,
                    external_db=external_db,
                    custom_subst_vars=custom_subst_var,
                )
            )

        return agg_records

    def _run_data_management_seq(
        self,
        dm_seq_name: str,
        custom_subst_vars: dict | None = None,
    ) -> requests.Response:
        """Executes a Data Management (DM) sequence in OneStream using the API.

        Constructs and sends a POST request to the OneStream DataManagement API to run
        the specified sequence, optionally including custom substitution variables.

        Args:
            dm_seq_name (str): The name of the Data Management sequence to execute.
            custom_subst_vars (dict[str, Any], optional): A dictionary mapping
                substitution variable names to lists of possible values
                to pass to the sequence. Defaults to None.

        Returns:
            requests.Response: The HTTP response object returned by the API after
                executing the sequence.
        """
        custom_subst_vars = custom_subst_vars or {}
        custom_subst_vars_str = self._unpack_custom_subst_vars_to_string(
            custom_subst_vars
        )

        endpoint = self.base_url.rstrip("/") + "/api/DataManagement/ExecuteSequence"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
        }

        payload = json.dumps(
            {
                "BaseWebServerUrl": self.base_url.rstrip("/") + "/OneStreamWeb",
                "ApplicationName": self.application,
                "SequenceName": dm_seq_name,
                "CustomSubstVarsAsCommaSeparatedPairs": custom_subst_vars_str,
            }
        )

        return self._send_api_request(endpoint, headers, payload)
