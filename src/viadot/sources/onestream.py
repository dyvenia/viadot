"""OneStream API connector for data extraction and management.

This module provides the OneStream class for interacting with OneStream API endpoints,
including Data Adapter queries, SQL queries, and Data Management sequence execution.
"""

from itertools import product
import json
from typing import Any

import pandas as pd
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.sources.base import Source


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
        server_url: str,
        application: str,
        config_key: str = "onestream",
        credentials: OneStreamCredentials | None = None,
        api_params: dict[str, str] | None = None,
        *args,
        **kwargs,
    ):
        """A connector class to ingest data out of the OneStream API endpoint.

        This connector provides access to various OneStream API endpoints for data
        extraction and management. Each endpoint requires a specific function designed
        to handle its unique requirements.

        Args:
            server_url (str): Base URL of the OneStream server.
            application (str): Name of the OneStream application to connect to.
            config_key (str, optional): Key in viadot config to fetch credentials.
                Defaults to "onestream".
            credentials (OneStreamCredentials | None): OneStream API credentials.
                Must contain 'api_token'. If not provided, will attempt to fetch from
                viadot config using config_key. Defaults to None.
            api_params (dict[str, str] | None): Additional API parameters to include
                in requests. Defaults to None, which sets {"api-version": "5.2.0"}.
            *args: Additional positional arguments passed to parent class.
            **kwargs: Additional keyword arguments passed to parent class.

        Note:
            The connector supports three main operations:
            1. Data Adapter queries: Fetch data using OneStream Data Adapters
            2. SQL queries: Execute SQL queries against OneStream databases
            3. Data Management: Run Data Management sequences
        """
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(OneStreamCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)
        self.server_url = server_url
        self.application = application
        self.api_token = self.credentials.get("api_token")
        self.ssl_cert = False
        self.api_params = api_params or {"api-version": "5.2.0"}

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
                params=self.api_params,
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

    def _fetch_req_results(
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
                "API call returned null response. Consider adding additional custom variables."
            )
            msg = "API response is null."
            raise ValueError(msg)

        return json_response[adapter_response_key]

    def _unpack_custom_vars_to_string(self, custom_vars: dict) -> str:
        """Converts a dictionary into a comma-separated string of key=value pairs.

        Args:
            custom_vars (dict): A dictionary containing custom variable
                names and their corresponding values.

        Returns:
            str: A key-value pair string formatted as 'key=value', separated by commas.
        """
        custom_vars_list = [f"{key}={element}" for key, element in custom_vars.items()]
        return ",".join(custom_vars_list)

    def _get_all_custom_vars_combinations(
        self, custom_vars: dict[str, list[Any]]
    ) -> list[dict[str, Any]]:
        """Generates a list of dictionaries of all combinations of custom variables.

        Args:
            custom_vars (dict[str, list[Any]]): A dictionary where each key
                maps to a list of possible values for that variable. The
                cartesian product of these lists will be computed.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each representing
                a unique combination of custom variable values, where each
                dictionary has the same keys as the input but with single
                values selected from the corresponding lists.
        """
        return [
            dict(zip(custom_vars.keys(), combination, strict=False))
            for combination in product(*custom_vars.values())
        ]

    def _get_adapter_results_data(
        self,
        adapter_name: str,
        workspace_name: str,
        adapter_response_key: str,
        custom_vars: dict[str, Any],
    ) -> dict[str, Any]:
        """Retrieve data from a specified Data Adapter (DA) in OneStream.

        Makes a direct API call to retrieve data from a Data Adapter in the
        specified workspace. The method constructs a POST request with the
        adapter details and custom variables, then extracts the relevant
        data from the response.

        Args:
            adapter_name (str): The name of the Data Adapter to query.
            workspace_name (str): The name of the workspace where the DA
                is located.
            adapter_response_key (str): The key in the JSON response that
                contains the adapter's returned data.
            custom_vars (dict[str, Any]): A dictionary mapping variable names
                (strings) to their values. Values can be of any type that can
                be converted to strings, as they are used as substitution
                variables in the Data Adapter.

        Returns:
            dict[str, Any]: The extracted records from the adapter's
                response, keyed by the specified adapter_response_key.

        Raises:
            ValueError: If the API response is null or missing the
                specified response key.
            requests.exceptions.RequestException: If the API request fails
                or times out.
        """
        custom_vars_str = self._unpack_custom_vars_to_string(custom_vars)
        endpoint = (
            self.server_url.rstrip("/") + "/api/DataProvider/GetAdoDataSetForAdapter"
        )

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
            "Accept-Encoding": "gzip, deflate, br",
            # Content-Length: "999"
        }
        payload = json.dumps(
            {
                "BaseWebServerUrl": self.server_url.rstrip("/") + "/OneStreamWeb",
                "ApplicationName": self.application,
                "WorkspaceName": workspace_name,
                "AdapterName": adapter_name,
                "ResultDataTableName": adapter_response_key,
                "CustomSubstVarsAsCommaSeparatedPairs": custom_vars_str,
            }
        )

        response = self._send_api_request(endpoint, headers, payload)

        return self._fetch_req_results(response, adapter_response_key)

    def get_agg_adapter_endpoint_data(
        self,
        adapter_name: str,
        workspace_name: str = "MainWorkspace",
        adapter_response_key: str = "Results",
        custom_vars: dict | None = None,
    ) -> dict:
        """Retrieves and aggregates data from a OneStream Data Adapter (DA).

        This function generates the cartesian product of all values in the
        `custom_vars` dictionary, constructs individual API requests for each
        combination, and aggregates the results into a dictionary keyed by
        a string representation of the variable values.

        Args:
            adapter_name (str): The name of the Data Adapter to query.
            workspace_name (str, optional): The workspace name where the DA
                is located. Defaults to "MainWorkspace".
            adapter_response_key (str, optional): The key in the JSON
                response that contains the adapter's returned data.
                Defaults to "Results".
            custom_vars (dict, optional): A dictionary where each key maps
                to a list of possible values for that variable.

        Returns:
            dict: A dictionary where each key is a string of concatenated
                custom variable values (e.g., "Region - Product"), and each
                value is the corresponding data retrieved from the DA.
        """
        custom_vars = custom_vars or {}

        custom_vars_list = self._get_all_custom_vars_combinations(custom_vars)

        agg_records = {}

        for custom_var in custom_vars_list:
            var_value = " - ".join(custom_var.values()) if custom_var else "Default"
            agg_records[var_value] = self._get_adapter_results_data(
                workspace_name=workspace_name,
                adapter_response_key=adapter_response_key,
                custom_vars=custom_vars,
                adapter_name=adapter_name,
            )

        return agg_records

    def _to_df(self, data: dict[str, list[dict[str, Any]]]) -> pd.DataFrame:
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
        if not data:
            msg = "The response data dictionary is empty"
            raise ValueError(msg)
        df_parts = (pd.json_normalize(data[key]) for key in data)
        return pd.concat(df_parts, ignore_index=True)

    def _run_sql(
        self,
        sql_query: str,
        db_location: str,
        results_table_name: str,
        external_db: str,
        custom_vars: dict[str, list[Any]] | None = None,
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
            custom_vars (dict[str, list[Any]] | None): Dictionary mapping
                variable names to lists of possible values. Defaults to None.

        Returns:
            dict[str, Any]: The extracted records from the specified
                results table.

        Raises:
            ValueError: If the API response is null or the results table
                is not found.
        """
        custom_vars = custom_vars or {}
        custom_vars_str = self._unpack_custom_vars_to_string(custom_vars)

        endpoint = (
            self.server_url.rstrip("/") + "/api/DataProvider/GetAdoDataSetForSqlCommand"
        )

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
        }
        # TODO: Check how it behaves when none values are passed as db location etc
        payload = json.dumps(
            {
                "BaseWebServerUrl": self.server_url.rstrip("/") + "/OneStreamWeb",
                "ApplicationName": self.application,
                "SqlQuery": sql_query,
                "ResultDataTableName": results_table_name,
                "DbLocation": db_location,
                "XFExternalDBConnectionName": external_db,
                "CustomSubstVarsAsCommaSeparatedPairs": custom_vars_str,
            }
        )

        response = self._send_api_request(endpoint, headers, payload)

        return self._fetch_req_results(response, results_table_name)

    def get_agg_sql_data(
        self,
        custom_vars: dict | None = None,
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
                Defaults to an empty string.
            custom_vars (dict, optional): A dictionary where each key maps to a list of
                possible values for that variable.

        Returns:
            dict: A dictionary where each key is a string of concatenated custom
                variable values (e.g., "Entity - Scenario"), and each value is
                    the corresponding data retrieved from the SQL query.
        """
        custom_vars = custom_vars or {}

        custom_vars_list = self._get_all_custom_vars_combinations(custom_vars)

        agg_records = {}

        for custom_var in custom_vars_list:
            var_value = " - ".join(custom_var.values()) if custom_var else "Default"
            agg_records[var_value] = self._run_sql(
                sql_query=sql_query,
                db_location=db_location,
                results_table_name=results_table_name,
                external_db=external_db,
                custom_vars=custom_vars,
            )

        return agg_records

    def run_data_management_seq(
        self,
        dm_seq_name: str,
        custom_vars: dict | None = None,
    ) -> requests.Response:
        """Executes a Data Management (DM) sequence in OneStream using the API.

        Constructs and sends a POST request to the OneStream DataManagement API to run
        the specified sequence, optionally including custom substitution variables.

        Args:
            dm_seq_name (str): The name of the Data Management sequence to execute.
            custom_vars (dict, optional): A dictionary of custom substitution variables
                to pass to the sequence.Defaults to an empty dictionary.

        Returns:
            requests.Response: The HTTP response object returned by the API after
                executing the sequence.
        """
        custom_vars = custom_vars or {}
        custom_vars_str = self._unpack_custom_vars_to_string(custom_vars)

        endpoint = self.server_url.rstrip("/") + "/api/DataManagement/ExecuteSequence"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
        }

        payload = json.dumps(
            {
                "BaseWebServerUrl": self.server_url.rstrip("/") + "/OneStreamWeb",
                "ApplicationName": self.application,
                "SequenceName": dm_seq_name,
                "CustomSubstVarsAsCommaSeparatedPairs": custom_vars_str,
            }
        )

        return self._send_api_request(endpoint, headers, payload)
