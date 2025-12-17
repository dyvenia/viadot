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
from viadot.utils import add_viadot_metadata_columns, handle_api_request


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
        """Connector class to ingest data from the OneStream API endpoint.

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
        params_with_default_api_version = {"api-version": "5.2.0"}
        params_with_default_api_version.update(params or {})
        self.params = params_with_default_api_version

    def _execute_api_method(
        self,
        api: Literal["data_adapter", "sql_query", "run_data_management_seq"],
        **kwargs,
    ) -> list[dict[str, Any]] | requests.Response:
        """Executes function for selected api endpoint.

        Args:
            api: The API endpoint type to execute.
            **kwargs: Additional arguments passed to the specific API method.

        Returns:
            list[dict[str, Any]] | requests.Response: The result of the API call.
        """
        match api:
            case "data_adapter":
                return self._get_agg_data_adapter_endpoint_data(**kwargs)
            case "sql_query":
                return self._get_agg_sql_query_endpoint_data(**kwargs)
            case "run_data_management_seq":
                return self._run_data_management_seq(**kwargs)

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

    def _get_agg_data_adapter_endpoint_data(
        self,
        adapter_name: str,
        workspace_name: str = "MainWorkspace",
        adapter_response_key: str = "Results",
        custom_subst_vars: dict[str, list[Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch and aggregate data from a OneStream Data Adapter (DA).

        This function constructs API requests (one per substitution-variable
        combination, if provided) and aggregates the results into a list of
        datasets.

        Args:
            adapter_name (str): The name of the Data Adapter to query.
            workspace_name (str, optional): The workspace name where the DA
                is located. Defaults to "MainWorkspace".
            adapter_response_key (str, optional): The key in the JSON
                response that contains the adapter's returned data.
                Defaults to "Results".
            custom_subst_vars (dict[str, list[Any]], optional): A dictionary
                mapping substitution variable names to lists of possible values.
                Defaults to None.

        Returns:
            list[dict[str, Any]]: A flat list of all record dictionaries
                aggregated from all substitution variable combinations.
        """
        custom_subst_vars = custom_subst_vars or {}
        custom_subst_vars_list = self._get_all_custom_subst_vars_combinations(
            custom_subst_vars
        )

        agg_records = []
        for custom_subst_vars in custom_subst_vars_list:
            agg_records.append(
                self._fetch_adapter_results_data(
                    workspace_name=workspace_name,
                    adapter_response_key=adapter_response_key,
                    custom_subst_vars=custom_subst_vars,
                    adapter_name=adapter_name,
                )
            )

        return agg_records

    def _fetch_adapter_results_data(
        self,
        adapter_name: str,
        workspace_name: str,
        adapter_response_key: str,
        custom_subst_vars: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Fetch data from a specified Data Adapter (DA) in OneStream.

        Makes a direct API call to fetch data from a Data Adapter in the
        specified workspace. The method constructs a POST request with the
        adapter details and custom substitution variables, then extracts the
        relevant data from the response.

        Args:
            adapter_name (str): The name of the Data Adapter to query.
            workspace_name (str): The name of the workspace where the DA
                is located.
            adapter_response_key (str): The key in the JSON response that
                contains the adapter's returned data.
            custom_subst_vars (dict[str, Any]):A dictionary mapping substitution
                variable names to lists of possible values.

        Returns:
            list[dict[str, Any]]: A list of record dictionaries from the
                adapter response.

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

        response = handle_api_request(
            method="POST",
            url=endpoint,
            params=self.params,
            headers=headers,
            data=payload,
            timeout=(60, 3600),
        )

        return self._extract_data_from_response(response, adapter_response_key)

    def _get_agg_sql_query_endpoint_data(
        self,
        custom_subst_vars: dict | None = None,
        sql_query: str = "",
        db_location: str = "Application",
        results_table_name: str = "Results",
        external_db: str = "",
    ) -> list[dict[str, Any]]:
        """Fetch and aggregate data from a SQL query endpoint in OneStream.

        This function constructs and executes SQL queries using the provided
        parameters and custom variable combinations. It is typically used to
        retrieve configuration-level data such as metadata, security groups,
        or settings.

        Args:
            sql_query (str): The SQL query to execute.
            db_location (str, optional): The database context in which to run
                the query. Try "Framework" for system level config data.
                Defaults to "Application".
            results_table_name (str, optional): The key in the JSON response
                containing the desired data. Defaults to "Results".
            external_db (str, optional): The name of an external database.
                Defaults to an empty string.
            custom_subst_vars (dict[str, list[Any]], optional): A dictionary
                mapping substitution variable names to lists of possible values.
                Defaults to None.

        Returns:
            list[dict[str, Any]]: A flat list of all record dictionaries
                aggregated from all substitution variable combinations.
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

        response = handle_api_request(
            method="POST",
            url=endpoint,
            params=self.params,
            headers=headers,
            data=payload,
            timeout=(60, 3600),
        )
        return self._extract_data_from_response(response, results_table_name)

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

        return handle_api_request(
            method="POST",
            url=endpoint,
            params=self.params,
            headers=headers,
            data=payload,
            timeout=(60, 3600),
        )

    @add_viadot_metadata_columns
    def to_df(
        self,
        api: Literal["data_adapter", "sql_query"],
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        **kwargs,
    ) -> pd.DataFrame:
        """Get data from the API endpoint and convert to a Pandas DataFrame.

        This method fetches data from the specified API endpoint and converts
        it to a DataFrame. All endpoint-specific parameters should be passed
        as keyword arguments.

        Args:
            api (Literal["data_adapter", "sql_query"]): The API endpoint type
                to query. Only endpoints that return tabular data are supported.
            if_empty (Literal["warn", "skip", "fail"], optional): Action to
                take if the DataFrame is empty.
                - "warn": Logs a warning.
                - "skip": Skips the operation.
                - "fail": Raises an error.
                Defaults to "warn".
            **kwargs: Endpoint-specific parameters passed to the underlying
                API method.
                For "data_adapter":
                    - adapter_name (str): Name of the Data Adapter to query.
                    - workspace_name (str, optional): Workspace name.
                      Defaults to "MainWorkspace".
                    - adapter_response_key (str, optional): Response key.
                      Defaults to "Results".
                    - custom_subst_vars (dict[str, list[Any]], optional):
                      Custom substitution variables.
                For "sql_query":
                    - sql_query (str): The SQL query to execute.
                    - custom_subst_vars (dict, optional): Custom
                      substitution variables.
                    - db_location (str, optional): Database location.
                      Defaults to "Application".
                    - results_table_name (str, optional): Results table name.
                      Defaults to "Results".
                    - external_db (str, optional): External database name.
                      Defaults to "".

        Returns:
            pd.DataFrame: A DataFrame containing all records from the fetched
                data.
        """
        data = self._execute_api_method(api=api, **kwargs)
        unpacked_data = [part for parts in data for part in parts]
        df = pd.DataFrame(unpacked_data)
        if df.empty:
            msg = "The response data is empty."
            self._handle_if_empty(if_empty, message=msg)
        return df

    def execute(
        self,
        api: Literal["run_data_management_seq"],
        **kwargs,
    ) -> requests.Response:
        """Execute an API endpoint operation that returns a Response object.

        This method is for API endpoints that don't return tabular data
        (e.g., "run_data_management_seq"). For data extraction endpoints,
        use to_df() instead. All endpoint-specific parameters should be
        passed as keyword arguments.

        Args:
            api (Literal["run_data_management_seq"]): The API endpoint type
                to execute. Only endpoints that don't return tabular data
                are supported.
            **kwargs: Endpoint-specific parameters.
                For "run_data_management_seq":
                    - dm_seq_name (str): The name of the Data Management
                      sequence to execute.
                    - custom_subst_vars (dict, optional): Custom substitution
                      variables.

        Returns:
            requests.Response: The HTTP response object from the API.
        """
        return self._execute_api_method(api=api, **kwargs)
