"""Hubspot API connector."""

import ast
from datetime import datetime
import json
import re
from typing import Any

import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import (
    add_viadot_metadata_columns,
    cast_df_cols,
    handle_api_response,
)


class HubspotCredentials(BaseModel):
    """HubSpot API credentials."""

    token: str


class Hubspot(Source):
    """A class that connects and extracts data from Hubspot API.

    Documentation is available here:
        https://developers.hubspot.com/docs/api/crm/understanding-the-crm.

    Connector allows to pull data in two ways:
        - using base API for crm schemas as an endpoint
            (eg. "contacts", ""line_items", "deals", ...),
        - using full url as endpoint.
    """

    API_URL = "https://api.hubapi.com"

    def __init__(
        self,
        *args,
        credentials: HubspotCredentials | None = None,
        config_key: str = "hubspot",
        **kwargs,
    ):
        """Create an instance of Hubspot.

        Args:
            credentials (Optional[HubspotCredentials], optional): Hubspot credentials.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "hubspot".

        Examples:
            hubspot = Hubspot(
                credentials=credentials,
                config_key=config_key,
            )
            hubspot.call_api(
                method=api_method,
                endpoint=endpoint,
                campaign_ids=campaign_ids,
                contact_type=contact_type,
                filters=filters,
                properties=properties,
                nrows=nrows,
            )
            data_frame = hubspot.to_df()

        Raises:
            CredentialError: If credentials are not provided in local_config or
                directly as a parameter.
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(HubspotCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.full_dataset = None

    def _date_to_unix_millis(self, date: str) -> int:
        """Convert date from "yyyy-mm-dd" to Unix timestamp in milliseconds.

        Milliseconds since 1970-01-01 (UTC). For example, "2023-04-06" -> 1680739200000.

        Args:
            date (str): Input date in format "yyyy-mm-dd".

        Returns:
            int: Milliseconds since 1970-01-01 00:00:00 UTC.
        """
        return int(datetime.timestamp(datetime.strptime(date, "%Y-%m-%d")) * 1000)

    def _get_api_url(
        self,
        endpoint: str | None = None,
        filters: dict[str, Any] | None = None,
        properties: list[str] | None = None,
    ) -> str:
        """Generates full url for Hubspot API given filters and parameters.

        Args:
            endpoint (Optional[str], optional): API endpoint or full URL.
                If a relative endpoint is provided, it will be resolved against API_URL.
                Defaults to None.
            filters (Optional[Dict[str, Any]], optional): Filters defined for the API
                body in specific order. Defaults to None.
            properties (Optional[List[str]], optional): List of user-defined columns to
                be pulled from the API. Defaults to None.

        Returns:
            str: The final API URL.
        """
        if not endpoint:
            msg = "Endpoint must be provided."
            raise ValueError(msg)

        # Full URL passthrough
        if endpoint.startswith("http"):
            return endpoint

        # Normalize leading slash
        endpoint = endpoint.lstrip("/")

        # Some product families (e.g., hubdb) expect direct prefixing
        if endpoint.startswith("hubdb"):
            url = f"{self.API_URL}/{endpoint}"
        elif filters:
            url = f"{self.API_URL}/crm/v3/objects/{endpoint}/search/?limit=100&"
        else:
            url = f"{self.API_URL}/crm/v3/objects/{endpoint}/?limit=100&"

        if properties:
            url += f"properties={','.join(properties)}&"

        return url

    def _format_filters(
        self,
        filters: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        """API body (filters) conversion from a user defined to API language.

        Note: Right now only converts date to Unix Timestamp.

        Args:
            filters (Optional[List[Dict[str, Any]]]): List of filters in JSON format.

        Returns:
            List[Dict[str, Any]]: List of cleaned filters in JSON format.
        """
        if not filters:
            return []

        for item in filters:
            for subitem in item["filters"]:
                for key in list(subitem.keys()):
                    lookup = subitem[key]
                    regex = re.findall(r"\d+-\d+-\d+", lookup)
                    if regex:
                        regex = self._date_to_unix_millis(lookup)
                        subitem[key] = f"{regex}"

        return filters

    def _get_request_body(self, filters: list[dict[str, Any]]) -> str:
        """Clean the filters body and convert to a JSON-formatted string.

        Args:
            filters (List[Dict[str, Any]]): Filters dictionary that will be passed to
                Hubspot API. Defaults to {}.

        Example:
                    filters = {
                                "filters": [
                                    {
                                    "propertyName": "createdate",
                                    "operator": "BETWEEN",
                                    "highValue": "2023-03-27",
                                    "value": "2023-03-26"
                                    }
                                ]
                            }
                Operators between the min and max value are listed below:
                [IN, NOT_HAS_PROPERTY, LT, EQ, GT, NOT_IN, GTE, CONTAINS_TOKEN,
                    HAS_PROPERTY, LTE, NOT_CONTAINS_TOKEN, BETWEEN, NEQ]
                LT - Less than
                LTE - Less than or equal to
                GT - Greater than
                GTE - Greater than or equal to
                EQ - Equal to
                NEQ - Not equal to
                BETWEEN - Within the specified range. In your request, use key-value
                    pairs to set highValue and value. Refer to the example above.
                IN - Included within the specified list. This operator is
                    case-sensitive, so inputted values must be in lowercase.
                NOT_IN - Not included within the specified list
                HAS_PROPERTY - Has a value for the specified property
                NOT_HAS_PROPERTY - Doesn't have a value for the specified property
                CONTAINS_TOKEN - Contains a token. In your request, you can use
                    wildcards (*) to complete a partial search. For example, use the
                    value *@hubspot.com to retrieve contacts with a HubSpot email
                    address.
                NOT_CONTAINS_TOKEN  -Doesn't contain a token

        Returns:
            str: JSON string with the request body.
        """
        return json.dumps({"filterGroups": filters, "limit": 100})

    def _build_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.credentials['token']}",
            "Content-Type": "application/json",
        }

    def _api_call(
        self,
        url: str | None = None,
        body: str | None = None,
        method: str | None = None,
    ) -> dict:
        """General method to connect to Hubspot API and generate the response.

        Args:
            url (Optional[str], optional): Hubspot API url. Defaults to None.
            body (Optional[str], optional): Filters that will be pushed to the API body.
                Defaults to None.
            method (Optional[str], optional): Method of the API call. Defaults to None.

        Returns:
            dict: API response in JSON format.
        """
        headers = self._build_headers()

        response = handle_api_response(
            url=url, headers=headers, data=body, method=method
        )

        return response.json()

    def _get_offset_from_response(
        self, api_response: dict[str, Any]
    ) -> tuple[str | None, Any | None]:
        """Assign offset type/value depending on keys in API response.

        Args:
            api_response (Dict[str, Any]): API response in JSON format.

        Returns:
            tuple[str | None, Any | None]: (offset_type, offset_value)
        """
        if "paging" in api_response:
            offset_type = "after"
            offset_value = (
                api_response.get("paging", {}).get("next", {}).get(offset_type)
            )
            return (offset_type, offset_value)

        has_more = api_response.get("has-more") or api_response.get("hasMore")

        if has_more is False:
            return (None, None)

        if api_response.get("vid-offset"):
            return ("vidOffset", api_response["vid-offset"])

        if api_response.get("offset"):
            return ("offset", api_response["offset"])

        return (None, None)

    def _extract_items(self, response: dict[str, Any]) -> list[Any]:
        """Extract the list of items from various HubSpot response shapes."""
        # Prefer v3 "results"
        if "results" in response and isinstance(response["results"], list):
            return response["results"]
        # Fallback: find the first list-valued key (e.g., v1 'contacts')
        for _, value in response.items():
            if isinstance(value, list):
                return value
        return []

    def _fetch(
        self,
        endpoint: str | None = None,
        filters: list[dict[str, Any]] | None = None,
        properties: list[str] | None = None,
        nrows: int = 1000,
    ) -> list[Any]:
        """General method to connect to Hubspot API and generate the response.

        Args:
            endpoint (Optional[str], optional): API endpoint for an individual request.
                Defaults to None.
            filters (Optional[List[Dict[str, Any]]], optional): Filters defined for the
                API body in specific order. Defaults to None.

        Example:
                    filters=[
                        {
                            "filters": [
                                {
                                    "propertyName": "createdate",
                                    "operator": "BETWEEN",
                                    "highValue": "1642636800000",
                                    "value": "1641995200000",
                                },
                                {
                                    "propertyName": "email",
                                    "operator": "CONTAINS_TOKEN",
                                    "value": "*@xxxx.xx",
                                },
                            ]
                        }
                    ],
            properties (Optional[List[Any]], optional): List of user-defined columns to
                be pulled from the API. Defaults to None.
            nrows (int, optional): Max number of rows to pull during execution.
                Defaults to 1000.
        """
        full_dataset: list[Any] = []
        url = self._get_api_url(
            endpoint=endpoint,
            filters=filters,
            properties=properties,
        )
        if filters:
            filters_formatted = self._format_filters(filters)
            body = self._get_request_body(filters=filters_formatted)
            method = "POST"
            partition = self._api_call(url=url, body=body, method=method)
            full_dataset = self._extract_items(partition)

            while "paging" in partition and len(full_dataset) < nrows:
                body = json.loads(self._get_request_body(filters=filters_formatted))
                body["after"] = partition["paging"]["next"]["after"]
                partition = self._api_call(
                    url=url, body=json.dumps(body), method=method
                )
                full_dataset.extend(self._extract_items(partition))

        else:
            method = "GET"
            partition = self._api_call(url=url, method=method)
            full_dataset = self._extract_items(partition)

            offset_type, offset_value = self._get_offset_from_response(partition)

            while offset_value and len(full_dataset) < nrows:
                url = self._get_api_url(
                    endpoint=endpoint,
                    properties=properties,
                    filters=filters,
                )

                if "?" in url:
                    url += f"&{offset_type}={offset_value}"
                else:
                    url += f"?{offset_type}={offset_value}"

                partition = self._api_call(url=url, method=method)
                full_dataset.extend(self._extract_items(partition))

                offset_type, offset_value = self._get_offset_from_response(partition)

        return full_dataset

    def _fetch_contact_ids(
        self,
        campaign_ids: list[str],
        contact_type: str = "influencedContacts",
    ) -> list[dict[str, Any]]:
        """Fetch influenced contact IDs for multiple campaigns.

        Builds a DataFrame with: campaign_id, contact_id, contact_type.

        Args:
            campaign_ids (list[str]): Campaign IDs to query.
            contact_type (str): Label to attach to each contact (e.g., "influenced").
        """
        rows: list[dict[str, Any]] = []

        for campaign_id in campaign_ids:
            url = f"https://api.hubapi.com/marketing/v3/campaigns/{campaign_id}/reports/contacts/{contact_type}"

            partition = self._api_call(url=url, method="GET")
            results = (partition or {}).get("results", [])
            for item in results:
                contact_id = item.get("id")
                if contact_id is not None:
                    rows.append(
                        {
                            "campaign_id": campaign_id,
                            "contact_id": contact_id,
                            "contact_type": contact_type,
                        }
                    )

        return rows

    def _get_campaign_metrics(
        self,
        campaign_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Fetch metrics for multiple campaigns.

        For each campaign, calls:
        https://api.hubapi.com/marketing/v3/campaigns/{campaign_id}/reports/metrics

        The response structure may vary by account. This method attaches the
        `campaign_id` and copies metric fields into the output rows.

        Args:
            campaign_ids (list[str]): Campaign IDs to query.
        """
        rows: list[dict[str, Any]] = []

        for campaign_id in campaign_ids:
            url = f"https://api.hubapi.com/marketing/v3/campaigns/{campaign_id}/reports/metrics"
            partition = self._api_call(url=url, method="GET")
            metrics = partition or {}
            row: dict[str, Any] = {"campaign_id": campaign_id}
            for key in [
                "sessions",
                "newContactsFirstTouch",
                "newContactsLastTouch",
                "influencedContacts",
            ]:
                if key in metrics:
                    row[key] = metrics[key]
            rows.append(row)

        return rows

    def _get_campaign_budget_totals(
        self,
        campaign_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Fetch budget totals for multiple campaigns.

        For each campaign, calls:
        https://api.hubapi.com/marketing/v3/campaigns/{campaign_id}/budget/totals

        The response includes many fields (budgetItems/spendItems), but we only keep:
        - budgetTotal
        - remainingBudget
        - spendTotal
        along with campaign_id.

        Args:
            campaign_ids (list[str]): Campaign IDs to query.
        """
        rows: list[dict[str, Any]] = []

        for campaign_id in campaign_ids:
            url = f"https://api.hubapi.com/marketing/v3/campaigns/{campaign_id}/budget/totals"
            data = self._api_call(url=url, method="GET") or {}
            row: dict[str, Any] = {
                "campaign_id": campaign_id,
                "budgetTotal": data.get("budgetTotal"),
                "remainingBudget": data.get("remainingBudget"),
                "spendTotal": data.get("spendTotal"),
                "currencyCode": data.get("currencyCode"),
            }
            rows.append(row)

        return rows

    def _get_campaign_details(self, campaign_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch details for multiple campaigns.

        For each campaign, calls:
        https://api.hubapi.com/marketing/v3/campaigns/{campaign_id}?properties=...
        The HubSpot response keeps requested fields inside the 'properties' object.

        Args:
            campaign_ids (list[str]): Campaign IDs to query.
        """
        props = [
            "hs_name",
            "hs_start_date",
            "hs_end_date",
            "hs_notes",
            "hs_owner",
        ]

        rows: list[dict[str, Any]] = []

        for campaign_id in campaign_ids:
            url = f"https://api.hubapi.com/marketing/v3/campaigns/{campaign_id}?properties={','.join(props)}"
            data = self._api_call(url=url, method="GET") or {}
            row: dict[str, Any] = {"campaign_id": campaign_id}
            properties_obj = data.get("properties") or {}
            for p in props:
                row[p] = properties_obj.get(p)
            rows.append(row)

        return rows
    
    @staticmethod
def _extract_identity_fields(row: dict[str, Any], col: str = "identity_profiles") -> None:
    """Extract email and LEAD_GUID from identity_profiles into flat columns.

    Mutates the row in place — adds:
        identity_email, identity_lead_guid, identity_saved_at

    Args:
        row (dict[str, Any]): Single row dict to mutate.
        col (str): Column name holding the identity_profiles list.
    """
    raw = row.get(col)
    if not raw:
        return

    # Parse if still a string
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return

    if not isinstance(raw, list) or not raw:
        return

    profile = raw[0]
    row["identity_saved_at"] = profile.get("saved-at-timestamp")

    for identity in profile.get("identities", []):
        id_type = identity.get("type")
        if id_type == "EMAIL":
            row["identity_email"] = identity.get("value")
        elif id_type == "LEAD_GUID":
            row["identity_lead_guid"] = identity.get("value")

    del row[col]

    @staticmethod
    def _expand_jsonlike_values(
        rows: list[dict[str, Any]],
        max_depth: int = 3,
        expand_first_item_cols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Expand JSON-like values into new columns recursively.

        For every row, detect columns that contain JSON-like objects (stringified
        JSON or dictionaries) and recursively flatten them into new top-level columns.
        Lists are serialized to a JSON string instead of being indexed into separate
        columns — this prevents schema explosion when list lengths vary across rows.

        For selected columns passed via `expand_first_item_cols`, only the first
        element of the list is expanded into top-level columns. This is a safe
        middle ground: you get structured fields without multiplying columns when
        list lengths vary across rows.

        The naming convention for expanded dicts is: <original_column>_json_<field>
        (or <original_column>_<field> if it was already expanded).

        IMPORTANT: The original nested columns are deleted from the row after
        successful expansion to prevent schema issues downstream.

        Args:
            rows (list[dict[str, Any]]): The list of row dictionaries to expand.
            max_depth (int): Maximum recursion depth for nested dict expansion.
                Defaults to 3.
            expand_first_item_cols (list[str] | None): Column names whose list value
                should be partially expanded — only the first element is flattened
                into top-level columns. Remaining elements are discarded.
                Defaults to None (all lists are serialized to JSON string).

        Returns:
            list[dict[str, Any]]: The rows with flattened JSON structures.
        """
        if not isinstance(rows, list):
            return rows

        expand_first_item_cols = set(expand_first_item_cols or [])

        for row in rows:
            if not isinstance(row, dict):
                continue

            needs_expansion = True
            depth = 0
            while needs_expansion and depth < max_depth:
                needs_expansion = False
                depth += 1
                for col in list(row.keys()):
                    value = row.get(col)

                    # Attempt to parse stringified JSON/Python literals
                    if isinstance(value, str):
                        val_str = value.strip()
                        if val_str.startswith(("{", "[")):
                            try:
                                value = json.loads(val_str)
                                row[col] = value
                            except json.JSONDecodeError:
                                try:
                                    value = ast.literal_eval(val_str)
                                    row[col] = value
                                except (ValueError, SyntaxError):
                                    pass

                    # Flatten dicts into top-level columns
                    if isinstance(value, dict):
                        for k, v in value.items():
                            new_key = (
                                f"{col}_{k}" if "_json_" in col else f"{col}_json_{k}"
                            )
                            row[new_key] = v
                        del row[col]
                        needs_expansion = True

                    elif isinstance(value, list) and value:
                        if col in expand_first_item_cols and isinstance(value[0], dict):
                            # Expand only the first element — stable schema regardless
                            # of how many items are in the list for a given contact.
                            first = value[0]
                            for k, v in first.items():
                                new_key = f"{col}_json_{k}"
                                row[new_key] = v
                            del row[col]
                            needs_expansion = True
                        else:
                            # Serialize lists to JSON string — never index into
                            # col_0_key, col_1_key... because variable-length lists
                            # cause a different schema on every load.
                            row[col] = json.dumps(value)

        return rows

    def call_api(
        self,
        method: str | None = None,
        endpoint: str | None = None,
        campaign_ids: list[str] | None = None,
        contact_type: str | None = None,
        filters: list[dict[str, Any]] | None = None,
        properties: list[str] | None = None,
        nrows: int = 1000,
    ) -> list[dict[str, Any]]:
        """Dispatch a HubSpot API call and return results.

        Args:
            method (str | None): Logical method selector ("get_all_contacts",
                "get_campaign_metrics", "get_campaign_details",
                "get_campaign_budget_totals", "fetch_contact_ids",
                or None for generic fetch).
            endpoint (str | None): Endpoint or full URL for generic fetch.
            campaign_ids (list[str] | None): Campaign IDs for campaign-specific methods.
            contact_type (str | None): Contact type for "fetch_contact_ids".
            filters (list[dict[str, Any]] | None): The filters to apply to the request.
            properties (list[str] | None): The properties to include in the request.
            nrows (int): Maximum number of rows to fetch.
        """
        methods_requiring_campaigns = [
            "get_campaign_metrics",
            "get_campaign_details",
            "get_campaign_budget_totals",
            "fetch_contact_ids",
        ]

        if method in methods_requiring_campaigns and not campaign_ids:
            self.logger.info(
                "No campaign_ids provided. Querying endpoint /marketing/v3/campaigns for full list..."
            )

            # HubSpot API v3
            all_campaigns_data = self._fetch(
                endpoint="https://api.hubapi.com/marketing/v3/campaigns", nrows=10000
            )

            campaign_ids = [c["id"] for c in all_campaigns_data if "id" in c]
            self.logger.info(
                f"Downloaded {len(campaign_ids)} campaigns id's. Beginning download of metrics..."
            )

            if not campaign_ids:
                self.logger.warning("There is no campaign_ids on HubSpot.")
                return []

        # HubSpot API v1
        if method == "get_all_contacts":
            data = self._fetch(
                endpoint="https://api.hubapi.com/contacts/v1/lists/all/contacts/all",
                nrows=nrows,
            )
        elif method == "get_campaign_metrics":
            data = self._get_campaign_metrics(
                campaign_ids=campaign_ids,
            )
        elif method == "get_campaign_budget_totals":
            data = self._get_campaign_budget_totals(
                campaign_ids=campaign_ids,
            )
        elif method == "get_campaign_details":
            data = self._get_campaign_details(
                campaign_ids=campaign_ids,
            )
        elif method == "fetch_contact_ids":
            data = self._fetch_contact_ids(
                campaign_ids=campaign_ids,
                contact_type=contact_type,
            )
        else:
            # Default fetch (usually v3 for most recent HubSpot objects)
            data = self._fetch(
                endpoint=endpoint,
                filters=filters,
                properties=properties,
                nrows=nrows,
            )

        # --- Post-processing: flatten nested structures into stable top-level columns ---

        # Expand dicts and serialize lists to JSON strings.
        # form-submissions is passed to expand_first_item_cols so only its first
        # element is flattened — avoids col_0_key / col_1_key schema explosion
        # when contacts have different numbers of form submissions across loads.
        data = self._expand_jsonlike_values(
            data,
            expand_first_item_cols=["form-submissions"],
        )

        # identity_profiles requires a dedicated extractor because it has a
        # two-level nested structure: a list of profiles, each containing a list
        # of typed identities (EMAIL, LEAD_GUID). _expand_jsonlike_values cannot
        # safely handle this without producing indexed columns — instead we
        # extract the primary email and LEAD_GUID into named flat columns.
        for row in data:
            self._extract_identity_fields(row, col="identity_profiles")

        return data

    @add_viadot_metadata_columns
    def to_df(
        self,
        data: list[dict[str, Any]] | None = None,
        if_empty: str = "warn",
    ) -> pd.DataFrame:
        """Generate a pandas DataFrame with the data in the Response and metadata.

        Args:
            data (list[dict[str, Any]] | None): The data to convert to pandas DataFrame.
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn".

        Returns:
            pd.DataFrame: The response data as a pandas DataFrame plus viadot metadata.
        """
        super().to_df(if_empty=if_empty)

        data_frame = pd.DataFrame(data)

        # change all object columns to string
        data_frame = cast_df_cols(data_frame, types_to_convert=["object"])

        if data_frame.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info("Successfully downloaded data from the Hubspot API.")

        return data_frame
