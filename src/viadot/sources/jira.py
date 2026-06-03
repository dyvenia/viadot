from typing import Any

import pandas as pd
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import (
    handle_api_response,
)


class JiraCredentials(BaseModel):
    """Jira API credentials."""

    CLIENT_ID: str
    CLIENT_SECRET: str


class Jira(Source):
    def __init__(
        self,
        *args,
        credentials: JiraCredentials,
        config_key: str = "jira",
        **kwargs: Any,
    ) -> None:
        """Create an instance of Jira.

        Args:
            credentials (Optional[JiraCredentials], optional): Jira credentials.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "jira".

        Raises:
            CredentialError: If credentials are not provided in local_config or
                directly as a parameter.
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(JiraCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self._access_token: str | None = None
        self._cloud_id: str | None = None

    def _get_access_token(self) -> str:
        if self._access_token:
            return self._access_token
        resp = requests.post(
            "https://api.atlassian.com/oauth/token",
            json={
                "grant_type": "client_credentials",
                "client_id": self.credentials["CLIENT_ID"],
                "client_secret": self.credentials["CLIENT_SECRET"],
            },
        )
        resp.raise_for_status()
        self._access_token = resp.json()["access_token"]
        return self._access_token

    def _get_cloud_id(self) -> str:
        if self._cloud_id:
            return self._cloud_id
        token = self._get_access_token()
        resp = requests.get(
            "https://api.atlassian.com/oauth/token/accessible-resources",
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        self._cloud_id = resp.json()[0]["id"]
        return self._cloud_id

    def _build_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
        }

    def _api_call(self, endpoint: str, params: dict | None = None) -> Any:
        cloud_id = self._get_cloud_id()
        url = f"https://api.atlassian.com/ex/jira/{cloud_id}/rest/api/3/{endpoint}"
        response = handle_api_response(
            url=url,
            headers=self._build_headers(),
            params=params,
            method="GET",
        )
        return response.json()

    def _fetch_fields(self) -> list[dict[str, Any]]:
        response = self._api_call("field")
        if isinstance(response, list):
            return response
        return response.get("values", [])

    def _fetch_issues(self, jql: str, fields: list[str]) -> list[dict]:
        all_issues = []
        next_page_token = None

        while True:
            params = {
                "jql": jql,
                "maxResults": 100,
                "fields": ",".join(fields),
            }
            if next_page_token:
                params["nextPageToken"] = next_page_token

            resp = self._api_call("search/jql", params=params)

            issues = resp.get("issues", [])
            all_issues.extend(issues)

            if resp.get("isLast", True):
                break

            next_page_token = resp.get("nextPageToken")
        return all_issues

    def to_df(
        self,
        jql: str,
        fields: list[str],
    ) -> pd.DataFrame:
        """Fetch Jira issues and return as a flat DataFrame.

        Args:
            jql (str): JQL query.
            fields (list[str]): List of field names to include in the DataFrame.

        Returns:
            pd.DataFrame: Flat DataFrame with renamed columns.
        """
        field_map = self._get_field_map()
        field_paths = {
            name: self._resolve_field_path(name, field_map) for name in fields
        }
        missing = [k for k, v in field_paths.items() if v is None]
        if missing:
            raise ValueError(f"Fields not found: {missing}")

        field_ids = list({x.split(".")[1] for x in field_paths.values()})
        issues = self._fetch_issues(jql=jql, fields=field_ids)
        df = pd.json_normalize(issues)
        if df.empty:
            return pd.DataFrame(columns=["Key"] + fields)

        rename_final: dict[str, str] = {
            path: name for name, path in field_paths.items()
        }
        cols = ["key"] + [p for p in rename_final if p in df.columns]
        return df[cols].set_axis(["Key"] + [rename_final[c] for c in cols[1:]], axis=1)

    def _get_field_map(self) -> dict[str, dict]:
        """Build a map of field name -> {id, type} from Jira /field endpoint.

        Returns:
            dict: e.g. {"Failure type - Software (grouped)": {"id": "customfield_16187", "schema": {"type": "option"}}}
        """
        fields = self._fetch_fields()
        return {
            field["name"]: {
                "id": field["id"],
                "schema": field.get("schema", {}),
            }
            for field in fields
        }

    def _resolve_field_path(self, field_name: str, field_map: dict) -> str | None:
        """Resolve a human-readable field name to a JSON path.

        Args:
            field_name (str): e.g. "Failure type - Software (grouped)"
            field_map (dict): output of _get_field_map()

        Returns:
            str: JSON path e.g. "fields.customfield_123.value"
                or None if field not found.
        """
        # standard fields -> known paths
        STANDARD_FIELDS = {
            "Summary": "fields.summary",
            "Current Status": "fields.status.name",
            "Issue Type": "fields.issuetype.name",
            "Resolution": "fields.resolution.name",
            "Created": "fields.created",
            "Resolved": "fields.resolutiondate",
            "Project Key": "fields.project.key",
            "Project Name": "fields.project.name",
        }
        if field_name in STANDARD_FIELDS:
            return STANDARD_FIELDS[field_name]

        # custom fields
        field = field_map.get(field_name)
        if not field:
            return None

        field_id = field["id"]
        schema_type = field["schema"].get("type", "")
        # if type is option/array — we fetch .value
        if schema_type in ("option", "array"):
            return f"fields.{field_id}.value"

        return f"fields.{field_id}"
