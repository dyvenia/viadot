import base64
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


class JiraCredentials(BaseModel):
    """Jira API credentials."""
 
    email: str
    token: str
    url: str


class Jira(Source):

    def __init__(self, 
                 *args,
                 credentials: JiraCredentials, 
                 config_key: str = "jira",
                 **kwargs: Any) -> None:
        
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
 
        self.base_url = validated_creds['url']

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers with Basic Auth (email + API token)."""
        raw = f"{self.credentials['email']}:{self.credentials['token']}"
        encoded = base64.b64encode(raw.encode()).decode()
        return {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def _api_call(
        self,
        url: str,
        method: str = "GET",
        body: str | None = None,
    ) -> dict[str, Any]:
        """Make a single HTTP call to the Jira API.
 
        Args:
            url (str): Full request URL.
            method (str): HTTP method. Defaults to "GET".
            body (str | None): JSON-serialised request body for POST calls.
 
        Returns:
            dict: Parsed JSON response.
        """
        headers = self._build_headers()
        response = handle_api_response(
            url=url,
            headers=headers,
            data=body,
            method=method,
        )
        return response.json()
    
    def _fetch_fields(self) -> list[dict[str, Any]]:
        """Fetch all fields defined in the Jira API using ``GET /rest/api/3/field``.
 
        Returns both system fields and custom fields.
 
        Returns:
            list[dict]: Raw field dicts as returned by the Jira API.
        """
        url = f"{self.base_url}/field"
        response = self._api_call(url=url, method="GET")
        if isinstance(response, list):
            return response
        return response.get("values", [])
    
