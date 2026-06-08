"""Prefect task for fetching Jira issues and returning a DataFrame."""

from typing import Any

import pandas as pd
from prefect import task

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.jira import Jira, JiraCredentials


@task(
    name="jira_to_df",
    description="Fetch Jira issues via JQL and return a flat DataFrame.",
    retries=3,
    retry_delay_seconds=10,
    timeout_seconds=60 * 60 * 3,
)
def jira_issues_to_df(
    jql: str,
    fields: list[str],
    credentials: dict[str, Any] | None = None,
    config_key: str = "jira",
    credentials_secret: str | None = None,
    custom_field_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Fetch Jira issues based on a JQL query and return a DataFrame.

    Args:
        jql: JQL query string, e.g. 'project = MYPROJ AND status = Open'.
        fields: List of human-readable Jira field names to fetch,
            e.g. ['Summary', 'Current Status', 'Assignee'].
        credentials: Optional dict with 'client_id' and 'client_secret'
            for Jira OAuth 2.0 authentication.
        config_key: Key to look up credentials in viadot config.
            Defaults to 'jira'.
        credentials_secret: Optional name of the AWS secret
            containing Jira credentials.
        custom_field_mapping: Optional dict mapping for custom field names
            to their corresponding Jira field IDs.

    Returns:
        pd.DataFrame: Flat DataFrame with Jira issues.
    """
    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )

    source = Jira(
        credentials=JiraCredentials(**credentials),
        config_key=config_key,
    )
    return source.to_df(
        jql=jql, fields=fields, custom_field_mapping=custom_field_mapping
    )
