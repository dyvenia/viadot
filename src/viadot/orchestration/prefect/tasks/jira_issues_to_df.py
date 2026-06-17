"""Prefect task for fetching Jira issues and returning a DataFrame."""

from typing import Any

import pandas as pd
from prefect import task

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.jira import Jira, JiraCredentials


@task(
    name="jira_issues_to_df",
    description="Fetch Jira issues via JQL and return a flat DataFrame.",
    retries=3,
    retry_delay_seconds=10,
    timeout_seconds=60 * 60 * 3,
)
def jira_issues_to_df(
    jql: str,
    fields: list[str] | None = None,
    technical_fields: list[str] | None = None,
    credentials: dict[str, Any] | None = None,
    config_key: str = "jira",
    credentials_secret: str | None = None,
    custom_field_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Fetch Jira issues based on a JQL query and return a DataFrame.

    Args:
        jql (str): JQL query string, e.g. 'project = MYPROJ AND status = Open'.
        fields (list[str], optional): List of human-readable Jira field names
            to fetch, e.g. ['Summary', 'Current Status', 'Assignee'].
            Defaults to None.
        technical_fields (list[str], optional): Raw Jira field ids (e.g.
            "summary", "customfield_16187"). If provided, issues are fetched
            directly and returned without any name resolution. Column names
            keep the raw ids and values are returned exactly as Jira sends
            them. Defaults to None.
        credentials (dict[str, Any], optional): Dict with 'client_id' and
            'client_secret' for Jira OAuth 2.0 authentication. Defaults to None.
        config_key (str, optional): Key to look up credentials in viadot
            config. Defaults to "jira".
        credentials_secret (str, optional): Name of the AWS secret containing
            Jira credentials. Defaults to None.
        custom_field_mapping (dict[str, str], optional): Dict mapping custom
            field names to their corresponding Jira field IDs. Defaults to None.

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
        jql=jql,
        fields=fields,
        technical_fields=technical_fields,
        custom_field_mapping=custom_field_mapping,
    )
