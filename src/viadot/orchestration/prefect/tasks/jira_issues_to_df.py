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
)
def jira_issues_to_df(
    jql: str,
    fields: list[str],
    credentials: dict[str, Any] | None = None,
    config_key: str = "jira",
    credentials_secret: str | None = None,
) -> pd.DataFrame:

    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )

    source = Jira(
        credentials=JiraCredentials(**credentials),
        config_key=config_key,
    )
    return source.to_df(jql=jql, fields=fields)
