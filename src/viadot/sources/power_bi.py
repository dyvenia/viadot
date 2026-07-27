from datetime import datetime, timedelta, timezone
import time
from typing import Literal

import pandas as pd
from pandas.core.api import DataFrame as DataFrame
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.sources.base import Source


class PowerBICredentials(BaseModel):
    """PowerBI API credentials."""

    tenant_id: str
    client_id: str
    client_secret: str


class PowerBiAuth:
    def __init__(
        self,
        credentials: PowerBICredentials,
        config_key: str | None = "powerbi",
        *args,
        **kwargs: str | int | bool,
    ) -> None:
        raw_creds = credentials or get_source_credentials(config_key)
        if isinstance(raw_creds, PowerBICredentials):
            self.credentials = dict(raw_creds)
        else:
            self.credentials = dict(PowerBICredentials(**raw_creds))

        self._token: str | None = None

    def _get_token(self) -> str:
        """Authenticate against Azure AD, cache the token for reuse."""
        if self._token:
            return self._token

        url = f"https://login.microsoftonline.com/{self.credentials['tenant_id']}/oauth2/v2.0/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.credentials["client_id"],
            "client_secret": self.credentials["client_secret"],
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        }
        response = requests.post(url, data=payload, timeout=30)
        response.raise_for_status()
        self._token = response.json()["access_token"]
        return self._token


class PowerBIActivityEvents(PowerBiAuth, Source):
    @staticmethod
    def build_url(date: str) -> str:
        return (
            "https://api.powerbi.com/v1.0/myorg/admin/activityevents"
            f"?startDateTime='{date}T00:00:00.000'"
            f"&endDateTime='{date}T23:59:59.999'"
        )

    def query(self, date: str) -> list[dict]:
        """Fetch all activity events for a single UTC day, handling pagination.

        Args:
            date: date string 'YYYY-MM-DD' (UTC day to extract).

        Returns:
            list[dict]: raw activity event records.
        """
        url = self.build_url(date)
        records: list[dict] = []

        while url:
            headers = {"Authorization": f"Bearer {self._get_token()}"}
            response = requests.get(url, headers=headers, timeout=60)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                self.logger.warning(f"Rate limited, sleeping {retry_after}s")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            body = response.json()
            records.extend(body.get("activityEventEntities", []))
            url = body.get("continuationUri")

        return records

    def to_df(
        self,
        date: str | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> DataFrame | None:
        """Download activity events for a day into a pandas DataFrame.

        Args:
            date: 'YYYY-MM-DD' UTC day to extract. Defaults to yesterday (UTC).
            if_empty: behavior when no events are returned.
        """
        if date is None:
            date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        records = self.query(date)
        if not records:
            self._handle_if_empty(
                if_empty=if_empty, message=f"No activityu events found for {date}."
            )
            return pd.DataFrame()

        return pd.json_normalize(records)
