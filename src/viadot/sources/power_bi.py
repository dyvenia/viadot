from collections.abc import Generator
from datetime import datetime, timedelta, timezone
import json
import logging
import time
from typing import Any, ClassVar, Literal

import pandas as pd
from pandas.core.api import DataFrame as DataFrame
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import handle_api_response


logging.basicConfig(level=logging.INFO)


class PowerBICredentials(BaseModel):
    """PowerBI API credentials."""

    tenant_id: str
    client_id: str
    client_secret: str


class PowerBiAuth:
    def __init__(
        self, credentials: PowerBICredentials, config_key: str | None = "powerbi"
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

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}"}


class PowerBIActivityEvents(PowerBiAuth, Source):
    """Power BI Admin - Activity Events source.

    Args:
        config_key: key in local viadot config with tenant_id/client_id/
            client_secret.
        credentials: credentials dict, alternative to config_key.
        columns_to_extract (list(str)): List of columns to extract. Defaults to None.
    """

    def __init__(
        self,
        *args,
        credentials: PowerBICredentials | None = None,
        config_key: str | None = "powerbi",
        columns_to_extract: list[str] | None = None,
        **kwargs: str | int | bool,
    ) -> None:
        super().__init__(
            *args, credentials=credentials, config_key=config_key, **kwargs
        )
        self.columns_to_extract = columns_to_extract

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
            response = requests.get(url, headers=self.headers, timeout=60)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                self.logger.warning(f"Rate limited, sleeping {retry_after}s")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            body = response.json()
            events = body.get("activityEventEntities", [])

            if self.columns_to_extract:
                events = [
                    {col: event.get(col) for col in self.columns_to_extract}
                    for event in events
                ]
            records.extend(events)

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


REPORT_FIELD_MAPPING: dict[str, str] = {
    "Report_id": "id",
    "Report_desc": "name",
    "Report_type": "reportType",
    "Report_modified_date": "modifiedDateTime",
    "Report_created_date": "createdDateTime",
    "Report_modified_by": "modifiedBy",
    "dataset_id": "datasetId",
}

WORKSPACE_FIELD_MAPPING: dict[str, str] = {
    "Workspace_id": "id",
    "Workspace_desc": "name",
    "Description": "description",
    "Workspace_type": "type",
    "Workspace_state": "state",
}

OWNER_FIELD_MAPPING: dict[str, str] = {
    "Owner_display_name": "displayName",
    "Owner_email": "emailAddress",
    "Owner_access_right": "reportUserAccessRight",
}

OWNER_ACCESS_RIGHT_VALUE = "Owner"

DEFAULT_GET_INFO_QUERY_PARAMS: dict[str, bool] = {
    "lineage": True,
    "datasourceDetails": True,
    "datasetSchema": True,
    "datasetExpressions": True,
    "getArtifactUsers": True,
}


class PowerBiReportScanner(PowerBiAuth, Source):
    def __init__(
        self,
        *args,
        credentials: PowerBICredentials | None = None,
        config_key: str | None = None,
        get_info_query_params: dict[str, bool] | None = None,
        logger: logging.Logger | None = None,
        **kwargs: str | int | bool,
    ) -> None:
        self.get_info_query_params = (
            get_info_query_params or DEFAULT_GET_INFO_QUERY_PARAMS
        )

        super().__init__(
            *args,
            credentials=credentials,
            config_key=config_key,
            **kwargs,
        )
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("PowerBiReportScanner initialized.")

    def get_modified_workspaces(self, target_date: str | None = None):
        url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified"
        if target_date is None:
            target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
        else:
            try:
                datetime.strptime(target_date, "%Y-%m-%d")
            except ValueError as e:
                msg = (
                    f"Invalid target_date '{target_date}'. Expected format: YYYY-MM-DD."
                )
                raise ValueError(msg) from e

        self.logger.info(f"Target_date : {target_date} (YYYY-MM-DD)")
        date_str = f"{target_date}T00:00:00.0000000Z"
        params = {
            "modifiedSince": date_str,
            "excludePersonalWorkspaces": True,
            "excludeInActiveWorkspaces": True,
        }
        response = handle_api_response(
            url, headers=self.headers, params=params, method="GET"
        )
        workspace_ids = [item["id"] for item in response.json()]
        self.logger.info(f"Found {len(workspace_ids)} modified workspaces.")
        return workspace_ids

    @staticmethod
    def chunk_list(input_list: list, size: int = 100) -> Generator[list, Any, Any]:
        for i in range(0, len(input_list), size):
            yield input_list[i : i + size]

    def get_workspaces_info(self, workspace_ids: list[str]) -> list[str]:
        scan_ids: list[str] = []
        for chunk in self.chunk_list(workspace_ids):
            url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo"
            response = handle_api_response(
                url,
                headers={**self.headers, "Content-Type": "application/json"},
                params=self.get_info_query_params,
                method="POST",
                data=json.dumps({"workspaces": chunk}),
            )
            scan_ids.append(response.json()["id"])
        return scan_ids

    def wait_for_scan(
        self, scan_id: str, timeout: int = 300, interval: int = 15
    ) -> bool:
        status_url = (
            f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}"
        )
        elapsed = 0
        while elapsed < timeout:
            response = handle_api_response(
                status_url, headers=self.headers, method="GET"
            )
            status = response.json()["status"]
            if status == "Succeeded":
                return True
            if status == "Failed":
                self.logger.warning(f"Scan {scan_id} failed")
                return False
            time.sleep(interval)
            elapsed += interval
        raise TimeoutError(f"Scan {scan_id} timed out")

    def fetch_report_scan(self, scan_ids: list[str]):
        all_results = []
        for scan_id in scan_ids:
            self.wait_for_scan(scan_id)
            result_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}"
            response = handle_api_response(
                result_url, headers=self.headers, method="GET"
            )
            all_results.append(response.json())
        return all_results


class PowerBiReportParser:
    """Parses raw Power BI scan results into dataframe."""

    TABLE_NAMES: ClassVar[frozenset[str]] = frozenset(
        {
            "reports",
            "reports_owners",
            "datasource_instances",
            "dataflows",
            "dataflow_datasource_links",
            "dataset_datasource_links",
        }
    )

    def __init__(
        self,
        report_mapping: dict | None = None,
        workspace_mapping: dict | None = None,
        owner_mapping: dict | None = None,
        target_date: str | None = None,
    ) -> None:
        self.report_mapping = report_mapping or REPORT_FIELD_MAPPING
        self.workspace_mapping = workspace_mapping or WORKSPACE_FIELD_MAPPING
        self.owner_mapping = owner_mapping or OWNER_FIELD_MAPPING
        self.target_date = target_date or (
            datetime.now(timezone.utc) - timedelta(days=1)
        ).strftime("%Y-%m-%d")

    def get_nested(self, obj: dict, path: str, default=None):
        """Pulls data from nested dict by provided patch."""
        current = obj
        for key in path.split("."):
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return default
        return current if current is not None else default

    def parse_scan_results(
        self,
        scan_results: list[dict],
        report_mapping: dict = REPORT_FIELD_MAPPING,
        workspace_mapping: dict = WORKSPACE_FIELD_MAPPING,
        owner_mapping: dict = OWNER_FIELD_MAPPING,
        owner_access_right_value: str = OWNER_ACCESS_RIGHT_VALUE,
    ) -> tuple[list[dict], list[dict]]:

        reports: list[dict] = []
        reports_owners: list[dict] = []

        for result in scan_results:
            for ws in result.get("workspaces", []):
                ws_row = {
                    target: self.get_nested(ws, path)
                    for target, path in workspace_mapping.items()
                }

                for rep in ws.get("reports", []):
                    rep_row = {
                        target: self.get_nested(rep, path)
                        for target, path in report_mapping.items()
                    }
                    rep_row.update(ws_row)
                    rep_row["Target_date"] = self.target_date
                    reports.append(rep_row)
                    report_id = rep.get("id")
                    for user in rep.get("users", []):
                        if (
                            user.get("reportUserAccessRight")
                            == owner_access_right_value
                        ):
                            owner_row = {
                                target: self.get_nested(user, path)
                                for target, path in owner_mapping.items()
                            }
                            owner_row["Report_id"] = report_id
                            owner_row["Target_date"] = self.target_date
                            reports_owners.append(owner_row)

        return reports, reports_owners

    def parse_datasource_instances(self, scan_results: list[dict]) -> list:
        datasource_instances = []
        for result in scan_results:
            for ds_instance in result.get("datasourceInstances", []):
                instance_id = ds_instance.get("datasourceId")
                ds_type = ds_instance.get("datasourceType")
                conn_details = ds_instance.get("connectionDetails", {})

                details_str = " | ".join(f"{k}={v}" for k, v in conn_details.items())
                connection_desc = f"{details_str}" if details_str else ds_type

                datasource_instances.append(
                    {
                        "Connection_id": instance_id,
                        "Connection_desc": connection_desc,
                        "Connection_type": ds_type,
                    }
                )
        return datasource_instances

    def parse_dataflows(self, scan_results: list[dict]) -> list:
        dataflows = []
        for result in scan_results:
            for ws in result.get("workspaces", []):
                for df in ws.get("dataflows", []):
                    dataflows.append(
                        {
                            "Dataflow_id": df.get("objectId"),
                            "Dataflow_desc": df.get("name"),
                            "Workspace_id": ws.get("id"),
                            "Configured_by": df.get("configuredBy"),
                            "Modified_by": df.get("modifiedBy"),
                            "Modified_date": df.get("modifiedDateTime"),
                            "Target_date": self.target_date,
                        }
                    )
        return dataflows

    def parse_dataflow_datasource_links(self, scan_results: list[dict]) -> list[dict]:
        links = []
        for result in scan_results:
            for ws in result.get("workspaces", []):
                for df in ws.get("dataflows", []):
                    for usage in df.get("datasourceUsages") or []:
                        links.append(
                            {
                                "Dataflow_id": df.get("objectId"),
                                "Connection_id": usage.get("datasourceInstanceId"),
                            }
                        )
        return links

    def parse_dataset_datasource_links(self, scan_results: list[dict]) -> list[dict]:
        links = []
        for result in scan_results:
            for ws in result.get("workspaces", []):
                for dataset in ws.get("datasets", []):
                    for usage in dataset.get("datasourceUsages") or []:
                        links.append(
                            {
                                "Semantic_model_id": dataset.get("id"),
                                "Connection_id": usage.get("datasourceInstanceId"),
                            }
                        )
        return links

    def to_df(self, scan_results: list[dict]) -> dict[str, pd.DataFrame]:
        """Convenience method running all parsers in one go, returning DataFrames."""
        reports, reports_owners = self.parse_scan_results(scan_results)
        return {
            "reports": pd.DataFrame(reports),
            "reports_owners": pd.DataFrame(reports_owners),
            "datasource_instances": pd.DataFrame(
                self.parse_datasource_instances(scan_results)
            ),
            "dataflows": pd.DataFrame(self.parse_dataflows(scan_results)),
            "dataflow_datasource_links": pd.DataFrame(
                self.parse_dataflow_datasource_links(scan_results)
            ),
            "dataset_datasource_links": pd.DataFrame(
                self.parse_dataset_datasource_links(scan_results)
            ),
        }
