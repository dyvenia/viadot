"""Power BI API sources for Viadot."""

from abc import ABC, abstractmethod
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
import json
import logging
import time
from typing import Any, ClassVar, Literal

import pandas as pd
from pandas.core.api import DataFrame
from pydantic import BaseModel
import requests

from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.base import Source
from viadot.utils import handle_api_response


logging.basicConfig(level=logging.INFO)

HTTP_TOO_MANY_REQUESTS = 429


class PowerBICredentials(BaseModel):
    """PowerBI API credentials."""

    tenant_id: str
    client_id: str
    client_secret: str


class PowerBiAuth:
    """Handle authentication for the Power BI API.

    Manages retrieval and storage of credentials required to obtain
    an access token for Power BI.
    """

    def __init__(
        self,
        credentials: PowerBICredentials | None = None,
        credential_secret: str | None = None,
    ) -> None:
        """Initialize the PowerBiAuth instance.

        Args:
            credentials (PowerBICredentials, optional): Power BI credentials
                to use for authentication. If not provided, credentials will
                be retrieved using `credential_secret`. Defaults to None.
            credential_secret (str, optional): The name of the secret to use
                for retrieving credentials, if `credentials` is not provided.
                Defaults to None.
        """
        raw_creds = credentials or get_credentials(credential_secret)
        if isinstance(raw_creds, PowerBICredentials):
            self.credentials = dict(raw_creds)
        else:
            self.credentials = dict(PowerBICredentials(**raw_creds))

        self._token: str | None = None

    def _get_token(self) -> str | None:
        """Authenticate against Azure AD and cache the token for reuse.

        If a token has already been retrieved, it is returned from cache
        instead of requesting a new one.

        Returns:
            str | None: The access token used for authenticating requests
                to the Power BI API.
        """
        if self._token:
            return self._token

        url = f"https://login.microsoftonline.com/{self.credentials['tenant_id']}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.credentials["client_id"],
            "client_secret": self.credentials["client_secret"],
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        }
        response = handle_api_response(url, method="POST", data=data)  # type: ignore
        self._token = response.json()["access_token"]
        return self._token

    @property
    def headers(self) -> dict[str, str]:
        """Build the authorization headers for Power BI API requests.

        Returns:
            dict[str, str]: A dictionary containing the `Authorization`
                header with a valid Bearer token.
        """
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
        columns_to_extract: list[str] | None = None,
        **kwargs: str | int | bool,
    ) -> None:
        """Initialize the PowerBIActivityEvents source.

        Args:
            *args: Positional arguments passed to the parent classes.
            credentials (PowerBICredentials, optional): Power BI credentials
                to use for authentication. Defaults to None.
            columns_to_extract (list[str], optional): List of columns to
                extract from the activity events response. Defaults to None.
            **kwargs (str | int | bool): Keyword arguments passed to the
                parent classes.
        """
        super().__init__(*args, credentials=credentials, **kwargs)
        self.columns_to_extract = columns_to_extract

    @staticmethod
    def build_url(date: str) -> str:
        """Build the Power BI Admin Activity Events API URL for a given day.

        Args:
            date (str): The date, in `YYYY-MM-DD` format, for which to
                retrieve activity events.

        Returns:
            str: The full URL for querying activity events for the given
                date, spanning from 00:00:00.000 to 23:59:59.999.
        """
        return (
            "https://api.powerbi.com/v1.0/myorg/admin/activityevents"
            f"?startDateTime='{date}T00:00:00.000'"
            f"&endDateTime='{date}T23:59:59.999'"
        )

    def query(self, date: str) -> list[dict]:
        """Fetch all activity events for a single UTC day, handling pagination.

        Args:
            date (str): date string 'YYYY-MM-DD' (UTC day to extract).

        Returns:
            list[dict]: raw activity event records.
        """
        url = self.build_url(date)
        records: list[dict] = []

        while url:
            response = requests.get(url, headers=self.headers, timeout=60)
            if response.status_code == HTTP_TOO_MANY_REQUESTS:
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

        Returns:
            pd.DataFrame: Dataframe with processed events.
        """
        if date is None:
            date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        records = self.query(date)
        if not records:
            self._handle_if_empty(
                if_empty=if_empty, message=f"No activity events found for {date}."
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
WORKSPACE_OWNER_ACCESS_RIGHT_VALUE = "Admin"

DEFAULT_GET_INFO_QUERY_PARAMS: dict[str, bool] = {
    "lineage": True,
    "datasourceDetails": True,
    "datasetSchema": True,
    "datasetExpressions": True,
    "getArtifactUsers": True,
}


class PowerBiReportParser(ABC):
    """Abstract interface for parsing raw Power BI scan results."""

    @abstractmethod
    def parse_scan_results(
        self, scan_results: list[dict]
    ) -> tuple[list[dict], list[dict]]:
        """Parse reports and report owners out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            tuple[list[dict], list[dict]]: A tuple of `(reports,
                reports_owners)`.
        """
        ...

    @abstractmethod
    def parse_datasource_instances(self, scan_results: list[dict]) -> list[dict]:
        """Parse datasource instances out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of datasource instance records.
        """
        ...

    @abstractmethod
    def parse_dataflows(self, scan_results: list[dict]) -> list[dict]:
        """Parse dataflows out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of dataflow records.
        """
        ...

    @abstractmethod
    def parse_dataflow_datasource_links(self, scan_results: list[dict]) -> list[dict]:
        """Parse dataflow-to-datasource links out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of link records mapping dataflows to
                datasource instances.
        """
        ...

    @abstractmethod
    def parse_dataset_datasource_links(self, scan_results: list[dict]) -> list[dict]:
        """Parse dataset-to-datasource links out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of link records mapping datasets to
                datasource instances.
        """
        ...


class PowerBiDefaultReportParser(PowerBiReportParser):
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
        """Initialize the PowerBiReportParser.

        Args:
            report_mapping (dict, optional): Mapping of target column names
                to nested paths within a report object. Defaults to
                `REPORT_FIELD_MAPPING`.
            workspace_mapping (dict, optional): Mapping of target column
                names to nested paths within a workspace object. Defaults
                to `WORKSPACE_FIELD_MAPPING`.
            owner_mapping (dict, optional): Mapping of target column names
                to nested paths within a report user object. Defaults to
                `OWNER_FIELD_MAPPING`.
            target_date (str, optional): The date, in `YYYY-MM-DD` format,
                to stamp parsed records with. Defaults to yesterday's date
                (UTC).
        """
        self.report_mapping = report_mapping or REPORT_FIELD_MAPPING
        self.workspace_mapping = workspace_mapping or WORKSPACE_FIELD_MAPPING
        self.owner_mapping = owner_mapping or OWNER_FIELD_MAPPING
        self.target_date = target_date or (
            datetime.now(timezone.utc) - timedelta(days=1)
        ).strftime("%Y-%m-%d")

    def get_nested(
        self,
        obj: dict[str, Any],
        path: str,
        default: Any | None = None,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        """Pulls data from nested dict by provided patch.

        Args:
            obj (dict): The dictionary to traverse.
            path (str): A dot-separated path of keys, e.g. `"a.b.c"`.
            default (Any, optional): The value to return if the path
                cannot be resolved. Defaults to None.

        Returns:
            Any: The value found at `path`, or `default` if any key in
                the path is missing or an intermediate value is not a
                dict.
        """
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
        owner_access_right_value: str = OWNER_ACCESS_RIGHT_VALUE,
    ) -> tuple[list[dict], list[dict]]:
        """Parse reports and report owners out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.
            owner_access_right_value (str, optional): The value of
                `reportUserAccessRight` that identifies a user as the
                report's owner. Defaults to `OWNER_ACCESS_RIGHT_VALUE`.

        Returns:
            tuple[list[dict], list[dict]]: A tuple of `(reports,
                reports_owners)`, where `reports` is a list of flattened
                report records and `reports_owners` is a list of report
                owner records.
        """
        reports: list[dict] = []
        reports_owners: list[dict] = []

        for result in scan_results:
            for ws in result.get("workspaces", []):
                ws_row = {
                    target: self.get_nested(ws, path)
                    for target, path in self.workspace_mapping.items()
                }

                for rep in ws.get("reports", []):
                    rep_row = {
                        target: self.get_nested(rep, path)
                        for target, path in self.report_mapping.items()
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
                                for target, path in self.owner_mapping.items()
                            }
                            owner_row["Report_id"] = report_id
                            owner_row["Target_date"] = self.target_date
                            reports_owners.append(owner_row)

        return reports, reports_owners

    def parse_datasource_instances(self, scan_results: list[dict]) -> list[dict]:
        """Parse datasource instances out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of datasource instance records, each with
                `Connection_id`, `Connection_desc`, and `Connection_type`.
        """
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

    def parse_dataflows(self, scan_results: list[dict]) -> list[dict]:
        """Parse dataflows out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of dataflow records, each with
                `Dataflow_id`, `Dataflow_desc`, `Workspace_id`,
                `Configured_by`, `Modified_by`, `Modified_date`, and
                `Target_date`.
        """
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
        """Parse dataflow-to-datasource links out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of link records, each with `Dataflow_id`
                and `Connection_id`, mapping dataflows to the datasource
                instances they use.
        """
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

    def parse_dataset_datasource_links(
        self,
        scan_results: list[dict],
        workspace_owner_access_right_value: str = WORKSPACE_OWNER_ACCESS_RIGHT_VALUE,
    ) -> list[dict]:
        """Parse dataset-to-datasource links out of raw scan results.

        Args:
            scan_results (list[dict]): The raw scan result payloads to
                parse.

        Returns:
            list[dict]: A list of link records, each with
                `Semantic_model_id` and `Connection_id`, mapping datasets
                to the datasource instances they use.
        """
        links = []
        for result in scan_results:
            for ws in result.get("workspaces", []):
                workspace_owners = [
                    user.get("displayName") or user.get("emailAddress")
                    for user in ws.get("users", [])
                    if user.get("groupUserAccessRight")
                    == workspace_owner_access_right_value
                ]
                for dataset in ws.get("datasets", []):
                    for usage in dataset.get("datasourceUsages") or []:
                        links.append(
                            {
                                "Semantic_model_id": dataset.get("id"),
                                "Dataset_desc": dataset.get("name"),
                                "Workspace_id": ws.get("id"),
                                "Workspace_desc": ws.get("name"),
                                "Workspace_owners": ", ".join(workspace_owners),
                                "Connection_id": usage.get("datasourceInstanceId"),
                            }
                        )
        return links


class PowerBiWorkspaceInfo(PowerBiAuth, Source):
    """Power BI Admin - Report Scanner source.

    Scans Power BI workspaces using the Admin Metadata Scanning API:
    detects recently modified workspaces, triggers scans for their
    metadata, waits for the scans to complete, and retrieves the
    resulting scan data.

    Args:
        credentials: credentials dict, alternative to config_key.
        credential_secret: key in local viadot config with tenant_id/
            client_id/client_secret.
        get_info_query_params (dict[str, bool], optional): Query parameters
            passed to the workspace `getInfo` endpoint (e.g. flags to
            include datasets, dataflows, etc.). Defaults to
            `DEFAULT_GET_INFO_QUERY_PARAMS`.
        logger (logging.Logger, optional): Logger instance to use.
            Defaults to a module-level logger.
    """

    def __init__(
        self,
        *args,
        credentials: PowerBICredentials | None = None,
        credential_secret: str | None = None,
        get_info_query_params: dict[str, bool] | None = None,
        logger: logging.Logger | None = None,
        base_url: str | None = None,
        parser: PowerBiReportParser | None = None,
        target_date: str | None = None,
        **kwargs: str | int | bool,
    ) -> None:
        """Initialize the PowerBiWorkspaceInfo source.

        Args:
            *args: Positional arguments passed to the parent classes.
            credentials (PowerBICredentials, optional): Power BI credentials
                to use for authentication. Defaults to None.
            credential_secret (str, optional): The name of the secret to use
                for retrieving credentials, if `credentials` is not provided.
                Defaults to None.
            get_info_query_params (dict[str, bool], optional): Query
                parameters passed to the workspace `getInfo` endpoint.
                Defaults to `DEFAULT_GET_INFO_QUERY_PARAMS`.
            logger (logging.Logger, optional): Logger instance to use for
                logging scan progress. Defaults to a module-level logger.
            **kwargs (str | int | bool): Keyword arguments passed to the
                parent classes.
            base_url (str, optional): Base URL for the Power BI API. Defaults to None.
            parser (PowerBiReportParser, optional): Parser instance to use for
                processing scan results. Defaults to None.
            target_date (str, optional): The date, in `YYYY-MM-DD` format,
                since which to look for modified workspaces. Defaults to None,
                in which case yesterday's date (UTC) is used.
        """
        self.target_date = target_date
        self.get_info_query_params = (
            get_info_query_params or DEFAULT_GET_INFO_QUERY_PARAMS
        )

        super().__init__(
            *args,
            credentials=credentials,
            credential_secret=credential_secret,
            **kwargs,
        )
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("PowerBiWorkspaceInfo initialized.")
        self.base_url = (
            base_url or "https://api.powerbi.com/v1.0/myorg/admin/workspaces"
        )
        self.parser: PowerBiReportParser = parser or PowerBiDefaultReportParser()

    def get_modified_workspaces(self, target_date: str | None = None) -> list[str]:
        """Retrieve IDs of workspaces modified since the given date.

        Args:
            target_date (str, optional): The date, in `YYYY-MM-DD` format,
                since which to look for modified workspaces. Defaults to
                None, in which case yesterday's date (UTC) is used.

        Raises:
            ValueError: If `target_date` is not in `YYYY-MM-DD` format.

        Returns:
            list[str]: A list of IDs of workspaces modified since
                `target_date`, excluding personal and inactive workspaces.
        """
        endpoint = "workspaces/modified"
        url = f"{self.base_url}/{endpoint}"
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
        """Split a list into chunks of a given size.

        Args:
            input_list (list): The list to split into chunks.
            size (int, optional): The maximum size of each chunk. Defaults
                to 100.

        Yields:
            list: Successive chunks of `input_list`, each of length at
                most `size`.
        """
        for i in range(0, len(input_list), size):
            yield input_list[i : i + size]

    def get_workspaces_info(self, workspace_ids: list[str]) -> list[str]:
        """Trigger metadata scans for the given workspaces.

        Workspace IDs are submitted to the Admin `getInfo` endpoint in
        chunks (to respect API limits), and a scan ID is returned for
        each submitted chunk.

        Args:
            workspace_ids (list[str]): The IDs of the workspaces to scan.

        Returns:
            list[str]: A list of scan IDs, one per submitted chunk of
                workspace IDs.
        """
        scan_ids: list[str] = []
        for chunk in self.chunk_list(workspace_ids):
            url = f"{self.base_url}/getInfo"
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
        """Poll the scan status endpoint until a scan finishes or times out.

        Args:
            scan_id (str): The ID of the scan to wait for.
            timeout (int, optional): The maximum time, in seconds, to wait
                for the scan to complete. Defaults to 300.
            interval (int, optional): The time, in seconds, to wait between
                consecutive status checks. Defaults to 15.

        Raises:
            TimeoutError: If the scan does not complete within `timeout`
                seconds.

        Returns:
            bool: True if the scan succeeded, False if the scan failed.
        """
        status_url = f"{self.base_url}/scanStatus/{scan_id}"
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
        msg = f"Scan {scan_id} timed out"
        raise TimeoutError(msg)

    def fetch_report_scan(self, scan_ids: list[str]) -> list[dict]:
        """Wait for scans to complete and fetch their results.

        For each scan ID, waits until the scan finishes (via
        `wait_for_scan`) and then retrieves the corresponding scan
        result.

        Args:
            scan_ids (list[str]): The IDs of the scans to fetch results
                for.

        Returns:
            list[dict]: A list of scan result payloads, one per scan ID,
                in the same order as `scan_ids`.
        """
        all_results = []
        for scan_id in scan_ids:
            self.wait_for_scan(scan_id)
            result_url = f"{self.base_url}/scanResult/{scan_id}"
            response = handle_api_response(
                result_url, headers=self.headers, method="GET"
            )
            all_results.append(response.json())
        return all_results

    def to_dict(self) -> dict[str, pd.DataFrame]:
        """Convenience method running all parsers in one go, returning DataFrames.

        Returns:
            dict[str, pd.DataFrame]: A dictionary mapping each table name
                in `TABLE_NAMES` to its corresponding parsed DataFrame
                (`reports`, `reports_owners`, `datasource_instances`,
                `dataflows`, `dataflow_datasource_links`, and
                `dataset_datasource_links`).

        """
        workspace_ids = self.get_modified_workspaces(self.target_date)
        scan_ids = self.get_workspaces_info(workspace_ids)
        scan_results = self.fetch_report_scan(scan_ids)
        reports, reports_owners = self.parser.parse_scan_results(scan_results)
        return {
            "reports": pd.DataFrame(reports),
            "reports_owners": pd.DataFrame(reports_owners),
            "datasource_instances": pd.DataFrame(
                self.parser.parse_datasource_instances(scan_results)
            ),
            "dataflows": pd.DataFrame(self.parser.parse_dataflows(scan_results)),
            "dataflow_datasource_links": pd.DataFrame(
                self.parser.parse_dataflow_datasource_links(scan_results)
            ),
            "dataset_datasource_links": pd.DataFrame(
                self.parser.parse_dataset_datasource_links(scan_results)
            ),
        }
