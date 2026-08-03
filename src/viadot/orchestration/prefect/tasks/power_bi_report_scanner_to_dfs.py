"""Power bi report scanner to Pandas dataframes tasks."""

import pandas as pd
from prefect import task

from viadot.sources.power_bi import PowerBiReportParser, PowerBiReportScanner


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def get_modified_workspace_ids(
    scanner: PowerBiReportScanner, target_date: str | None = None
) -> list[str]:
    """Get the IDs of workspaces modified since `target_date`."""
    return scanner.get_modified_workspaces(target_date)


@task(retries=2, retry_delay_seconds=30)
def get_scan_ids(scanner: PowerBiReportScanner, workspace_ids: list[str]) -> list[str]:
    """Get the scan IDs for the given workspace IDs."""
    return scanner.get_workspaces_info(workspace_ids)


@task(retries=1)
def fetch_scan_results(
    scanner: PowerBiReportScanner, scan_ids: list[str]
) -> list[dict]:
    """Fetch the results for the given scan IDs."""
    return scanner.fetch_report_scan(scan_ids)


@task
def parse_scan_results(
    scan_results: list[dict], target_date: str | None = None
) -> dict[str, pd.DataFrame]:
    """Parse the scan results into a dictionary of DataFrames."""
    parser = PowerBiReportParser(target_date=target_date)
    return parser.to_df(scan_results)
