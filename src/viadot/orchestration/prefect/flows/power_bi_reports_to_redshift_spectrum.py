from typing import Literal

from prefect import flow
from prefect.logging import get_run_logger

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    fetch_scan_results,
    get_modified_workspace_ids,
    get_scan_ids,
    parse_scan_results,
)
from viadot.sources import PowerBiReportParser, PowerBiReportScanner


@flow(name="powerbi-report-scan", log_prints=True)
def power_bi_scan_reports(
    config_key: str | None = None,
    target_date: str | None = None,
    to_path: str | None = None,
    schema_name: str | None = None,
    table_mapping: dict[str, str] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append", "skip"] = "append",
    if_exists_mapping: dict[str, Literal["overwrite", "append", "skip"]] | None = None,
    partition_cols: list[str] | None = None,
    compression: str | None = None,
    aws_sep: str = ",",
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
) -> None:
    """Scan modified Power BI workspaces and load the results into Redshift Spectrum.

    Fetches metadata (reports, owners, datasource instances, dataflows, and their
    links) for workspaces modified since `target_date` via the Power BI Admin Scan
    API, parses the scan results into DataFrames, and loads each non-empty
    DataFrame into Redshift Spectrum.

    Args:
        config_key: The key in the viadot config holding the Power BI credentials.
            Defaults to None.
        target_date: The date (YYYY-MM-DD) used to filter modified workspaces and
            tag the parsed records. Defaults to yesterday (UTC) if not provided.
        to_path: The base S3 path under which each table will be written, as
            `{to_path}/{table}`. Defaults to None.
        schema_name: The Redshift Spectrum schema to load the tables into.
            Defaults to None.
        table_mapping: A mapping from internal report names (e.g. "reports",
            "dataflows") to custom target table names. Unrecognized keys are
            ignored with a warning; missing keys fall back to the default table
            names. Defaults to None.
        extension: The file extension/format to use when writing to S3.
            Defaults to ".parquet".
        if_exists: The default behavior when the target table already exists.
            One of "overwrite", "append", or "skip". Defaults to "append".
        if_exists_mapping: A mapping from internal report names to a
            table-specific `if_exists` behavior, overriding the global
            `if_exists` default for that table. Defaults to None.
        partition_cols: The columns to partition the output data by.
            Defaults to None.
        compression: The compression codec to use when writing the output files.
            Defaults to None.
        aws_sep: The field separator to use when writing CSV output. Defaults to ",".
        aws_config_key: The key in the viadot config holding the AWS credentials.
            Defaults to None.
        credentials_secret: The name of the secret holding the AWS credentials,
            used as an alternative to `aws_config_key`. Defaults to None.

    Returns:
        None.
    """
    logger = get_run_logger()

    if table_mapping is None:
        table_mapping = {
            "reports": "powerbi_reports",
            "reports_owners": "powerbi_report_owners",
            "datasource_instances": "powerbi_connections",
            "dataflows": "powerbi_dataflows",
            "dataflow_datasource_links": "powerbi_dataflow_links",
            "dataset_datasource_links": "powerbi_dataset_links",
        }

    if if_exists_mapping is None:
        if_exists_mapping = {
            "reports": "append",
            "reports_owners": "append",
            "datasource_instances": "append",
            "dataflows": "append",
            "dataflow_datasource_links": "overwrite",
            "dataset_datasource_links": "overwrite",
        }

    unknown_keys = set(table_mapping) - PowerBiReportParser.TABLE_NAMES
    if unknown_keys:
        logger.warning(
            f"table_mapping contains unknown keys, they will be ignored: {sorted(unknown_keys)}"
        )

    scanner = PowerBiReportScanner(config_key=config_key, logger=logger)  # type: ignore

    workspace_ids = get_modified_workspace_ids(scanner, target_date)

    if not workspace_ids:
        logger.info("No modified workspaces found.")
        return

    scan_ids = get_scan_ids(scanner, workspace_ids)
    scan_results = fetch_scan_results(scanner, scan_ids)
    dataframes = parse_scan_results(scan_results, target_date)
    for report_name, df in dataframes.items():
        table = table_mapping[report_name]
        table_if_exists = if_exists_mapping.get(report_name, if_exists)
        if df.empty:
            logger.info(f"Skipping, empty DataFrame: {table}")
            continue
        logger.info(f"Loading {table} into Redshift Spectrum.")
        df_to_redshift_spectrum(
            df=df,
            to_path=f"{to_path}/{table}",
            schema_name=schema_name,
            table=table,
            extension=extension,
            if_exists=table_if_exists,
            partition_cols=partition_cols,
            compression=compression,
            sep=aws_sep,
            config_key=aws_config_key,
            credentials_secret=credentials_secret,
        )
