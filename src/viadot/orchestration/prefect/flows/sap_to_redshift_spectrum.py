"""Flows for downloading data from SAP and uploading it to AWS Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, sap_rfc_to_df
from viadot.orchestration.prefect.tasks.dbt import (
    trigger_downstream_nodes as trigger_downstream_nodes_task,
)
from viadot.orchestration.prefect.tasks.dbt import (
    update_node_state,
)
from viadot.orchestration.prefect.utils import get_credentials, with_flow_timeout_param


@flow(
    name="extract--sap--redshift_spectrum",
    description="Extract data from SAP and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
@with_flow_timeout_param()
def sap_to_redshift_spectrum(  # noqa: PLR0913
    to_path: str,
    schema_name: str,
    table: str,
    tests: dict[str, Any] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    aws_sep: str = ",",
    dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
    dynamic_date_format: str = "%Y%m%d",
    dynamic_date_timezone: str = "UTC",
    credentials_secret: str | None = None,
    aws_config_key: str | None = None,
    query: str | None = None,
    func: str | None = None,
    rfc_total_col_width_character_limit: int = 400,
    rfc_unique_id: list[str] | None = None,
    sap_credentials_secret: str | None = None,
    sap_config_key: str | None = None,
    sap_sep: str | None = "♔",
    replacement: str = "-",
    manifest_path: str | None = None,
    manifest_store_type: str = "s3",
    manifest_store_credentials_secret: str | None = None,
    track_state: bool = False,
    state_path: str | None = None,
    state_store_type: str = "s3",
    state_store_credentials_secret: str | None = None,
    deployments_dir: str | None = None,
    sla_breach_grace_period_minutes: int = 30,
    trigger_downstream_nodes: bool = False,
    trigger_downstream_nodes_delay: int = 0,
) -> None:
    """Download a pandas `DataFrame` from SAP and upload it to AWS Redshift Spectrum.

    Args:
        to_path (str): Path to a S3 folder where the table will be located.
            Defaults to None.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        tests (dict[str], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from viadot.utils. Defaults to None.
        partition_cols (list[str]): List of column names that will be used to create
            partitions. Only takes effect if dataset=True.
        extension (str): Required file type. Accepted file formats are 'csv' and
            'parquet'.
        if_exists (str, optional): 'overwrite' to recreate any possible existing table
            or 'append' to keep any possible existing table. Defaults to overwrite.
        partition_cols (list[str], optional): List of column names that will be used to
            create partitions. Only takes effect if dataset=True. Defaults to None.
        index (bool, optional): Write row names (index). Defaults to False.
        compression (str, optional): Compression style (None, snappy, gzip, zstd).
        aws_sep (str, optional): Field delimiter for the output file. Defaults to ','.
        dynamic_date_symbols (list[str], optional): Symbols used for dynamic date
            handling. Defaults to ["<<", ">>"].
        dynamic_date_format (str, optional): Format used for dynamic date parsing.
            Defaults to "%Y%m%d".
        dynamic_date_timezone (str, optional): Timezone used for dynamic date
            processing. Defaults to "UTC".
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        query (str): The query to be executed with pyRFC.
        func (str, optional): SAP RFC function to use. Defaults to None.
        rfc_total_col_width_character_limit (int, optional): Number of characters by
            which query will be split in chunks in case of too many columns for RFC
            function. According to SAP documentation, the limit is 512 characters.
            However, we observed SAP raising an exception even on a slightly lower
            number of characters, so we add a safety margin. Defaults to 400.
        rfc_unique_id  (list[str], optional): Reference columns to merge chunks Data
            Frames. These columns must to be unique. If no columns are provided, all
                data frame columns will by concatenated. Defaults to None.
        sap_credentials_secret (str, optional): The name of the AWS secret that stores
            SAP credentials. Defaults to None.
        sap_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        replacement (str, optional): In case of sep is on a columns, set up a new
            character to replace inside the string to avoid flow breakdowns.
            Defaults to "-".
        sap_sep (str, optional): The separator to use when reading query results.
            If set to None, multiple options are automatically tried.
            Defaults to ♔.
        manifest_path (str, optional): URI of the manifest file
            (e.g. ``"s3://bucket/manifest.json"``). Required if ``state_path`` is
            provided. Defaults to None.
        manifest_store_type (str, optional): Backend type for the manifest store.
            Defaults to "s3".
        manifest_store_credentials_secret (str, optional): Prefect secret name holding
            manifest store credentials. Omit to use ambient AWS credentials.
            Defaults to None.
        track_state (bool): Whether to track the state of the dbt node in a state file.
        state_path (str, optional): URI of the state file
            (e.g. ``"s3://bucket/state.json"``). If provided, the flow writes
            ``running``, then ``success`` or ``failed`` to the state store.
            Defaults to None.
        state_store_type (str, optional): Backend type for the state store.
            Defaults to "s3".
        state_store_credentials_secret (str, optional): Prefect secret name holding
            state store credentials. Omit to use ambient AWS credentials.
            Defaults to None.
        deployments_dir (str | Path, optional): Directory containing Prefect deployment
            YAML files, used to retrieve the schedules in case the node is a source
            node. If not provided, defaults to
            ``<this_file's_parent>/../../deployments``.
        sla_breach_grace_period_minutes (int, optional): Grace period in minutes before
            an SLA breach is triggered. Defaults to 30.
        trigger_downstream_nodes (bool, optional): Whether to trigger downstream nodes
            after a successful run. Defaults to False.
        trigger_downstream_nodes_delay (int, optional): Delay in minutes before
            triggering downstream nodes. Defaults to 0.


    Examples:
        sap_to_redshift_spectrum(
            ...
            rfc_unique_id=["VBELN", "LPRIO"],
            ...
        )
    """
    if trigger_downstream_nodes and not track_state:
        msg = "State tracking must be enabled to trigger downstream nodes."
        raise ValueError(msg)

    state_update_params = {
        "node_name": table,
        "node_type": "source",
        "state_path": state_path,
        "state_store_type": state_store_type,
        "state_store_credentials": get_credentials(state_store_credentials_secret)
        if state_store_credentials_secret
        else None,
        "manifest_path": manifest_path,
        "manifest_store_type": manifest_store_type,
        "manifest_store_credentials": get_credentials(manifest_store_credentials_secret)
        if manifest_store_credentials_secret
        else None,
        "deployments_dir": deployments_dir,
        "trigger_delay": trigger_downstream_nodes_delay,
        "sla_breach_grace_period_minutes": sla_breach_grace_period_minutes,
    }

    if track_state:
        update_node_state(**state_update_params, status="running")

    _node_status = "failed"
    _manifest = None
    try:
        df = sap_rfc_to_df(
            query=query,
            tests=tests,
            func=func,
            rfc_unique_id=rfc_unique_id,
            rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
            credentials_secret=sap_credentials_secret,
            config_key=sap_config_key,
            dynamic_date_symbols=dynamic_date_symbols,
            dynamic_date_format=dynamic_date_format,
            dynamic_date_timezone=dynamic_date_timezone,
            replacement=replacement,
            sep=sap_sep,
        )

        df_to_redshift_spectrum(
            df=df,
            to_path=to_path,
            schema_name=schema_name,
            table=table,
            extension=extension,
            if_exists=if_exists,
            partition_cols=partition_cols,
            index=index,
            compression=compression,
            sep=aws_sep,
            config_key=aws_config_key,
            credentials_secret=credentials_secret,
        )
        _node_status = "success"
    finally:
        if track_state:
            _manifest = update_node_state(**state_update_params, status=_node_status)

    if trigger_downstream_nodes:
        trigger_downstream_nodes_task(
            node_name=table,
            manifest=_manifest,
            state_path=state_path,
            state_store_credentials=state_update_params["state_store_credentials"],
        )
