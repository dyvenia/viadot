"""Flows for downloading data from Sharepoint and uploading it to AWS Redshift Spectrum."""  # noqa: W505

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    sharepoint_to_df,
)
from viadot.orchestration.prefect.tasks.dbt import (
    trigger_downstream_nodes as trigger_downstream_nodes_task,
)
from viadot.orchestration.prefect.tasks.dbt import (
    update_node_state,
)
from viadot.orchestration.prefect.utils import get_credentials, with_flow_timeout_param


@flow(
    name="extract--sharepoint--redshift_spectrum",
    description="Extract data from Sharepoint and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
@with_flow_timeout_param()
def sharepoint_to_redshift_spectrum(  # noqa: PLR0913
    sharepoint_url: str,
    to_path: str,
    schema_name: str,
    table: str,
    tests: dict[str, Any] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    sep: str = ",",
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
    credentials_secret_cert_auth: str | None = None,
    credentials_secret_cert_password: str | None = None,
    sheet_name: str | list[str | int] | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    na_values: list[str] | None = None,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
    file_sheet_mapping: dict | None = None,
    manifest_path: str | None = None,
    artifact_store_type: str = "s3",
    artifact_store_credentials_secret: str | None = None,
    track_state: bool = False,
    state_path: str | None = None,
    state_store_type: str = "s3",
    state_store_credentials_secret: str | None = None,
    deployments_dir: str | None = None,
    sla_breach_grace_period_minutes: int = 30,
    trigger_downstream_nodes: bool = False,
    trigger_downstream_nodes_delay: int = 0,
) -> None:
    """Extract data from SharePoint and load it into AWS Redshift Spectrum.

    This function downloads data either from SharePoint file or the whole directory and
    uploads it to AWS Redshift Spectrum.

    Modes:
    If the `URL` ends with the file (e.g ../file.xlsx) it downloads only the file and
    creates a table from it.
    If the `URL` ends with the folder (e.g ../folder_name/): it downloads multiple files
    and creates a table from them:
        - If `file_sheet_mapping` is provided, it downloads and processes only
            the specified files and sheets.
        - If `file_sheet_mapping` is NOT provided, it downloads and processes all of
            the files from the chosen folder.


    Args:
        sharepoint_url (str): The URL to the file.
        to_path (str): Path to a S3 folder where the table will be located. Defaults to
            None.
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
        sep (str, optional): Field delimiter for the output file. Defaults to ','.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        credentials_secret_cert_auth (str, optional): The name of the secret storing
            the credentials for certificate authentication. Defaults to None.
        credentials_secret_cert_password (str, optional): The name of the secret
            storing the password for the certificate file. Defaults to None.
        sheet_name (str | list | int, optional): Strings are used for sheet names.
            Integers are used in zero-indexed sheet positions (chart sheets do not count
            as a sheet position). Lists of strings/integers are used to request multiple
            sheets. Specify None to get all worksheets. Defaults to None.
        columns (str | list[str] | list[int], optional): Which columns to ingest.
            Defaults to None.
        na_values (list[str] | None): Additional strings to recognize as NA/NaN.
            If list passed, the specific NA values for each column will be recognized.
            Defaults to None.
        sharepoint_credentials_secret (str, optional): The name of the secret storing
            Sharepoint credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        sharepoint_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        file_sheet_mapping (dict): A dictionary where keys are filenames and values are
            the sheet names to be loaded from each file. If provided, only these files
            and sheets will be downloaded. Defaults to None.
        manifest_path (str, optional): URI of the manifest file
            (e.g. ``"s3://bucket/manifest.json"``). Required if ``state_path`` is
            provided. Defaults to None.
        artifact_store_type (str, optional): Backend type for the artifact store.
            Defaults to "s3".
        artifact_store_credentials_secret (str, optional): Prefect secret name holding
            artifact store credentials. Omit to use ambient AWS credentials.
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
            ``prefect/deployments``.
        sla_breach_grace_period_minutes (int, optional): Grace period in minutes before
            an SLA breach is triggered. Defaults to 30.
        trigger_downstream_nodes (bool, optional): Whether to trigger downstream nodes
            after a successful run. Defaults to False.
        trigger_downstream_nodes_delay (int, optional): Delay in minutes before
            triggering downstream nodes. Defaults to 0.
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
        "artifact_store_type": artifact_store_type,
        "artifact_store_credentials": get_credentials(artifact_store_credentials_secret)
        if artifact_store_credentials_secret
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
        df = sharepoint_to_df(
            url=sharepoint_url,
            sheet_name=sheet_name,
            tests=tests,
            columns=columns,
            na_values=na_values,
            file_sheet_mapping=file_sheet_mapping,
            credentials_secret_basic_auth=sharepoint_credentials_secret,
            credentials_secret_cert_auth=credentials_secret_cert_auth,
            credentials_secret_cert_password=credentials_secret_cert_password,
            config_key=sharepoint_config_key,
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
            sep=sep,
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
