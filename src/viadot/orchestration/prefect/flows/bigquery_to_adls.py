"""'bigquery_to_adls.py'."""

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import bigquery_to_df, df_to_adls


@flow(
    name="BigQuery extraction to ADLS",
    description="Extract data from BigQuery and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def bigquery_to_adls(  # noqa: PLR0913
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    query: str | None = None,
    dataset_name: str | None = None,
    table_name: str | None = None,
    date_column_name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    """Download data from BigQuery to Azure Data Lake.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        query (str, optional): SQL query to querying data in BigQuery. Format of basic
            query: (SELECT * FROM `{project}.{dataset_name}.{table_name}`).
            Defaults to None.
        dataset_name (str, optional): Dataset name. Defaults to None.
        table_name (str, optional): Table name. Defaults to None.
        dataset_name (str, optional): Dataset name. Defaults to None.
        date_column_name (str, optional): The user can provide the name of the date
            column. If the user-specified column does not exist, all data will be
            retrieved from the table. Defaults to None.
        start_date (str, optional): Parameter to pass start date e.g.
            "2022-01-01". Defaults to None.
        end_date (str, optional): Parameter to pass end date e.g.
            "2022-01-01". Defaults to None.
        columns (list[str], optional): List of columns from given table name.
            Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        adls_azure_key_vault_secret (str, optional): The name of the Azure Key Vault
            secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (str, optional): Azure Data Lake destination file path.
            Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    data_frame = bigquery_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        query=query,
        dataset_name=dataset_name,
        table_name=table_name,
        date_column_name=date_column_name,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
