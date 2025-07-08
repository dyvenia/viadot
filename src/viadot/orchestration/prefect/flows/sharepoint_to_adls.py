"""Flows for pulling data from/into Sharepoint."""

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_adls, sharepoint_to_df


@flow(
    name="extract--sharepoint--adls",
    description="Extract data from Exchange Rates API and load it into Azure Data Lake.",
    retries=1,
    retry_delay_seconds=60,
)
def sharepoint_to_adls(
    sharepoint_url: str,
    adls_path: str,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
    adls_credentials_secret: str | None = None,
    adls_config_key: str | None = None,
    sheet_name: str | list[str | int] | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    overwrite: bool = False,
) -> None:
    """Download a file from Sharepoint and upload it to Azure Data Lake.

    Args:
        sharepoint_url (str): The URL to the file.
        adls_path (str): The destination path.
        sharepoint_credentials_secret (str, optional): The name of the Azure Key Vault
            secret storing the Sharepoint credentials. Defaults to None.
        sharepoint_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the ADLS credentials. Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        sheet_name (str | list | int, optional): Strings are used
            for sheet names. Integers are used in zero-indexed sheet positions
            (chart sheets do not count as a sheet position). Lists of strings/integers
            are used to request multiple sheets. Specify None to get all worksheets.
            Defaults to None.
        columns (str | list[str] | list[int], optional): Which columns to ingest.
            Defaults to None.
        overwrite (bool, optional): Whether to overwrite files in the lake. Defaults
            to False.
    """
    df = sharepoint_to_df(
        url=sharepoint_url,
        credentials_secret=sharepoint_credentials_secret,
        config_key=sharepoint_config_key,
        sheet_name=sheet_name,
        columns=columns,
    )
    return df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite,
    )
