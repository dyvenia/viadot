"""Flows for downloading data from Sharepoint and uploading it to Amazon S3."""

from prefect import flow

from viadot.orchestration.prefect.tasks import s3_upload_file, sharepoint_download_file


@flow(
    name="extract--sharepoint--s3",
    description="Flows for downloading data from Sharepoint and uploading it to Amazon S3.",
    retries=1,
    retry_delay_seconds=60,
)
def sharepoint_to_s3(
    url: str,
    local_path: str,
    to_path: str,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
    aws_config_key: str | None = None,
) -> None:
    """Download a file from Sharepoint and upload it to S3.

    Args:
        url (str): The URL of the file to be downloaded.
        local_path (str): Local file directory. Defaults to None.
        to_path (str): Where to download the file.
        sharepoint_credentials_secret (str, optional): The name of the secret that
            stores Sharepoint credentials. Defaults to None. More info on:
            https://docs.prefect.io/concepts/blocks/
        sharepoint_config_key (str, optional): The key in the viadot config holding
            relevant credentials.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    sharepoint_download_file(
        url=url,
        to_path=local_path,
        credentials_secret=sharepoint_credentials_secret,
        config_key=sharepoint_config_key,
    )

    s3_upload_file(
        from_path=local_path,
        to_path=to_path,
        config_key=aws_config_key,
    )
