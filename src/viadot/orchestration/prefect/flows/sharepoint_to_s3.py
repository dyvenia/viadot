"""Flows for downloading data from Sharepoint and uploading it to Amazon S3."""

from typing import Any

from viadot.orchestration.prefect.tasks import s3_upload_file, sharepoint_download_file
from viadot.sources.sharepoint import SharepointCredentials

from prefect import flow


@flow(
    name="extract--sharepoint--s3",
    description="Flows for downloading data from Sharepoint and uploading it to Amazon S3.",
    retries=1,
    retry_delay_seconds=60,
)
def sharepoint_to_s3(  # noqa: PLR0913, PLR0917
    url: str,
    local_path: str,
    to_path: str,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_credentials: SharepointCredentials | None = None,
    sharepoint_config_key: str | None = None,
    aws_credentials: dict[str, Any] | None = None,
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
        sharepoint_credentials (SharepointCredentials): Sharepoint credentials.
        sharepoint_config_key (str, optional): The key in the viadot config holding
            relevant credentials.
        aws_credentials (dict[str, Any], optional): Credentials to the Amazon S3.
            Defaults to None.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    sharepoint_download_file(
        url=url,
        to_path=local_path,
        credentials_secret=sharepoint_credentials_secret,
        credentials=sharepoint_credentials,
        config_key=sharepoint_config_key,
    )

    s3_upload_file(
        from_path=local_path,
        to_path=to_path,
        credentials=aws_credentials,
        config_key=aws_config_key,
    )
