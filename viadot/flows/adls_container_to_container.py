from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from prefect import Flow, task
from prefect.utilities import logging

from ..tasks import AzureDataLakeDownload, AzureDataLakeUpload

download_task = AzureDataLakeDownload(gen=2)
upload_task = AzureDataLakeUpload(gen=2)


logger = logging.get_logger(__name__)


@task
def add_ingestion_metadata(
    path: str,
    sep: str = "\t",
):
    """Add ingestion metadata column(s), eg. data download date"""
    df = pd.read_csv(path, sep=sep)
    df["_viadot_downloaded_at_utc"] = datetime.now(timezone.utc)
    df.to_csv(path, sep=sep, index=False)


class ADLSConToCon(Flow):
    """Move file(s) from Azure Data Lake gen1 to gen2.

    Args:
        name (str): The name of the flow.
        from_path (str): The path to the  Data Lake folder.
        to_path (str): The path of the final file location a/a/filename.extension.
        local_file_path (str): Where the gen1 file should be downloaded.
        overwrite (str): Whether to overwrite the destination file(s).
        gen2_sp_credentials_secret (str): The Key Vault secret holding Service Pricipal credentials for gen2 lake
        vault_name (str): The name of the vault from which to retrieve the secrets.
    """

    def __init__(
        self,
        name: str,
        from_path: str,
        to_path: str,
        local_file_path: str = None,
        overwrite: bool = True,
        sep: str = "\t",
        gen2_sp_credentials_secret: str = None,
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.from_path = from_path
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.to_path = to_path
        self.overwrite = overwrite
        self.sep = sep
        self.gen2_sp_credentials_secret = gen2_sp_credentials_secret
        self.vault_name = vault_name
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        download_task.bind(
            # tutaj poparacuj nad zczytaniem pliku ostatniego!!!
            from_path=self.from_path,
            to_path=self.local_file_path,
            gen=2,
            gen2_sp_credentials_secret=self.gen2_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        add_ingestion_metadata.bind(path=self.local_file_path, sep=self.sep, flow=self)
        upload_task.bind(
            from_path=self.local_file_path,
            to_path=self.to_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.gen2_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        add_ingestion_metadata.set_upstream(download_task, flow=self)
        upload_task.set_upstream(add_ingestion_metadata, flow=self)
