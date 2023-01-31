import os
from typing import Any, Dict, List, Literal
import pandas as pd

from datetime import datetime
from prefect import Flow, task
from prefect.utilities import logging
from prefect.engine.signals import FAIL
from prefect.triggers import all_successful
from viadot.tasks import MindfulToCSV
from viadot.tasks import AzureDataLakeUpload
from viadot.task_utils import add_ingestion_metadata_task, adls_bulk_upload

logger = logging.get_logger()
file_to_adls_task = AzureDataLakeUpload()


@task
def add_timestamp(files_names: List = None, sep: str = "\t") -> None:
    """Add new column _viadot_downloaded_at_utc into each file given in the function.

    Args:
        files_names (List, optional): File names where to add the new column. Defaults to None.
        sep (str, optional): Separator type to load and to save data. Defaults to "\t".
    """
    if not files_names:
        logger.warning("Avoided adding a timestamp. No files were reported.")
    else:
        for file in files_names:
            df = pd.read_csv(file, sep=sep)
            df_updated = add_ingestion_metadata_task.run(df)
            df_updated.to_csv(file, index=False, sep=sep)


class MindfulToADLS(Flow):
    def __init__(
        self,
        name: str,
        credentials_mindful: Dict[str, Any] = None,
        credentials_secret: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
        date_interval: int = 1,
        region: Literal["us1", "us2", "us3", "ca1", "eu1", "au1"] = "eu1",
        file_extension: Literal["parquet", "csv"] = "csv",
        sep: str = "\t",
        timeout: int = 3600,
        file_path: str = "",
        adls_file_path: str = None,
        adls_overwrite: bool = True,
        adls_sp_credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """Mindful flow to download the CSV files and upload them to ADLS.

        Args:
            name (str): The name of the Flow.
            credentials_mindful (Dict[str, Any], optional): Credentials to connect with Mindful API. Defaults to None.
            credentials_secret (str, optional): Name of the credential secret to retreave the credentials. Defaults to None.
            start_date (datetime, optional): Start date of the request. Defaults to None.
            end_date (datetime, optional): End date of the resquest. Defaults to None.
            date_interval (int, optional): How many days are included in the request.
                If end_date is passed as an argument, date_interval will be invalidated. Defaults to 1.
            region (Literal[us1, us2, us3, ca1, eu1, au1], optional): SD region from where to interact with the mindful API. Defaults to "eu1".
            file_extension (Literal[parquet, csv], optional): File extensions for storing responses. Defaults to "csv".
            sep (str, optional): Separator in csv file. Defaults to "\t".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
            file_path (str, optional): Path where to save the file locally. Defaults to ''.
            adls_file_path (str, optional): The destination path at ADLS. Defaults to None.
            adls_overwrite (bool, optional): Whether to overwrite files in the data lake. Defaults to True.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
        """

        self.credentials_mindful = credentials_mindful
        self.credentials_secret = credentials_secret
        self.start_date = start_date
        self.end_date = end_date
        self.date_interval = date_interval
        self.region = region
        self.file_extension = file_extension
        self.sep = sep
        self.file_path = file_path
        self.timeout = timeout

        self.adls_file_path = adls_file_path
        self.adls_overwrite = adls_overwrite
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        super().__init__(*args, name=name, **kwargs)

        self.mind_flow()

    def mind_flow(self) -> Flow:
        to_csv = MindfulToCSV(timeout=self.timeout)

        file_names = to_csv.bind(
            credentials_mindful=self.credentials_mindful,
            credentials_secret=self.credentials_secret,
            start_date=self.start_date,
            end_date=self.end_date,
            date_interval=self.date_interval,
            region=self.region,
            file_extension=self.file_extension,
            file_path=self.file_path,
            flow=self,
        )

        add_timestamp.bind(file_names, sep=self.sep, flow=self)

        adls_bulk_upload.bind(
            file_names=file_names,
            file_name_relative_path=self.file_path,
            adls_file_path=self.adls_file_path,
            adls_sp_credentials_secret=self.adls_sp_credentials_secret,
            adls_overwrite=self.adls_overwrite,
            timeout=self.timeout,
            flow=self,
        )

        add_timestamp.set_upstream(file_names, flow=self)
        adls_bulk_upload.set_upstream(add_timestamp, flow=self)
