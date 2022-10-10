import os
from typing import Any, Dict, List, Literal

import pandas as pd
from datetime import datetime
from prefect import Flow, task

from viadot.tasks import MindfulToCSV
from viadot.tasks import AzureDataLakeUpload
from viadot.task_utils import add_ingestion_metadata_task

file_to_adls_task = AzureDataLakeUpload()


@task
def adls_bulk_upload(
    file_names: List[str],
    file_name_relative_path: str = "",
    adls_file_path: str = None,
    adls_sp_credentials_secret: str = None,
    adls_overwrite: bool = True,
) -> List[str]:
    """Function that upload files to defined path in ADLS.

    Args:
        file_names (List[str]): List of file names to generate paths.
        file_name_relative_path (str, optional): Path where to save the file locally. Defaults to ''.
        adls_file_path (str, optional): Azure Data Lake path. Defaults to None.
        adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
        adls_overwrite (bool, optional): Whether to overwrite files in the data lake. Defaults to True.

    Returns:
        List[str]: List of paths
    """

    for file in file_names:
        file_path = str(adls_file_path + "/" + file)
        file_to_adls_task.run(
            from_path=os.path.join(file_name_relative_path, file),
            to_path=file_path,
            sp_credentials_secret=adls_sp_credentials_secret,
            overwrite=adls_overwrite,
        )


@task
def add_timestamp(files_names: List = None, sep: str = "\t") -> None:
    """Add new column _viadot_downloaded_at_utc into each file given in the function.

    Args:
        files_names (List, optional): File names where to add the new column. Defaults to None.
        sep (str, optional): Separator type to load and to save data. Defaults to "\t".
    """
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

        self.adls_file_path = adls_file_path
        self.adls_overwrite = adls_overwrite
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        super().__init__(*args, name=name, **kwargs)

        self.mind_flow()

    def mind_flow(self) -> Flow:
        to_csv = MindfulToCSV()

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

        uploader = adls_bulk_upload(
            file_names=file_names,
            file_name_relative_path=self.file_path,
            adls_file_path=self.adls_file_path,
            adls_sp_credentials_secret=self.adls_sp_credentials_secret,
            adls_overwrite=self.adls_overwrite,
            flow=self,
        )

        add_timestamp.set_upstream(file_names, flow=self)
        uploader.set_upstream(add_timestamp, flow=self)
