from typing import Any, Dict, List, Literal, Union

import pandas as pd
from prefect import Flow, Task, apply_map

from ..task_utils import (
    add_ingestion_metadata_task,
    df_to_csv,
    df_to_parquet,
    union_dfs_task,
    credentials_loader,
)
from viadot.tasks import AzureDataLakeUpload, OutlookToDF


class OutlookToADLS(Flow):
    def __init__(
        self,
        mailbox_list: List[str],
        name: str = None,
        start_date: str = None,
        end_date: str = None,
        local_file_path: str = None,
        output_file_extension: str = ".parquet",
        adls_file_path: str = None,
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        limit: int = 10000,
        timeout: int = 3600,
        if_exists: Literal["append", "replace", "skip"] = "append",
        outlook_credentials_secret: str = "OUTLOOK",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from Outlook source to Azure Data Lake in parquet format by default.

        Args:
            mailbox_list (List[str]): Mailbox name.
            name (str, optional): The name of the flow. Defaults to None.
            start_date (str, optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            output_file_extension (str, optional): Output file extension. Defaults to ".parquet".
            adls_file_path (str, optional): Azure Data Lake destination file path. Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake. Defaults to None.
            outlook_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with outlook credentials.
            limit (int, optional): Number of fetched top messages. Defaults to 10000.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
            if_exists (Literal['append', 'replace', 'skip'], optional): What to do if the local file already exists. Defaults to "append".
        """

        self.mailbox_list = mailbox_list
        self.start_date = start_date
        self.end_date = end_date
        self.limit = limit
        self.timeout = timeout
        self.local_file_path = local_file_path
        self.if_exsists = if_exists

        # AzureDataLakeUpload
        self.adls_file_path = adls_file_path
        self.output_file_extension = output_file_extension
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.outlook_credentials_secret = outlook_credentials_secret

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_outlook_df(
        self, mailbox_list: Union[str, List[str]], flow: Flow = None
    ) -> Task:
        credentials = credentials_loader.run(
            credentials_secret=self.outlook_credentials_secret
        )
        outlook_to_df = OutlookToDF(timeout=self.timeout, credentials=credentials)
        df = outlook_to_df.bind(
            mailbox_name=mailbox_list,
            start_date=self.start_date,
            end_date=self.end_date,
            limit=self.limit,
            flow=flow,
        )

        return df

    def gen_flow(self) -> Flow:

        dfs = apply_map(self.gen_outlook_df, self.mailbox_list, flow=self)

        df = union_dfs_task.bind(dfs, flow=self)
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exsists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exsists,
                flow=self,
            )

        file_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_with_metadata.set_upstream(df, flow=self)
        df_to_file.set_upstream(df_with_metadata, flow=self)
        file_to_adls_task.set_upstream(df_to_file, flow=self)
