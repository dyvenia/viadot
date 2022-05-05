import os
from typing import Any, Dict, List, Union, Literal

import pendulum
from prefect import Flow, Task, apply_map, task
import pandas as pd
from ..utils import slugify
from ..task_utils import df_to_csv, union_dfs_task

from ..tasks import OutlookToDF, AzureDataLakeUpload

file_to_adls_task = AzureDataLakeUpload()
outlook_to_df = OutlookToDF()


class OutlookToCSVs(Flow):
    def __init__(
        self,
        mailbox_list: List[str],
        name: str = None,
        start_date: str = None,
        end_date: str = None,
        local_file_path: str = None,
        extension_file: str = ".csv",
        adls_dir_path: str = None,
        adls_file_path: str = None,
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        limit: int = 10000,
        if_exists: Literal["append", "replace", "skip"] = "append",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):

        self.mailbox_list = mailbox_list
        self.start_date = start_date
        self.end_date = end_date
        self.limit = limit
        self.local_file_path = local_file_path
        # AzureDataLakeUpload
        self.adls_dir_path = adls_dir_path
        self.adls_file_path = adls_file_path
        self.extension_file = extension_file
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        self.if_exsists = if_exists

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_outlook_df(
        self, mailbox_list: Union[str, List[str]], flow: Flow = None
    ) -> Task:

        report = outlook_to_df.bind(
            mailbox_name=mailbox_list,
            start_date=self.start_date,
            end_date=self.end_date,
            limit=self.limit,
            flow=flow,
        )

        return report

    def gen_flow(self) -> Flow:

        dfs = apply_map(self.gen_outlook_df, self.mailbox_list, flow=self)  # df_tuples

        df = union_dfs_task.bind(dfs, flow=self)

        df_to_file = df_to_csv.bind(
            df=df, path=self.local_file_path, if_exists=self.if_exsists, flow=self
        )

        file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_to_file.set_upstream(df, flow=self)
        file_to_adls_task.set_upstream(df_to_file, flow=self)
