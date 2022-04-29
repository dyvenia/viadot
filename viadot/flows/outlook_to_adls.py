import os
from typing import Any, Dict, List, Union, Tuple

import pendulum
from prefect import Flow, Task, apply_map, task
import pandas as pd
from ..utils import slugify
from ..task_utils import df_to_csv

from ..tasks import OutlookToDF, AzureDataLakeUpload

file_to_adls_task = AzureDataLakeUpload()
outlook_to_df = OutlookToDF()

# @task
# def df_to_csv_file_task(df, file_path, extension_file: str = ".csv"):
#     df.to_csv(f"{file_path}{extension_file}", index=False)
COLUMN_LIST = [
    "subject",
    "conversation ID",
    "conversation index",
    "categories",
    "sender",
    "unread",
    "received time",
]


@task
def df_to_csv_file_task(
    df_tuple: Tuple[pd.DataFrame, pd.DataFrame],
    dir_path: str,
    extension_file: str = ".csv",
    # header: List[str] = COLUMN_LIST,
):
    # DF_IN = pd.DataFrame(columns=header)
    # DF_OUT = pd.DataFrame(columns=header)
    df_in = df_tuple[0]
    df_out = df_tuple[1]
    # df_in = DF_IN.append(df_in, ignore_index=True)
    # df_out = DF_OUT.append(df_out, ignore_index=True)
    df_out.to_csv(
        f"{dir_path}/Outbox{extension_file}", mode="a", index=False
    )  # , header=False)
    df_in.to_csv(
        f"{dir_path}/Inbox{extension_file}", mode="a", index=False
    )  # , header=False)


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
        # adls_file_path: str = None,
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        limit: int = 10000,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):

        self.mailbox_list = mailbox_list
        self.start_date = start_date
        self.end_date = end_date
        self.limit = limit

        # AzureDataLakeUpload
        self.extension_file = extension_file
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.dir_names = [
            mailbox.split("@")[0].replace(".", "_").replace("-", "_")
            for mailbox in self.mailbox_list
        ]
        self.local_file_paths = [
            f"{dir_path}/Outbox{self.extension_file}" for dir_path in self.dir_names
        ]
        self.adls_file_paths = [
            f"{adls_dir_path}/{file}" for file in self.local_file_paths
        ]
        for dir in self.dir_names:
            if not os.path.exists(dir):
                os.makedirs(dir)

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_outlook_df(
        self, mailbox_list: Union[str, List[str]], flow: Flow = None
    ) -> Task:
        dfs_tuple = outlook_to_df.bind(
            mailbox_name=mailbox_list,
            start_date=self.start_date,
            end_date=self.end_date,
            limit=self.limit,
            flow=flow,
        )
        return dfs_tuple

    def gen_flow(self) -> Flow:
        df_tuples = apply_map(self.gen_outlook_df, self.mailbox_list, flow=self)

        df_to_csv_file_task.map(df_tuple=df_tuples, dir_path=self.dir_names, flow=self)

        # file_to_adls_task.map(
        #     from_path=self.local_file_paths,
        #     to_path=self.adls_file_paths,
        #     overwrite=self.overwrite_adls,
        #     sp_credentials_secret=self.adls_sp_credentials_secret,
        #     flow=self,
        # )
