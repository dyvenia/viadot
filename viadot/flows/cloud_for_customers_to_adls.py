import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
import pendulum
import prefect
from prefect import Flow, Task, apply_map, task
from prefect.backend import set_key_value
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

from ..task_utils import add_ingestion_metadata_task
from ..tasks import (
    AzureDataLakeUpload,
    CloudForCustomersToDF,
)

logger = logging.get_logger(__name__)

cloud_for_customers_to_df_task = CloudForCustomersToDF()
file_to_adls_task = AzureDataLakeUpload()


@task
def df_to_csv_task(df, path: str, if_exists: str = "replace"):
    if if_exists == "append":
        if os.path.isfile(path):
            csv_df = pd.read_csv(path)
            out_df = pd.concat([csv_df, df])
        else:
            out_df = df
    elif if_exists == "replace":
        out_df = df
    out_df.to_csv(path, index=False)


class CloudForCustomersToADLS(Flow):
    def __init__(
        self,
        url: str,
        endpoint: str,
        name: str,
        adls_sp_credentials_secret: str = None,
        fields: List[str] = None,
        local_file_path: str = None,
        output_file_extension: str = ".csv",
        adls_dir_path: str = None,
        if_empty: str = "warn",
        if_exists: str = "replace",
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):

        # CloudForCustomersToDF
        self.if_empty = if_empty
        self.url = url
        self.endpoint = endpoint
        self.fields = fields

        # AzureDataLakeUpload
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.if_exists = if_exists
        self.output_file_extension = output_file_extension
        self.local_file_path = (
            local_file_path or self.slugify(name) + self.output_file_extension
        )
        self.now = str(pendulum.now("utc"))
        self.adls_dir_path = adls_dir_path
        self.adls_file_path = os.path.join(
            adls_dir_path, self.now + self.output_file_extension
        )

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def cloud_for_customers_to_df_task(self, flow: Flow = None) -> Task:

        c4c_df = cloud_for_customers_to_df_task.bind(
            url=self.url,
            endpoint=self.endpoint,
            fields=self.fields,
            flow=flow,
        )

        return c4c_df

    def gen_flow(self) -> Flow:

        df = self.cloud_for_customers_to_df_task(flow=self)

        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

        df_to_file = df_to_csv_task.bind(
            df=df_with_metadata,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )

        file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        file_to_adls_task.set_upstream(df_to_file, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()
