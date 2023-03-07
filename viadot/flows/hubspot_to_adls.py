import os
from pathlib import Path
from typing import Any, Dict, List, Union

from prefect import Flow
from prefect.utilities import logging

from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
)
from viadot.tasks import AzureDataLakeUpload, HubspotToDF

logger = logging.get_logger(__name__)

file_to_adls_task = AzureDataLakeUpload()
hubspot_to_df_task = HubspotToDF()


class HubspotToADLS(Flow):
    def _init_(
        self,
        name: str,
        endpoint: str,
        properties: List[any],
        filters: Dict[str, Any],
        nrows: int = 1000,
        adls_file_name: str = None,
        adls_dir_path: str = None,
        overwrite: bool = True,
        if_exists: str = "replace",
        adls_sp_credentials_secret: str = None,
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        self.name = name
        self.endpoint = endpoint
        self.properties = properties
        self.filters = filters
        self.nrows = nrows
        self.overwrite = overwrite
        self.if_exists = if_exists

        # AzureDataLakeUpload
        self.adls_file_name = adls_file_name or self.slugify(name)
        self.adls_dir_path = adls_dir_path

        if adls_file_name is not None:
            self.adls_file_path = os.path.join(adls_dir_path, adls_file_name)
            self.adls_schema_file_dir_file = os.path.join(
                adls_dir_path, "schema", Path(adls_file_name).stem + ".json"
            )

        else:
            self.adls_file_path = os.path.join(
                adls_dir_path, self.now + self.output_file_extension
            )
            self.adls_schema_file_dir_file = os.path.join(
                adls_dir_path, "schema", self.now + ".json"
            )
        self.overwrite_adls = overwrite
        self.output_file_extension = self.adls_file_name.strip(".")[1]

        # self.if_empty = if_empty
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        self.vault_name = vault_name

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        df = hubspot_to_df_task.bind(
            endpoint=self.endpoint,
            properties=self.properties,
            filters=self.filters,
            nrows=self.nrows,
        )

        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
        dtypes_dict = df_get_data_types_task.bind(df_with_metadata, flow=self)
        df_mapped = df_map_mixed_dtypes_for_parquet.bind(
            df_with_metadata, dtypes_dict, flow=self
        )
        if self.output_file_extension == "parquet":

            df_to_file = df_to_parquet.bind(
                df=df_mapped,
                path=self.adls_file_name,
                if_exists=self.if_exists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df_with_metadata,
                path=self.adls_file_name,
                if_exists=self.if_exists,
                flow=self,
            )

        file_to_adls_task.bind(
            from_path=self.adls_file_name,
            to_path=self.adls_file_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_mapped.set_upstream(df_with_metadata, flow=self)
        df_to_file.set_upstream(df_mapped, flow=self)

        file_to_adls_task.set_upstream(df_to_file, flow=self)
