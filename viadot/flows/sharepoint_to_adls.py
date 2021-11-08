import os
from typing import Any, Dict, List
import pendulum
from prefect import Flow, task
from prefect.backend import set_key_value


from ..task_utils import (
    df_get_data_types_task,
    add_ingestion_metadata_task,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json,
)
from ..tasks import AzureDataLakeUpload
from ..tasks.sharepoint import SharepointToDF

excel_to_df_task = SharepointToDF()
file_to_adls_task = AzureDataLakeUpload()
json_to_adls_task = AzureDataLakeUpload()


@task
def df_mapp_mixed_dtypes_for_parquet_task(df, dtypes_dict):
    df_mapped = df.copy()
    for col, dtype in dtypes_dict.items():
        if dtype != "Date":
            if dtype == "DateTime":
                df_mapped[col] = df_mapped[col].astype("string")
            else:
                df_mapped[col] = df_mapped[col].astype(f"{dtype.lower()}")
        if dtype == "Object":
            df_mapped[col] = df_mapped[col].astype("string")
    return df_mapped


class SharepointToADLS(Flow):
    def __init__(
        self,
        name: str = None,
        nrows_to_df: int = None,
        file_from_sharepoint: str = None,
        output_file_extension: str = ".csv",
        local_dir_path: str = None,
        adls_dir_path: str = None,
        adls_sp_credentials_secret: str = None,
        if_empty: str = "warn",
        if_exists: str = "replace",
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):

        # ShareointToDF
        self.if_empty = if_empty
        self.nrows = nrows_to_df
        self.file_from_sharepoint = file_from_sharepoint
        self.local_dir_path = local_dir_path

        # AzureDataLakeUpload
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.if_exists = if_exists
        self.output_file_extension = output_file_extension
        self.now = str(pendulum.now("utc"))
        if self.local_dir_path is not None:
            self.local_file_path = (
                self.local_dir_path + self.slugify(name) + self.output_file_extension
            )
        else:
            self.local_file_path = self.slugify(name) + self.output_file_extension
        self.local_json_path = self.slugify(name) + ".json"
        self.adls_schema_file_dir_file = os.path.join(
            adls_dir_path, "schema", self.now + ".json"
        )
        self.adls_dir_path = adls_dir_path
        self.adls_file_path = os.path.join(
            adls_dir_path, self.now + self.output_file_extension
        )

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df = excel_to_df_task.bind(
            path_to_file=self.file_from_sharepoint, nrows=self.nrows, flow=self
        )
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
        dtypes_dict = df_get_data_types_task.bind(df_with_metadata, flow=self)
        df_mapped = df_mapp_mixed_dtypes_for_parquet_task.bind(
            df_with_metadata, dtypes_dict, flow=self
        )
        if self.output_file_extension == ".parquet":

            df_to_file = df_to_parquet.bind(
                df=df_mapped,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
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

        dtypes_to_json.bind(
            dtypes_dict=dtypes_dict, local_json_path=self.local_json_path, flow=self
        )
        json_to_adls_task.bind(
            from_path=self.local_json_path,
            to_path=self.adls_schema_file_dir_file,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_mapped.set_upstream(df_with_metadata, flow=self)
        dtypes_to_json.set_upstream(df_mapped, flow=self)
        df_to_file.set_upstream(dtypes_to_json, flow=self)

        file_to_adls_task.set_upstream(df_to_file, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()
