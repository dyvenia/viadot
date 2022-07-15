from typing import Any, Dict, List, Literal

from prefect import Flow, Task

from viadot.task_utils import df_to_csv
from viadot.tasks import AzureDataLakeUpload

from viadot.tasks.genesys import GenesysToDF

from ..task_utils import (
    add_ingestion_metadata_task,
    df_to_csv,
    df_to_parquet,
)

genesys_report = GenesysToDF()
file_to_adls_task = AzureDataLakeUpload()


class GenesysToADLS(Flow):
    def __init__(
        self,
        name: str,
        columns: List[str] = None,
        vault_name: str = None,
        local_file_path: str = None,
        output_file_extension: str = ".csv",
        sep: str = "\t",
        to_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.name = name
        self.columns = columns
        self.vault_name = vault_name
        self.local_file_path = local_file_path
        self.output_file_extension = output_file_extension
        self.sep = sep
        self.to_path = to_path
        self.if_exists = if_exists
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.credentials_secret = credentials_secret
        self.if_exsists = if_exists

        super().__init__(*args, name=name, **kwargs)

    def gen_genesys_df(self) -> Task:

        return

    def gen_flow(self) -> Flow:

        df = genesys_report.run()
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
        # print(df_with_metadata)
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

        # file_to_adls_task.bind(
        #     from_path=self.local_file_path,
        #     to_path=self.adls_file_path,
        #     overwrite=self.overwrite_adls,
        #     sp_credentials_secret=self.adls_sp_credentials_secret,
        #     flow=self,
        # )

        df_with_metadata.set_upstream(df, flow=self)
        df_to_file.set_upstream(df_with_metadata, flow=self)
        # file_to_adls_task.set_upstream(df_to_file, flow=self)
