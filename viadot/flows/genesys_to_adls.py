from typing import Any, Dict, List, Literal

from prefect import Flow

from viadot.task_utils import df_to_csv
from viadot.tasks import AzureDataLakeUpload

from viadot.tasks.genesys import GenesysToDF

from ..task_utils import (
    add_ingestion_metadata_task,
    df_to_csv,
    df_to_parquet,
)

genesys_report = GenesysToDF()


class GenesysToADLS(Flow):
    def __init__(
        self,
        name: str,
        columns: List[str] = None,
        vault_name: str = None,
        file_path: str = None,
        output_file_extension: str = ".csv",
        sep: str = "\t",
        to_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        sp_credentials_secret: str = None,
        credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.name = name
        self.columns = columns
        self.vault_name = vault_name
        self.file_path = file_path
        self.output_file_extension = output_file_extension
        self.sep = sep
        self.to_path = to_path
        self.if_exists = if_exists
        self.overwrite_adls = overwrite_adls
        self.sp_credentials_secret = sp_credentials_secret
        self.credentials_secret = credentials_secret

        super().__init__(*args, name=name, **kwargs)

    def gen_flow(self) -> Flow:

        df = genesys_report.to_df()
