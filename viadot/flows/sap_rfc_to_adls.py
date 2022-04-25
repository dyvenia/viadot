import csv
from typing import Any, Dict, List, Literal
from prefect import Flow

from viadot.tasks import SAPRFCToDF
from viadot.tasks import AzureDataLakeUpload
from viadot.task_utils import df_to_csv

download_sap_task = SAPRFCToDF()
file_to_adls_task = AzureDataLakeUpload()


class SAPRFCToADLS(Flow):
    def __init__(
        self,
        name: str,
        query: str = None,
        sep: str = None,
        func: str = "BBP_RFC_READ_TABLE",
        sap_credentials: dict = None,
        local_file_path: str = None,
        file_sep: str = "\t",
        if_exists: Literal["append", "replace", "skip"] = "replace",
        adls_path: str = None,
        overwrite: bool = False,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        gen: int = 2,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """ """
        self.query = query
        self.sep = sep
        self.func = func
        self.sap_credentials = sap_credentials
        self.local_file_path = local_file_path
        self.file_sep = file_sep
        self.if_exists = if_exists
        self.adls_path = adls_path
        self.overwrite = overwrite
        self.sp_credentials_secret = sp_credentials_secret
        self.vault_name = vault_name
        self.gen = gen

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df = download_sap_task.bind(
            query=self.query,
            sep=self.sep,
            func=self.func,
            credentials=self.sap_credentials,
            flow=self,
        )
        csv = df_to_csv.bind(
            df=df,
            sep=self.file_sep,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )
        adls_upload = file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.sp_credentials_secret,
            gen=self.gen,
            flow=self,
        )
        csv.set_upstream(df, flow=self)
        adls_upload.set_upstream(csv, flow=self)
