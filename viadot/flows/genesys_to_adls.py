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
        adls_file_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """Flow for downloading data from genesys API to Azure Data Lake in csv format by default.

        Args:
            name (str): The name of the flow.
            columns (List[str], optional): The report fields. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of.csv for data
            which is not easy to handle with parquet. Defaults to ".csv".
            sep (str, optional): The separator used in csv. Defaults to "\t".
            adls_file_path (str, optional): Azure Data Lake destination file path. Defaults to None.
            if_exists (str, optional): What to do if the file exists. Defaults to "replace".
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to True.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret for Bigquery project. Defaults to None.
        """

        self.name = name
        self.columns = columns
        self.vault_name = vault_name
        self.local_file_path = local_file_path
        self.output_file_extension = output_file_extension
        self.sep = sep
        self.if_exists = if_exists
        self.adls_file_path = adls_file_path
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.credentials_secret = credentials_secret
        self.if_exsists = if_exists

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        df = genesys_report.bind(report_columns=self.columns, flow=self)
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
                sep=self.sep,
                if_exists=self.if_exsists,
                flow=self,
            )

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
