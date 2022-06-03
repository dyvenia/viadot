import pandas as pd
from typing import Any, Dict, List, Literal
from prefect import Flow, task, unmapped

from viadot.tasks import SAPRFCToDF
from viadot.tasks import AzureDataLakeUpload
from viadot.task_utils import df_to_csv, df_to_parquet, concat_dfs

download_sap_task = SAPRFCToDF()
file_to_adls_task = AzureDataLakeUpload()


class SAPRFCToADLS(Flow):
    def __init__(
        self,
        name: str,
        query: str = None,
        queries: List[str] = None,
        rfc_sep: str = None,
        func: str = "RFC_READ_TABLE",
        sap_credentials: dict = None,
        output_file_extension: str = ".parquet",
        local_file_path: str = None,
        file_sep: str = "\t",
        if_exists: Literal["append", "replace", "skip"] = "replace",
        adls_path: str = None,
        overwrite: bool = False,
        adls_sp_credentials_secret: str = None,
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from SAP database using the RFC protocol and uploading it to Azure Data Lake.

        Note that only a very limited subset of SQL is supported:
            - aliases
            - where clauses combined using the AND operator
            - limit & offset

        Unsupported:
            - aggregations
            - joins
            - subqueries
            - etc.

        Args:
            name (str): The name of the flow.
            query (str): Query to be executed with pyRFC. If multiple queries needed use `queries` parmeter. Defaults to None.
            queries(List[str]) The list of queries to be executed with pyRFC. Defaults to None.
            rfc_sep(str, optional): Which separator to use when querying SAP. If not provided, multiple options are automatically tried.
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            sap_credentials (dict, optional): The credentials to use to authenticate with SAP. By default, they're taken from the local viadot config.
            output_file_extension (str, optional): Output file extension - to allow selection of .csv for data which is not easy to handle with parquet. Defaults to ".parquet".
            local_file_path (str, optional): Local destination path. Defaults to None.
            file_sep(str, optional): The separator to use in the CSV. Defaults to "\t".
            if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
            adls_path(str, optional): Azure Data Lake destination file path. Defaults to None.
            overwrite(bool, optional) Whether to overwrite the file in ADLS. Defaults to False.
            adls_sp_credentials_secret(str, optional): The name of the Azure Key Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.Defaults to None.
            vault_name(str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
        """
        self.query = query
        self.queries = queries
        self.rfc_sep = rfc_sep
        self.func = func
        self.sap_credentials = sap_credentials
        self.output_file_extension = output_file_extension
        self.local_file_path = local_file_path
        self.file_sep = file_sep
        self.if_exists = if_exists
        self.adls_path = adls_path
        self.overwrite = overwrite
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.vault_name = vault_name

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        if self.queries is not None:
            df = download_sap_task.map(
                query=self.queries,
                sep=unmapped(self.rfc_sep),
                func=unmapped(self.func),
                credentials=unmapped(self.sap_credentials),
                flow=self,
            )
            df_final = concat_dfs.bind(df, flow=self)
            df_final.set_upstream(df, flow=self)
        else:
            df_final = download_sap_task(
                query=self.query,
                sep=self.rfc_sep,
                func=self.func,
                credentials=self.sap_credentials,
                flow=self,
            )

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df_final,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df_final,
                sep=self.file_sep,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )

        adls_upload = file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_to_file.set_upstream(df_final, flow=self)
        adls_upload.set_upstream(df_to_file, flow=self)
