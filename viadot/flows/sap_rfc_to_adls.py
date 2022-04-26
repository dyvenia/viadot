import pandas as pd
from typing import Any, Dict, List, Literal
from prefect import Flow, task, unmapped

from viadot.tasks import SAPRFCToDF
from viadot.tasks import AzureDataLakeUpload
from viadot.task_utils import df_to_csv

download_sap_task = SAPRFCToDF()
file_to_adls_task = AzureDataLakeUpload()


@task
def concat_dfs(dfs: List[pd.DataFrame]):
    """
    Task to combine list of data frames into one

    Args:
        dfs (List[pd.DataFrame]): List of dataframes to concat.
    Returns:
        full_df (pd.DataFrame()): Pandas dataframe containing all columns from dataframes from list.
    """
    full_df = pd.DataFrame()
    for i in range(len(dfs) - 1):
        full_df = pd.concat([full_df, dfs[i]], axis=1)
    return full_df


class SAPRFCToADLS(Flow):
    def __init__(
        self,
        name: str,
        query_list: List[str],
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
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from SAP DataBase using the RFC protocol.

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
            query_list(List[str]) The list of queries to be executed with pyRFC.
            sep(str, optional): Which separator to use when querying SAP. If not provided, multiple options are automatically tried.
            func (str, optional): SAP RFC function to use. Defaults to "BBP_RFC_READ_TABLE".
            credentials (dict, optional): The credentials to use to authenticate with SAP. By default, they're taken from the local viadot config.
            local_file_path (str, optional): Local destination path. Defaults to None.
            file_sep(str, optional): The separator to use in the CSV. Defaults to "\t".
            if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
            adls_path(str, optional): Azure Data Lake destination file path. Defaults to None.
            overwrite(bool, optional) Whether to overwrite the file in ADLS. Defaults to False.
            sp_credentials_secret(str, optional): The name of the Azure Key Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.Defaults to None.
            vault_name(str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
        """
        self.query_list = query_list
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

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        df = download_sap_task.map(
            query=self.query_list,
            sep=unmapped(self.sep),
            func=unmapped(self.func),
            credentials=unmapped(self.sap_credentials),
            flow=self,
        )
        df_full = concat_dfs.bind(df, flow=self)
        csv = df_to_csv.bind(
            df=df_full,
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
            flow=self,
        )
        df_full.set_upstream(df, flow=self)
        csv.set_upstream(df_full, flow=self)
        adls_upload.set_upstream(csv, flow=self)
