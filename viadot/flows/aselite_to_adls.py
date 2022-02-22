import pandas as pd
from typing import Any, Dict, List, Literal
from prefect import Flow, task
from viadot.tasks import AzureDataLakeUpload, AzureSQLDBQuery
from viadot.task_utils import df_to_csv
from viadot.tasks.aselite import ASELiteToDF
#tasks 
df_task = ASELiteToDF()
#query to df albo od razu query to csv
#df_to_csv_task = df_to_csv()
file_to_adls_task = AzureDataLakeUpload()

@task 
def query_result_to_df_task(result: List[tuple], cols: List[str] ):
    return pd.DataFrame.from_records(result, columns = cols)

class ASLitetoADLS(Flow):
    def __init__(
        self,
        name: str,
        query: str = None,
        sqldb_credentials_secret: str = None,
        adls_sp_credentials_secret: str = None,
        vault_name: str = None,
        # schema: str = None,
        # table: str = None,
        if_empty: str = "warn",
        file_path: str = "None", #from path
        sep: str = "\t",
        to_path: str = None, # storage
        if_exists: Literal["replace", "append", "delete"] = "replace",
        col_names: List[str] = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        # Query task
        self.query = query
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.vault_name = vault_name

        # table and schema needed only  in query
        # self.schema = schema
        # self.table = table
        self.if_empty = if_empty # empty query
        self.col_names = col_names

        self.file_path = file_path # path where to write file (csv) locally
        self.sep = sep
        # Svae to storage
        self.to_path = to_path
        self.if_exists = if_exists

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_result = query_task.bind( query = self.query, credentials_secret = self.sqldb_credentials_secret, vault_name = self.vault_name, flow = self)
        df = query_result_to_df_task.bind(query_result, cols = self.col_names, flow = self)
        csv = df_to_csv.bind(df, path= self.file_path, sep = self.sep, if_exists = self.if_exists, flow = self)
        adls_upload = file_to_adls_task.bind(from_path = self.file_path, to_path = self.to_path, flow =self)
        print(query_result)
        df.set_upstream(query_result, flow=self)
        csv.set_upstream(df, flow =self)
        adls_upload.set_upstream(csv, flow =self)

