from typing import Any, Dict, List, Literal

from prefect import Flow

from viadot.task_utils import df_to_csv
from viadot.tasks import AzureDataLakeUpload
from viadot.tasks.mysql_to_df import MySqlToDf


class MySqlToADLS(Flow):
    def __init__(
        self,
        name: str,
        country_short: Literal["AT", "DE", "CH", None],
        query: str = None,
        sqldb_credentials_secret: str = None,
        vault_name: str = None,
        file_path: str = None,
        sep: str = "\t",
        to_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        sp_credentials_secret: str = None,
        credentials_secret: str = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for downloading data from MySQL to csv file, then uploading it to ADLS.

        Args:
            name (str): The name of the flow.
            country_short (str): Country short to extract proper credentials.
            query (str): Query to perform on a database. Defaults to None.
            sqldb_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            MySQL Database credentials. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            file_path (str, optional): Local destination path. Defaults to None.
            sep (str, optional): The delimiter for the output CSV file. Defaults to "\t".
            to_path (str): The path to an ADLS file. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            overwrite_adls  (str, optional): Whether to overwrite_adls  the destination file. Defaults to True.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            credentials_secret (str, optional): Key Vault name. Defaults to None.
            columns_to_clean (List(str), optional): Select columns to clean, used with remove_special_characters.
            If None whole data frame will be processed. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        # Connect to sql
        self.country_short = country_short
        self.query = query
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.vault_name = vault_name
        self.overwrite_adls = overwrite_adls

        # Upload to ADLS
        self.file_path = file_path
        self.sep = sep
        self.to_path = to_path
        self.if_exists = if_exists
        self.sp_credentials_secret = sp_credentials_secret
        self.credentials_secret = credentials_secret
        self.timeout = timeout

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        df_task = MySqlToDf(country_short=self.country_short, timeout=self.timeout)
        df = df_task.bind(
            credentials_secret=self.credentials_secret, query=self.query, flow=self
        )

        create_csv = df_to_csv.bind(
            df,
            path=self.file_path,
            sep=self.sep,
            if_exists=self.if_exists,
            flow=self,
        )

        file_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        adls_upload = file_to_adls_task.bind(
            from_path=self.file_path,
            to_path=self.to_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.sp_credentials_secret,
            flow=self,
        )

        create_csv.set_upstream(df, flow=self)
        adls_upload.set_upstream(create_csv, flow=self)
