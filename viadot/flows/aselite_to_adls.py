from typing import Any, Dict, List, Literal

from prefect import Flow

from viadot.task_utils import (
    df_clean_column,
    df_converts_bytes_to_int,
    df_to_csv,
    validate_df,
)
from viadot.tasks import AzureDataLakeUpload
from viadot.tasks.aselite import ASELiteToDF


class ASELiteToADLS(Flow):
    def __init__(
        self,
        name: str,
        query: str = None,
        sqldb_credentials_secret: str = None,
        vault_name: str = None,
        file_path: str = None,
        sep: str = "\t",
        to_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite: bool = True,
        validate_df_dict: Dict[str, Any] = None,
        convert_bytes: bool = False,
        sp_credentials_secret: str = None,
        remove_special_characters: bool = None,
        columns_to_clean: List[str] = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for downloading data from ASElite to csv file, then uploading it to ADLS.

        Args:
            name (str): The name of the flow.
            query (str): Query to perform on a database. Defaults to None.
            sqldb_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ASElite SQL Database credentials. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            file_path (str, optional): Local destination path. Defaults to None.
            sep (str, optional): The delimiter for the output CSV file. Defaults to "\t".
            to_path (str): The path to an ADLS file. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            overwrite (str, optional): Whether to overwrite the destination file. Defaults to True.
            validate_df_dict (Dict[str], optional): A dictionary with optional list of tests to verify the output
                dataframe. If defined, triggers the `validate_df` task from task_utils. Defaults to None.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            remove_special_characters (str, optional): Call a function that remove special characters like escape symbols. Defaults to None.
            columns_to_clean (List(str), optional): Select columns to clean, used with remove_special_characters.
            If None whole data frame will be processed. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        self.query = query
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.vault_name = vault_name
        self.overwrite = overwrite
        self.validate_df_dict = validate_df_dict

        self.file_path = file_path
        self.sep = sep
        self.to_path = to_path
        self.if_exists = if_exists
        self.convert_bytes = convert_bytes
        self.sp_credentials_secret = sp_credentials_secret
        self.remove_special_characters = remove_special_characters
        self.columns_to_clean = columns_to_clean
        self.timeout = timeout

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df_task = ASELiteToDF(timeout=self.timeout)
        df = df_task.bind(
            query=self.query,
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        if self.convert_bytes == True:
            df = df_converts_bytes_to_int.bind(df, flow=self)

        if self.remove_special_characters == True:
            df = df_clean_column(df, columns_to_clean=self.columns_to_clean, flow=self)

        if self.validate_df_dict:
            validation_task = validate_df.bind(
                df, tests=self.validate_df_dict, flow=self
            )
            validation_task.set_upstream(df, flow=self)

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
            overwrite=self.overwrite,
            sp_credentials_secret=self.sp_credentials_secret,
            flow=self,
        )

        if self.validate_df_dict:
            create_csv.set_upstream(validation_task, flow=self)

        create_csv.set_upstream(df, flow=self)
        adls_upload.set_upstream(create_csv, flow=self)
