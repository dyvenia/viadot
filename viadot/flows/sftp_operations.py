from typing import Any, Dict, List, Literal
from prefect import Flow
from viadot.flows.adls_to_azure_sql import df_to_csv_task

from viadot.tasks import SftpToDF
from viadot.tasks import AzureDataLakeUpload, AzureSQLCreateTable, BCPTask
from viadot.task_utils import add_ingestion_metadata_task


class SftpToAzureSQL(Flow):
    def __init__(
        self,
        name: str,
        from_path: str = None,
        file_name: str = None,
        columns: List[str] = None,
        sep: str = "\t",
        remove_tab: bool = True,
        dtypes: Dict[str, Any] = None,
        table: str = None,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "fail",
        sftp_credentials_secret: Dict[str, Any] = None,
        sftp_credentials: Dict[str, Any] = None,
        sqldb_credentials_secret: str = None,
        on_bcp_error: Literal["skip", "fail"] = "fail",
        error_log_file_path: str = "SFTP_logs.log",
        vault_name: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        Bulk insert data from SFTP server into an Azure SQL Database table.
        This task also creates the table if it doesn't exist.

        Args:
            name (str): The name of the flow.
            from_path (str, optional): Path to the file in SFTP server. Defaults to None.
            file_name (str, optional): File name for local file. Defaults to None.
            columns (List[str], optional): Columns to read from the file. Defaults to None.
            sep (str, optional): The separator to use to read the CSV file. Defaults to "\t".
            remove_tab (bool, optional): Whether to remove tab delimiters from the data. Defaults to False.
            dtypes (dict, optional): Which custom data types should be used for SQL table creation task.
            table (str, optional): Destination table. Defaults to None.
            schema (str, optional): Destination schema. Defaults to None.
            if_exists (Literal, optional): What to do if the table already exists. Defaults to "replace".
            sftp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary credentials for SFTP connection. Defaults to None.
            sftp_credentials (Dict[str, Any], optional): SFTP server credentials. Defaults to None.
            sqldb_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            on_bcp_error (Literal["skip", "fail"], optional): What to do if error occurs. Defaults to "fail".
            error_log_file_path (string, optional): Full path of an error file. Defaults to "./log_file.log".
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # SFTP
        self.from_path = from_path
        self.sftp_credentials_secret = sftp_credentials_secret
        self.sftp_credentials = sftp_credentials
        self.columns = columns
        # File args
        if file_name is None:
            self.file_name = from_path.split("/")[-1]

        else:
            self.file_name = file_name

        self.sep = sep
        self.remove_tab = remove_tab
        self.timeout = timeout

        # Read schema
        self.schema = schema
        self.table = table
        self.dtypes = dtypes

        # AzureSQLCreateTable
        self.table = table
        self.schema = schema
        self.if_exists = if_exists
        self.vault_name = vault_name

        # BCPTask
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.on_bcp_error = on_bcp_error
        self.error_log_file_path = error_log_file_path

        super().__init__(
            name=name,
            *args,
            **kwargs,
        )

        self.gen_flow()

    @staticmethod
    def _map_if_exists(if_exists: str) -> str:
        mapping = {"append": "skip"}
        return mapping.get(if_exists, if_exists)

    def __call__(self, *args, **kwargs):
        """Download file-like object from SFTP server and load data into Azure SQL database."""
        return super().__call__(*args, **kwargs)

    def gen_flow(self) -> Flow:
        sftp = SftpToDF(
            sftp_credentials_secret=self.sftp_credentials_secret,
            credentials=self.sftp_credentials,
            timeout=self.timeout,
        )
        df = sftp.bind(
            from_path=self.from_path,
            columns=self.columns,
            flow=self,
        )
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
        df_to_csv_task.bind(
            df=df_with_metadata,
            remove_tab=self.remove_tab,
            path=self.file_name,
            flow=self,
        )

        create_table_task = AzureSQLCreateTable(timeout=self.timeout)
        create_table_task.bind(
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            if_exists=self._map_if_exists(self.if_exists),
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        bulk_insert_task = BCPTask(timeout=self.timeout)
        bulk_insert_task.bind(
            path=self.file_name,
            schema=self.schema,
            table=self.table,
            error_log_file_path=self.error_log_file_path,
            on_error=self.on_bcp_error,
            credentials_secret=self.sqldb_credentials_secret,
            flow=self,
        )

        df_with_metadata.set_upstream(df, flow=self)
        df_to_csv_task.set_upstream(df_with_metadata, flow=self)
        create_table_task.set_upstream(df_to_csv_task, flow=self)
        bulk_insert_task.set_upstream(create_table_task, flow=self)


class SftpToADLS(Flow):
    def __init__(
        self,
        name: str,
        from_path: str = None,
        file_name: str = None,
        sep: str = "\t",
        remove_tab: bool = True,
        overwrite: bool = True,
        to_path: str = None,
        columns: List[str] = None,
        sftp_credentials_secret: Dict[str, Any] = None,
        sftp_credentials: Dict[str, Any] = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        Bulk insert data from SFTP server into an Azure SQL Database table.
        This task also creates the table if it doesn't exist.

        Args:
            name (str): The name of the flow.
            from_path (str, optional): Path to the file in SFTP server. Defaults to None.
            file_name (str, optional): File name for local file. Defaults to None.
            sep (str, optional): The separator to use to read the CSV file. Defaults to "\t".
            remove_tab (bool, optional): Whether to remove tab delimiters from the data. Defaults to False.
            overwrite (bool, optional): Whether to overwrite files in the lake. Defaults to False.
            to_path (str, optional): The destination path in ADLS. Defaults to None.
            columns (List[str], optional): Columns to read from the file. Defaults to None.
            sftp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary credentials for SFTP connection. Defaults to None.
            sftp_credentials (Dict[str, Any], optional): SFTP server credentials. Defaults to None.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # SFTP
        self.from_path = from_path
        self.sftp_credentials_secret = sftp_credentials_secret
        self.sftp_credentials = sftp_credentials
        self.columns = columns
        self.timeout = timeout

        # File args
        if file_name is None:
            self.file_name = from_path.split("/")[-1]

        else:
            self.file_name = file_name

        self.sep = sep
        self.remove_tab = remove_tab

        # ADLS
        self.to_path = to_path
        self.sp_credentials_secret = sp_credentials_secret
        self.vault_name = vault_name
        self.overwrite = overwrite

        super().__init__(
            name=name,
            *args,
            **kwargs,
        )

        self.gen_flow()

    def __call__(self, *args, **kwargs):
        """Download file-like object from SFTP server and load data into ADLS."""
        return super().__call__(*args, **kwargs)

    def gen_flow(self) -> Flow:
        ftp = SftpToDF(
            sftp_credentials_secret=self.sftp_credentials_secret,
            credentials=self.sftp_credentials,
            timeout=self.timeout,
        )
        df = ftp.bind(
            from_path=self.from_path,
            columns=self.columns,
            flow=self,
        )
        df_to_csv_task.bind(
            df=df, remove_tab=self.remove_tab, path=self.file_name, flow=self
        )

        upload_to_adls = AzureDataLakeUpload(timeout=self.timeout)
        upload_df = upload_to_adls.bind(
            from_path=self.file_name,
            to_path=self.to_path,
            sp_credentials_secret=self.sp_credentials_secret,
            overwrite=self.overwrite,
            vault_name=self.vault_name,
            flow=self,
        )

        df_to_csv_task.set_upstream(df, flow=self)
        upload_df.set_upstream(df_to_csv_task, flow=self)
