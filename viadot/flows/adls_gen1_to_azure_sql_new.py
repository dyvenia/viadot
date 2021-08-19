from typing import Any, Dict, List

import pandas as pd
from prefect import Flow
from prefect.utilities import logging
from prefect.utilities.tasks import task

from viadot.flows.adls_to_azure_sql import df_to_csv_task
from viadot.task_utils import METADATA_COLUMNS, add_ingestion_metadata_task

from ..tasks import AzureDataLakeToDF, AzureDataLakeUpload, AzureSQLCreateTable, BCPTask

gen1_to_df_task = AzureDataLakeToDF(gen=1)
gen2_upload_task = AzureDataLakeUpload(gen=2)
create_table_task = AzureSQLCreateTable()
bulk_insert_task = BCPTask()

logger = logging.get_logger(__name__)


@task
def df_to_csv_task(df: pd.DataFrame, path: str, sep: str = "\t") -> None:
    df.to_csv(path, index=False, sep=sep)


@task
def df_replace_special_chars(df: pd.DataFrame) -> None:
    df = df.replace(r"\n|\t", "", regex=True)
    return df


class ADLSGen1ToAzureSQLNew(Flow):
    """Move file(s) from Azure Data Lake gen1 to gen2.

    Args:
        name (str): The name of the flow.
        gen1_path (str): The path to the gen1 Data Lake file/folder.
        gen2_path (str): The path of the final gen2 file/folder.
        local_file_path (str): Where the gen1 file should be downloaded.
        overwrite (str): Whether to overwrite the destination file(s).
        read_sep (str): The delimiter for the gen1 file.
        write_sep (str): The delimiter for the output file.
        read_quoting (str): The quoting option for the input file.
        read_lineterminator (str): The line terminator for the input file.
        read_error_bad_lines (bool): Whether to raise an exception on bad lines.
        gen1_sp_credentials_secret (str): The Key Vault secret holding Service Pricipal credentials for gen1 lake
        gen2_sp_credentials_secret (str): The Key Vault secret holding Service Pricipal credentials for gen2 lake
        sqldb_credentials_secret (str): The Key Vault secret holding Azure SQL Database credentials
        vault_name (str): The name of the vault from which to retrieve `sp_credentials_secret`
    """

    def __init__(
        self,
        name: str,
        gen1_path: str,
        gen2_path: str,
        local_file_path: str = None,
        overwrite: bool = True,
        read_sep: str = "\t",
        write_sep: str = "\t",
        read_quoting: str = None,
        read_lineterminator: str = None,
        read_error_bad_lines: bool = True,
        schema: str = None,
        table: str = None,
        dtypes: dict = None,
        if_exists: str = "fail",
        gen1_sp_credentials_secret: str = None,
        gen2_sp_credentials_secret: str = None,
        sqldb_credentials_secret: str = None,
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.gen1_path = gen1_path
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.gen2_path = gen2_path
        self.overwrite = overwrite
        self.read_sep = read_sep
        self.write_sep = write_sep
        self.read_quoting = read_quoting
        self.read_lineterminator = read_lineterminator
        self.read_error_bad_lines = read_error_bad_lines
        self.schema = schema
        self.table = table
        self.dtypes = dtypes
        self.if_exists = if_exists
        self.gen1_sp_credentials_secret = gen1_sp_credentials_secret
        self.gen2_sp_credentials_secret = gen2_sp_credentials_secret
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.vault_name = vault_name
        super().__init__(*args, name=name, **kwargs)
        self.dtypes.update(METADATA_COLUMNS)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        df = gen1_to_df_task.bind(
            path=self.gen1_path,
            gen=1,
            lineterminator=self.read_lineterminator,
            sep=self.read_sep,
            quoting=self.read_quoting,
            error_bad_lines=self.read_error_bad_lines,
            sp_credentials_secret=self.gen1_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        df2 = df_replace_special_chars.bind(df=df, flow=self)
        df_with_metadata = add_ingestion_metadata_task.bind(df=df2, flow=self)
        df_to_csv_task.bind(
            df=df_with_metadata,
            path=self.local_file_path,
            sep=self.write_sep,
            flow=self,
        )
        gen2_upload_task.bind(
            from_path=self.local_file_path,
            to_path=self.gen2_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.gen2_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        create_table_task.bind(
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            if_exists=self.if_exists,
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        bulk_insert_task.bind(
            path=self.local_file_path,
            schema=self.schema,
            table=self.table,
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        df_with_metadata.set_upstream(df_replace_special_chars, flow=self)
        df_to_csv_task.set_upstream(df_with_metadata, flow=self)
        gen2_upload_task.set_upstream(df_to_csv_task, flow=self)
        create_table_task.set_upstream(df_to_csv_task, flow=self)
        bulk_insert_task.set_upstream(create_table_task, flow=self)
