from operator import ne
from typing import Any, Dict, List, Literal

from prefect import Flow
from prefect.utilities import logging
from prefect.backend import set_key_value

logger = logging.get_logger()

from viadot.task_utils import (
    add_ingestion_metadata_task,
    cast_df_to_str,
    df_to_parquet,
    set_new_kv,
)
from viadot.tasks import DuckDBCreateTableFromParquet, SAPRFCToDF


class SAPToDuckDB(Flow):
    def __init__(
        self,
        query: str,
        table: str,
        local_file_path: str,
        func: str = "RFC_READ_TABLE",
        rfc_total_col_width_character_limit: int = 400,
        name: str = None,
        sep: str = None,
        schema: str = None,
        table_if_exists: Literal[
            "fail", "replace", "append", "skip", "delete"
        ] = "fail",
        if_empty: Literal["warn", "skip", "fail"] = "skip",
        sap_credentials: dict = None,
        duckdb_credentials: dict = None,
        update_kv: bool = False,
        filter_column: str = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """A flow for moving data from SAP to DuckDB.

        Args:
            query (str): The query to be executed on SAP with pyRFC.
            table (str): Destination table in DuckDB.
            local_file_path (str): The path to the source Parquet file.
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            rfc_total_col_width_character_limit (int, optional): Number of characters by which query will be split in
            chunks in case of too many columns for RFC function. According to SAP documentation, the
            limit is 512 characters. However, we observed SAP raising an exception even on a slightly
            lower number of characters, so we add a safety margin. Defaults to 400.
            name (str, optional): The name of the flow. Defaults to None.
            sep (str, optional): The separator to use when reading query results. If not provided,
            multiple options are automatically tried. Defaults to None.
            schema (str, optional): Destination schema in DuckDB. Defaults to None.
            table_if_exists (Literal, optional):  What to do if the table already exists. Defaults to "fail".
            if_empty (Literal, optional): What to do if Parquet file is empty. Defaults to "skip".
            sap_credentials (dict, optional): The credentials to use to authenticate with SAP.
            By default, they're taken from the local viadot config.
            duckdb_credentials (dict, optional): The config to use for connecting with DuckDB. Defaults to None.
            update_kv (bool, optional): Whether or not to update key value on Prefect. Defaults to False.
            filter_column (str, optional): Name of the field based on which key value will be updated. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        # SAPRFCToDF
        self.query = query
        self.func = func
        self.rfc_total_col_width_character_limit = rfc_total_col_width_character_limit
        self.sep = sep
        self.sap_credentials = sap_credentials

        # DuckDBCreateTableFromParquet
        self.table = table
        self.schema = schema
        self.if_exists = table_if_exists
        self.if_empty = if_empty
        self.local_file_path = local_file_path or self.slugify(name) + ".parquet"
        self.duckdb_credentials = duckdb_credentials
        self.update_kv = update_kv
        self.filter_column = filter_column

        super().__init__(*args, name=name, **kwargs)

        self.sap_to_df_task = SAPRFCToDF(credentials=sap_credentials, timeout=timeout)
        self.create_duckdb_table_task = DuckDBCreateTableFromParquet(
            credentials=duckdb_credentials, timeout=timeout
        )

        self.gen_flow()

    def gen_flow(self) -> Flow:

        df = self.sap_to_df_task.bind(
            query=self.query,
            sep=self.sep,
            func=self.func,
            rfc_total_col_width_character_limit=self.rfc_total_col_width_character_limit,
            flow=self,
        )

        df_mapped = cast_df_to_str.bind(df, flow=self)
        df_with_metadata = add_ingestion_metadata_task.bind(df_mapped, flow=self)
        parquet = df_to_parquet.bind(
            df=df_with_metadata,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )

        table = self.create_duckdb_table_task.bind(
            path=self.local_file_path,
            schema=self.schema,
            table=self.table,
            if_exists=self.if_exists,
            if_empty=self.if_empty,
            flow=self,
        )

        table.set_upstream(parquet, flow=self)

        if self.update_kv == True:
            set_new_kv.bind(
                kv_name=self.name,
                df=df,
                filter_column=self.filter_column,
                flow=self,
            )
            set_new_kv.set_upstream(table, flow=self)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()
