from typing import Any, Dict, List, Literal
from prefect import Flow
from prefect.utilities import logging

logger = logging.get_logger()

from ..task_utils import (
    add_ingestion_metadata_task,
    df_to_parquet,
)
from ..tasks import SAPRFCToDF, DuckDBCreateTableFromParquet


class SAPToDuckDB(Flow):
    def __init__(
        self,
        query: str,
        table: str,
        local_file_path: str,
        func: str = "RFC_READ_TABLE",
        name: str = None,
        sep: str = None,
        schema: str = None,
        table_if_exists: Literal[
            "fail", "replace", "append", "skip", "delete"
        ] = "fail",
        sap_credentials: dict = None,
        duckdb_credentials: dict = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """A flow for moving data from SAP to DuckDB.

        Args:
            query (str): The query to be executed on SAP with pyRFC.
            table (str): Destination table in DuckDB.
            local_file_path (str): The path to the source Parquet file.
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            name (str, optional): The name of the flow. Defaults to None.
            sep (str, optional): The separator to use when reading query results. If not provided,
            multiple options are automatically tried. Defaults to None.
            schema (str, optional): Destination schema in DuckDB. Defaults to None.
            table_if_exists (Literal, optional):  What to do if the table already exists. Defaults to "fail".
            sap_credentials (dict, optional): The credentials to use to authenticate with SAP.
            By default, they're taken from the local viadot config.
            duckdb_credentials (dict, optional): The config to use for connecting with DuckDB. Defaults to None.
        """

        # SAPRFCToDF
        self.query = query
        self.func = func
        self.sep = sep
        self.sap_credentials = sap_credentials

        # DuckDBCreateTableFromParquet
        self.table = table
        self.schema = schema
        self.if_exists = table_if_exists
        self.local_file_path = local_file_path or self.slugify(name) + ".parquet"
        self.duckdb_credentials = duckdb_credentials

        super().__init__(*args, name=name, **kwargs)

        self.sap_to_df_task = SAPRFCToDF(credentials=sap_credentials)
        self.create_duckdb_table_task = DuckDBCreateTableFromParquet(
            credentials=duckdb_credentials
        )

        self.gen_flow()

    def gen_flow(self) -> Flow:

        df = self.sap_to_df_task.bind(
            query=self.query,
            sep=self.sep,
            func=self.func,
            flow=self,
        )

        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

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
            flow=self,
        )

        table.set_upstream(parquet, flow=self)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()
