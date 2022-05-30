from prefect import Flow
from typing import Any, Dict, List, Literal

from ..tasks import EpicorOrdersToDF, DuckDBCreateTableFromParquet
from ..task_utils import df_to_parquet, add_ingestion_metadata_task


class EpicorOrdersToDuckDB(Flow):
    def __init__(
        self,
        name: str,
        base_url: str,
        filters_xml: str,
        local_file_path: str,
        epicor_credentials: Dict[str, Any] = None,
        epicor_config_key: str = None,
        start_date_field: str = "BegInvoiceDate",
        end_date_field: str = "EndInvoiceDate",
        duckdb_table: str = None,
        duckdb_schema: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
        duckdb_credentials: dict = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading orders data from Epicor API and uploading it to DuckDB using Parquet files.

        Args:
            base_url (str, required): Base url to Epicor Orders.
            filters_xml (str, required): Filters in form of XML. The date filter is required.
            local_file_path (str): The path to the source Parquet file.
            epicor_credentials (Dict[str, Any], optional): Credentials to connect with Epicor API containing host, port,
                username and password. Defaults to None.
            epicor_config_key (str, optional): Credential key to dictionary where details are stored. Defaults to None.
            start_date_field (str, optional) The name of filters field containing start date. Defaults to "BegInvoiceDate".
            end_date_field (str, optional) The name of filters field containing end date. Defaults to "EndInvoiceDate".
            duckdb_table (str, optional): Destination table in DuckDB. Defaults to None.
            duckdb_schema (str, optional): Destination schema in DuckDB. Defaults to None.
            if_exists (Literal, optional):  What to do if the table already exists. Defaults to "fail".
            duckdb_credentials (dict, optional): Credentials for the DuckDB connection. Defaults to None.
        """
        self.base_url = base_url
        self.epicor_credentials = epicor_credentials
        self.epicor_config_key = epicor_config_key
        self.filters_xml = filters_xml
        self.end_date_field = end_date_field
        self.start_date_field = start_date_field
        self.local_file_path = local_file_path
        self.duckdb_table = duckdb_table
        self.duckdb_schema = duckdb_schema
        self.if_exists = if_exists
        self.duckdb_credentials = duckdb_credentials

        super().__init__(*args, name=name, **kwargs)

        self.df_task = EpicorOrdersToDF(
            base_url=self.base_url,
            filters_xml=self.filters_xml,
        )
        self.create_duckdb_table_task = DuckDBCreateTableFromParquet(
            credentials=duckdb_credentials
        )

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df = self.df_task.bind(
            flow=self,
            credentials=self.epicor_credentials,
            config_key=self.epicor_config_key,
            end_date_field=self.end_date_field,
            start_date_field=self.start_date_field,
        )
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

        parquet = df_to_parquet.bind(
            df=df_with_metadata,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )
        create_duckdb_table = self.create_duckdb_table_task.bind(
            path=self.local_file_path,
            schema=self.duckdb_schema,
            table=self.duckdb_table,
            if_exists=self.if_exists,
            flow=self,
        )
        create_duckdb_table.set_upstream(parquet, flow=self)
