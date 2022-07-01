import os

from viadot.config import local_config
from viadot.flows import EpicorOrdersToDuckDB
from viadot.tasks import DuckDBQuery, DuckDBToDF

TABLE = "test_epicor"
SCHEMA = "sandbox"
LOCAL_PATH = "test_epicor.parquet"


def test_epicor_to_duckdb():
    duckdb_creds = {"database": "/home/viadot/database/test.duckdb"}
    flow = EpicorOrdersToDuckDB(
        name="test",
        epicor_config_key="EPICOR",
        base_url=local_config.get("EPICOR").get("test_url"),
        filters_xml="""
    <OrderQuery>
        <QueryFields>
            <CompanyNumber>001</CompanyNumber>
            <BegInvoiceDate>2022-05-16</BegInvoiceDate>
            <EndInvoiceDate>2022-05-16</EndInvoiceDate>
            <RecordCount>3</RecordCount>
        </QueryFields>
    </OrderQuery>""",
        if_exists="replace",
        duckdb_table=TABLE,
        duckdb_schema=SCHEMA,
        duckdb_credentials=duckdb_creds,
        local_file_path=LOCAL_PATH,
    )

    result = flow.run()
    assert result.is_successful()

    df_task = DuckDBToDF(credentials=duckdb_creds)
    df = df_task.run(table=TABLE, schema=SCHEMA)

    assert df.shape == (24, 59)

    run_query = DuckDBQuery()
    run_query.run(query=f"DROP TABLE {SCHEMA}.{TABLE}", credentials=duckdb_creds)
    os.remove(LOCAL_PATH)
