import os

from viadot.config import local_config

try:
    import pyrfc
except ModuleNotFoundError:
    raise

from viadot.flows import SAPToDuckDB

sap_test_creds = local_config.get("SAP").get("TEST")
duckdb_creds = {"database": "test1.duckdb"}


def test_sap_to_duckdb():
    flow = SAPToDuckDB(
        name="SAPToDuckDB flow test",
        query="""
        select
        ,CLIENT as client
        ,KNUMV as number_of_the_document_condition
        ,KPOSN as condition_item_number
        ,STUNR as step_number
        ,KAPPL as application
        from PRCD_ELEMENTS
        where KNUMV = '2003393196'
            and KPOSN = '000001'
            or STUNR = '570'
            and CLIENT = '009'
        limit 3
        """,
        schema="main",
        table="test",
        local_file_path="local.parquet",
        table_if_exists="replace",
        sap_credentials=sap_test_creds,
        duckdb_credentials=duckdb_creds,
    )

    result = flow.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])

    os.remove("test1.duckdb")
