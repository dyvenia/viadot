import pytest
from pyrfc import Connection

from viadot.sources import SAPBW
from viadot.task_utils import credentials_loader

CREDENTIALS = credentials_loader.run(credentials_secret="SAP")
SAPBW = SAPBW(credentials=CREDENTIALS.get("BW"))


@pytest.fixture(scope="session")
def mdx_query_variable():
    mdx_query = """
        SELECT
                {
            }
                ON COLUMNS,
        NON EMPTY
                { 
                    { [0CALMONTH].[202301] } 
        } 
        DIMENSION PROPERTIES
        DESCRIPTION,
        MEMBER_NAME
        ON ROWS

        FROM ZCSALORD1/ZBW4_ZCSALORD1_006_BOA
    
                """
    yield mdx_query


@pytest.fixture(scope="session")
def wrong_mdx_query_variable():
    wrong_mdx_query = """
        SELECT
                {
            }
                ON COLUMNS,
        NON EMPTY
                { 
                    { [0CALMONTH].[202301]  *
        } 
        DIMENSION PROPERTIES
        DESCRIPTION,
        MEMBER_NAME
        ON ROWS

        FROM ZCSALORD1/ZBW4_ZCSALORD1_006_BOA
    
                """
    yield wrong_mdx_query


def test_get_connection():
    conn = SAPBW.get_connection()
    assert isinstance(conn, Connection)


def test_get_all_available_columns(mdx_query_variable):
    all_available_columns = SAPBW.get_all_available_columns(mdx_query_variable)
    assert isinstance(all_available_columns, list)


def test_get_output_data(mdx_query_variable):
    query_output = SAPBW.get_output_data(mdx_query_variable)
    assert isinstance(query_output, dict)


def test_wrong_mdx_query_for_get_output_data(wrong_mdx_query_variable):
    query_output = SAPBW.get_output_data(wrong_mdx_query_variable)
    assert (
        query_output.get("RETURN").get("MESSAGE")
        == "Syntax error: Syntax Error : ... { [0CALMONTH].[2023, row 226, item: 226"
    )
