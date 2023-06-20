import pytest
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


def test_get_all_available_columns(mdx_query_variable):
    all_available_columns = SAPBW.get_all_available_columns(mdx_query_variable)
    assert isinstance(all_available_columns, list)


def test_get_output_data(mdx_query_variable):
    columns = SAPBW.get_output_data(mdx_query_variable)
    assert isinstance(columns, list)
