import pandas as pd
import pytest

from viadot.task_utils import credentials_loader
from viadot.tasks import SAPBWToDF

CREDENTIALS = credentials_loader.run(credentials_secret="SAP")
sapbw_task = SAPBWToDF(sapbw_credentials=CREDENTIALS.get("BW"))


@pytest.fixture(scope="session")
def output_variable():
    output = {
        "RETURN": {
            "TYPE": "",
            "ID": "",
            "NUMBER": "000",
            "MESSAGE": "",
            "LOG_NO": "",
            "LOG_MSG_NO": "000000",
            "MESSAGE_V1": "",
            "MESSAGE_V2": "",
            "MESSAGE_V3": "",
            "MESSAGE_V4": "",
            "PARAMETER": "",
            "ROW": 0,
            "FIELD": "",
            "SYSTEM": "",
        },
        "STATISTIC": {"STEP": "003YPR44RQTVS3BSMZTKDYBMD"},
        "DATA": [
            {
                "COLUMN": 0,
                "ROW": 0,
                "DATA": "January 2023",
                "VALUE_DATA_TYPE": "CHAR",
                "CELL_STATUS": "",
            },
            {
                "COLUMN": 1,
                "ROW": 0,
                "DATA": "202301",
                "VALUE_DATA_TYPE": "NUMC",
                "CELL_STATUS": "",
            },
        ],
        "HEADER": [
            {
                "COLUMN": 0,
                "ROW": 0,
                "DATA": "[0CALMONTH].[LEVEL01].[DESCRIPTION]",
                "VALUE_DATA_TYPE": "CHAR",
                "CELL_STATUS": "",
            },
            {
                "COLUMN": 1,
                "ROW": 0,
                "DATA": "[0CALMONTH].[LEVEL01].[MEMBER_NAME]",
                "VALUE_DATA_TYPE": "CHAR",
                "CELL_STATUS": "",
            },
        ],
    }
    yield output


@pytest.fixture(scope="session")
def user_mapping():
    mapping = {
        "[0CALMONTH].[LEVEL01].[DESCRIPTION]": "Calendar Year/Month",
        "[0CALMONTH].[LEVEL01].[MEMBER_NAME]": "Calendar Year/Month key",
    }
    yield mapping


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


df_to_test = pd.DataFrame(
    data={
        "[0CALMONTH].[LEVEL01].[DESCRIPTION]": ["January 2023"],
        "[0CALMONTH].[LEVEL01].[MEMBER_NAME]": ["202301"],
        "date": ["2023-06-19 11:12:43+00:00"],
    },
)


def test_apply_user_mapping(user_mapping):
    apply_mapping = sapbw_task.apply_user_mapping(df_to_test, user_mapping)
    print(user_mapping.values())
    assert list(apply_mapping.columns) == list(user_mapping.values())
    assert isinstance(apply_mapping, pd.DataFrame)


def test_to_df(output_variable):
    df = sapbw_task.to_df(output_variable)
    assert isinstance(df, pd.DataFrame)


def test_run(mdx_query_variable, user_mapping):
    df = sapbw_task.run(mdx_query_variable, user_mapping)
    assert isinstance(df, pd.DataFrame)
