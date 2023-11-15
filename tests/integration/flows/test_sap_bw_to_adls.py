import os
from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import ValidationError
from viadot.flows import SAPBWToADLS

DATA = {
    "[0CALMONTH].[LEVEL01].[DESCRIPTION]": ["January 2023"],
    "date": ["2023-06-19 11:12:43+00:00"],
}

ADLS_FILE_NAME = "test_sap_bw_to_adls.parquet"
ADLS_DIR_PATH = "raw/tests/"


@mock.patch(
    "viadot.tasks.SAPBWToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sap_bw_to_adls_flow_run(mocked_class):
    flow = SAPBWToADLS(
        "test_sap_bw_to_adls_flow_run",
        sapbw_credentials_key="SAP",
        env="BW",
        mdx_query="""
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
        
                    """,
        mapping_dict={"[0CALMONTH].[LEVEL01].[DESCRIPTION]": "Calendar Year/Month"},
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_sap_bw_to_adls_flow_run.parquet")
    os.remove("test_sap_bw_to_adls_flow_run.json")


def test_sap_bw_validate_df_task_success(caplog):
    flow = SAPBWToADLS(
        "test_sap_bw_validate_df_task_success",
        mdx_query="""
            SELECT
            { [Measures].[003YPR4ERQZRMSMFEQ8123HRR] } ON COLUMNS,
            NON EMPTY
            { 
                { [0CALDAY].[20230102] } *
                            { 
                    [0COMP_CODE].[1120] 
                    }  
            } 
            ON ROWS
        FROM ZCSALBIL1/ZBW4_ZCSALBIL1_002_BOA
        """,
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        validate_df_dict={
            "column_size": {"[0CALDAY].[LEVEL01].[MEMBER_CAPTION]": 10},
            "column_list_to_match": [
                "[0CALDAY].[LEVEL01].[MEMBER_CAPTION]",
                "[0COMP_CODE].[LEVEL01].[MEMBER_CAPTION]",
                "[Measures].[003YPR4ERQZRMSMFEQ8123HRR]",
            ],
        },
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_sap_bw_validate_df_task_success.parquet")
    os.remove("test_sap_bw_validate_df_task_success.json")


def test_sap_bw_validate_df_task_fail(caplog):
    flow = SAPBWToADLS(
        "test_sap_bw_validate_df_task_fail",
        mdx_query="""
            SELECT
            { [Measures].[003YPR4ERQZRMSMFEQ8123HRR] } ON COLUMNS,
            NON EMPTY
            { 
                { [0CALDAY].[20230102] } *
                            { 
                    [0COMP_CODE].[1120] 
                    }  
            } 
            ON ROWS
        FROM ZCSALBIL1/ZBW4_ZCSALBIL1_002_BOA
        """,
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        validate_df_dict={
            "column_size": {"[0CALDAY].[LEVEL01].[MEMBER_CAPTION]": 8},
            "column_list_to_match": [
                "[0CALDAY].[LEVEL01].[MEMBER_CAPTION]",
                "[0COMP_CODE].[LEVEL01].[MEMBER_CAPTION]",
            ],
        },
    )

    try:
        flow.run()
    except ValidationError:
        pass

    os.remove("test_sap_bw_validate_df_task_fail.parquet")
    os.remove("test_sap_bw_validate_df_task_fail.json")
