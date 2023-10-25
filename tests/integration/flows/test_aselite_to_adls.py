import logging
import os
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Flow
from prefect.run_configs import DockerRun
from prefect.tasks.secrets import PrefectSecret

from viadot.flows.aselite_to_adls import ASELiteToADLS
from viadot.task_utils import df_converts_bytes_to_int, df_to_csv, validate_df
from viadot.tasks import AzureDataLakeUpload
from viadot.tasks.aselite import ASELiteToDF

TMP_FILE_NAME = "test_flow.csv"
MAIN_DF = None

df_task = ASELiteToDF()
file_to_adls_task = AzureDataLakeUpload()


def test_aselite_to_adls():
    credentials_secret = PrefectSecret("aselite").run()
    vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()

    query_designer = """SELECT TOP 10 [ID]
        ,[SpracheText]
        ,[SpracheKat]
        ,[SpracheMM]
        ,[KatSprache]
        ,[KatBasisSprache]
        ,[CodePage]
        ,[Font]
        ,[Neu]
        ,[Upd]
        ,[UpdL]
        ,[LosKZ]
        ,[AstNr]
        ,[KomKz]
        ,[RKZ]
        ,[ParentLanguageNo]
        ,[UPD_FIELD]
    FROM [UCRMDEV].[dbo].[CRM_00]"""

    flow = ASELiteToADLS(
        "Test flow",
        query=query_designer,
        sqldb_credentials_secret=credentials_secret,
        vault_name=vault_name,
        file_path=TMP_FILE_NAME,
        to_path="raw/supermetrics/mp/result_df_flow_at_des_m.csv",
        run_config=None,
    )

    result = flow.run()
    assert result.is_successful()

    MAIN_DF = pd.read_csv(TMP_FILE_NAME, delimiter="\t")

    assert isinstance(MAIN_DF, pd.DataFrame) == True

    assert MAIN_DF.shape == (10, 17)

    os.remove(TMP_FILE_NAME)


def test_aselite_to_adls_validate_success():
    credentials_secret = PrefectSecret("aselite").run()
    vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()

    query_designer = """SELECT TOP 10 [ID]
        ,[SpracheText]
        ,[SpracheKat]
        ,[SpracheMM]
        ,[KatSprache]
        ,[KatBasisSprache]
        ,[CodePage]
        ,[Font]
        ,[Neu]
        ,[Upd]
        ,[UpdL]
        ,[LosKZ]
        ,[AstNr]
        ,[KomKz]
        ,[RKZ]
        ,[ParentLanguageNo]
        ,[UPD_FIELD]
    FROM [UCRMDEV].[dbo].[CRM_00]"""

    validate_df_dict = {
        "column_size": {"ParentLanguageNo": 1},
        "column_unique_values": ["ID"],
        "dataset_row_count": {"min": 0, "max": 10},
        "column_match_regex": {"SpracheText", r"TE_.*"},
    }

    flow = ASELiteToADLS(
        "Test flow",
        query=query_designer,
        sqldb_credentials_secret=credentials_secret,
        vault_name=vault_name,
        file_path=TMP_FILE_NAME,
        validate_df_dict=validate_df_dict,
        to_path="raw/supermetrics/mp/result_df_flow_at_des_m.csv",
        run_config=None,
    )

    result = flow.run()
    assert result.is_successful()

    MAIN_DF = pd.read_csv(TMP_FILE_NAME, delimiter="\t")

    assert isinstance(MAIN_DF, pd.DataFrame) == True

    assert MAIN_DF.shape == (10, 17)

    os.remove(TMP_FILE_NAME)
