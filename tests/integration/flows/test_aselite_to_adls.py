from typing import Any, Dict, List, Literal
from prefect import Flow
from viadot.tasks import AzureDataLakeUpload
from prefect.run_configs import DockerRun
from viadot.task_utils import df_to_csv, df_converts_bytes_to_int
from viadot.tasks.aselite import ASELiteToDF
import logging
from viadot.flows.aselite_to_adls import ASELitetoADLS
from prefect.tasks.secrets import PrefectSecret
import pandas as pd
import os

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

    RUN_CONFIG = DockerRun(
        image="docker.pkg.github.com/dyvenia/viadot/viadot:latest",
        labels=["prod"],
    )

    flow = ASELitetoADLS(
        "Test flow ",
        query=query_designer,
        sqldb_credentials_secret="AIA-ASELITE-QA",
        vault_name="azuwevelcrkeyv001s",
        file_path=TMP_FILE_NAME,
        to_path="raw/supermetrics/mp/result_df_flow_at_des_m.csv",
        run_config=RUN_CONFIG,
    )

    result = flow.run()
    assert result.is_successful()


def test_generated_csv_file():

    MAIN_DF = pd.read_csv(TMP_FILE_NAME, delimiter="\t")

    if isinstance(MAIN_DF, pd.DataFrame):
        assert True

    assert MAIN_DF.shape == (10, 17)

    os.remove(TMP_FILE_NAME)
