import pytest
import os
import pathlib
import pandas as pd

from .sources import Sharepoint
from .tasks import SharepointToDF
from .flows import sharepoint_to_adls as s_flow


s = Sharepoint()
excel_to_df_task = SharepointToDF()

file_name = 'EUL Data.xlsm'
df_v_n = pd.read_excel(file_name, sheet_name=2)


def test_connection():
    s_connect = s.check_connection()
    status = s_connect.get('https://velux.sharepoint.com/sites/EULNORDIC/Shared%20Documents/Dashboard')
    assert str(status) == '<Response [200]>'


def test_file_extension():
    file_ext = ['.xlsm','.xlsx']
    assert pathlib.Path(s.url_to_file).suffix in file_ext


def test_file_name():
    assert os.path.basename(s.url_to_file) == file_name


def test_file_download():
    s.download_file(filename = file_name)
    files = []
    for file in os.listdir():
        if os.path.isfile(os.path.join(file)):
            files.append(file)
    assert file_name in files


def test_file_to_df():
    df_test = pd.DataFrame(data={'col1': [1, 2]})
    assert type(df_v_n) == type(df_test)


def test_get_data_types():
    dtypes_map = s_flow.df_get_data_types_task.run(df_v_n)
    dtypes = [v for k,v in dtypes_map.items()]
    assert 'String' in dtypes


def test_map_dtypes_for_parquet(): 
    dtyps_dict = s_flow.df_get_data_types_task.run(df_v_n)
    df_map = s_flow.df_mapp_mixed_dtypes_for_parquet_task.run(df_v_n, dtyps_dict)
    sum_df_cols = df_v_n.select_dtypes(include = ['object', 'string']).columns.value_counts().sum()
    sum_df_map_cols = df_map.select_dtypes(include = 'string').columns.value_counts().sum()
    assert sum_df_cols == sum_df_map_cols
