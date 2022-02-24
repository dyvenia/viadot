import pytest
import pandas as pd
from typing import List
import numpy as np
from viadot.task_utils import (
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_converts_bytes_to_int,
)


def count_dtypes(dtypes_dict: dict = None, dtypes_to_count: List[str] = None) -> int:
    dtypes_counter = 0
    for v in dtypes_dict.values():
        if v in dtypes_to_count:
            dtypes_counter += 1
    return dtypes_counter


def test_map_dtypes_for_parquet():
    df = pd.DataFrame(
        {
            "a": {0: 55.7, 1: "Hello", 2: "Hello"},
            "b": {0: "Start", 1: "Hello", 2: "Hello"},
            "w": {0: 679, 1: "Hello", 2: "Hello"},
            "x": {0: 1, 1: 2, 2: 444},
            "y": {0: 1.5, 1: 11.97, 2: 56.999},
            "z": {0: "Start", 1: 1, 2: "2021-01-01"},
        }
    )
    dtyps_dict = df_get_data_types_task.run(df)
    sum_of_dtypes = count_dtypes(dtyps_dict, ["Object", "String"])

    df_map = df_map_mixed_dtypes_for_parquet.run(df, dtyps_dict)
    dtyps_dict_mapped = df_get_data_types_task.run(df_map)
    sum_of_mapped_dtypes = count_dtypes(dtyps_dict_mapped, ["String"])

    assert sum_of_dtypes == sum_of_mapped_dtypes


def test_df_converts_bytes_to_int():
    dane = {
        "ID": {0: 1, 1: 2, 2: 100, 3: 101, 4: 102},
        "SpracheText": {
            0: "TE_CATALOG_BASE_LANG",
            1: "TE_Docu",
            2: "TE_German",
            3: "TE_English",
            4: "TE_French",
        },
        "RKZ": {
            0: b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\r\x01\x00\x00\x00\x00\x00\x00\x04\x00\x00q\x9f#NV\x8dG\x00",
            1: b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\r\x01\x00\x00\x00\x00\x00\x00\x04\x00\x00<\xa0#NV\x8dG\x00",
            2: b"\r\xa3\x86\x01\x00\x01\x00\x00\x00\x00\x04\r\x01\x00\x00\x00\x00\x00\x00\x04\x00\x003\x9f#NV\x8dG\x00",
            3: b"\r\xa3\x86\x01\x00\x01\x00\x00\x00\x00\x04\r\x01\x00\x00\x00\x00\x00\x00\x04\x00\x00R\x9f#NV\x8dG\x00",
            4: b"\r\xa3\x86\x01\x00\x01\x00\x00\x00\x00\x04\r\x01\x00\x00\x00\x00\x00\x00\x04\x00\x00\xee\x9f#NV\x8dG\x00",
        },
    }

    df = pd.DataFrame.from_dict(dane)
    test_df = df_converts_bytes_to_int.run(df)
    lst = test_df["RKZ"][0]
    is_it_or_not = all(isinstance(x, (int, int)) for x in lst)
    assert is_it_or_not == True
