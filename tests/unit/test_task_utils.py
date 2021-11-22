import pytest
import pandas as pd
from typing import List

from viadot.task_utils import df_get_data_types_task, df_map_mixed_dtypes_for_parquet


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
