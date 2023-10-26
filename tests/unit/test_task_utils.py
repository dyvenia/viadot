import os
from typing import List
from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import ValidationError
from viadot.task_utils import (
    add_ingestion_metadata_task,
    adls_bulk_upload,
    anonymize_df,
    chunk_df,
    df_clean_column,
    df_converts_bytes_to_int,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json_task,
    union_dfs_task,
    validate_df,
    write_to_json,
)


class MockAzureUploadClass:
    def run(
        from_path: str = "",
        to_path: str = "",
        sp_credentials_secret: str = "",
        overwrite: bool = False,
    ) -> None:
        pass


def count_dtypes(dtypes_dict: dict = None, dtypes_to_count: List[str] = None) -> int:
    dtypes_counter = 0
    for v in dtypes_dict.values():
        if v in dtypes_to_count:
            dtypes_counter += 1
    return dtypes_counter


def test_add_ingestion_metadata_task():
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    result = add_ingestion_metadata_task.run(df)
    assert "_viadot_downloaded_at_utc" in result.columns


def test_add_ingestion_metadata_task_empty():
    df = pd.DataFrame()
    result = add_ingestion_metadata_task.run(df)
    assert result.empty


def test_add_ingestion_metadata_task_no_data():
    df = pd.DataFrame({"col1": []})
    result = add_ingestion_metadata_task.run(df)
    assert "_viadot_downloaded_at_utc" in result.columns


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
    data = {
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

    df = pd.DataFrame.from_dict(data)
    test_df = df_converts_bytes_to_int.run(df)
    lst = test_df["RKZ"][0]
    is_int = all(isinstance(x, (int, int)) for x in lst)
    assert is_int == True


def test_chunk_df():
    df = pd.DataFrame(
        {
            "AA": [1, 2, 3, 4, 5],
            "BB": [11, 22, 33, 4, 5],
            "CC": [4, 5, 6, 4, 5],
            "DD": [44, 55, 66, 1, 2],
        }
    )
    res = chunk_df.run(df=df, size=2)
    assert len(res) == 3


def test_df_get_data_types_task():
    df = pd.DataFrame(
        {
            "a": {0: "ann", 1: "test", 2: "Hello"},
            "b": {0: 9, 1: "2021-01-01", 2: "Hello"},
            "w": {0: 679, 1: "Hello", 2: "Hello"},
            "x": {0: -1, 1: 2, 2: 444},
            "y": {0: 1.5, 1: 11.97, 2: 56.999},
            "z": {0: "2022-01-01", 1: "2021-11-01", 2: "2021-01-01"},
        }
    )
    res = df_get_data_types_task.run(df)
    assert res == {
        "a": "String",
        "b": "Object",
        "w": "Object",
        "x": "Integer",
        "y": "Float",
        "z": "Date",
    }


def test_df_to_csv():
    df = pd.DataFrame(
        {
            "a": {0: "a", 1: "b", 2: "c"},
            "b": {0: "a", 1: "b", 2: "c"},
            "w": {0: "a", 1: "b", 2: "c"},
        }
    )

    df_to_csv.run(df, "test.csv")
    result = pd.read_csv("test.csv", sep="\t")
    assert df.equals(result)
    os.remove("test.csv")


def test_df_to_parquet():
    df = pd.DataFrame(
        {
            "a": {0: "a", 1: "b", 2: "c"},
            "b": {0: "a", 1: "b", 2: "c"},
            "w": {0: "a", 1: "b", 2: "c"},
        }
    )

    df_to_parquet.run(df, "test.parquet")
    result = pd.read_parquet("test.parquet")
    assert df.equals(result)
    os.remove("test.parquet")


def test_union_dfs_task():
    df1 = pd.DataFrame(
        {
            "a": {0: "a", 1: "b", 2: "c"},
            "b": {0: "a", 1: "b", 2: "c"},
            "w": {0: "a", 1: "b", 2: "c"},
        }
    )
    df2 = pd.DataFrame(
        {
            "a": {0: "d", 1: "e"},
            "b": {0: "d", 1: "e"},
        }
    )
    list_dfs = []
    list_dfs.append(df1)
    list_dfs.append(df2)
    res = union_dfs_task.run(list_dfs)
    assert isinstance(res, pd.DataFrame)
    assert len(res) == 5


def test_dtypes_to_json_task():
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    dtypes_to_json_task.run(dtypes_dict=dtypes, local_json_path="dtypes.json")
    assert os.path.exists("dtypes.json")
    os.remove("dtypes.json")


def test_write_to_json():
    dict = {"name": "John", 1: [2, 4, 3]}
    write_to_json.run(dict, "dict.json")
    assert os.path.exists("dict.json")
    os.remove("dict.json")


def test_df_clean_column_all():
    data = {
        "col_1": ["a", "b\\r", "\tc", "d \r\n a"],
        "col_2": ["a", "b\\r", "\tc", "d \r\n a"],
    }
    expected_output = {
        "col_1": {0: "a", 1: "b", 2: "c", 3: "d  a"},
        "col_2": {0: "a", 1: "b", 2: "c", 3: "d  a"},
    }
    df = pd.DataFrame.from_dict(data)
    output = df_clean_column.run(df).to_dict()
    assert expected_output == output


def test_df_clean_column_defined():
    data = {
        "col_1": ["a", "b", "c", "d  a"],
        "col_2": ["a\t\r", "b\\r", "\tc", "d \r\n a"],
    }
    expected_output = {
        "col_1": {0: "a", 1: "b", 2: "c", 3: "d  a"},
        "col_2": {0: "a", 1: "b", 2: "c", 3: "d  a"},
    }
    df = pd.DataFrame.from_dict(data)
    output = df_clean_column.run(df, ["col_2"]).to_dict()
    assert output == expected_output


@mock.patch("viadot.task_utils.AzureDataLakeUpload", return_value=MockAzureUploadClass)
@pytest.mark.bulk
def test_adls_bulk_upload(mock_upload):
    file_names = ["random_1.csv", "random_2.csv"]

    adls_bulk_upload.run(file_names=file_names, adls_file_path="any/at/random")
    mock_upload.assert_called_once()


def test_anonymize_df_all():
    data = pd.DataFrame(
        {
            "date_col": ["2005-01-01", "2010-01-02", "2023-01-03"],
            "email": [
                "john.malkovich@bing.com",
                "hannah.montana@disney.com",
                "ilovedogs33@dyvenia.com",
            ],
            "name": ["John", "Hannah", "Patryk"],
            "last_name": ["Malkovich", "Montana", "Dyvenian"],
            "active": [1, 0, 0],
        }
    )

    expected_output = {
        "date_col": {0: "2005-01-01", 1: "2010-01-02", 2: "2023-01-03"},
        "email": {0: "***", 1: "***", 2: "***"},
        "name": {0: "***", 1: "***", 2: "***"},
        "last_name": {0: "***", 1: "***", 2: "***"},
        "active": {0: 1, 1: 0, 2: 0},
    }
    output = anonymize_df.run(data, ["email", "name", "last_name"]).to_dict()
    assert output == expected_output


def test_anonymize_df_hash_defined_threshold_date():
    data = pd.DataFrame(
        {
            "date_col": ["2005-01-01", "2010-01-02", "2023-01-03"],
            "email": [
                "john.malkovich@bing.com",
                "hannah.montana@disney.com",
                "ilovedogs33@dyvenia.com",
            ],
            "name": ["John", "Hannah", "Patryk"],
            "last_name": ["Malkovich", "Montana", "Dyvenian"],
            "active": [1, 0, 0],
        }
    )

    expected_output = {
        "date_col": {0: "2005-01-01", 1: "2010-01-02", 2: "2023-01-03"},
        "email": {
            0: hash("john.malkovich@bing.com"),
            1: hash("hannah.montana@disney.com"),
            2: "ilovedogs33@dyvenia.com",
        },
        "name": {0: hash("John"), 1: hash("Hannah"), 2: "Patryk"},
        "last_name": {0: hash("Malkovich"), 1: hash("Montana"), 2: "Dyvenian"},
        "active": {0: 1, 1: 0, 2: 0},
    }

    output = anonymize_df.run(
        data,
        ["email", "name", "last_name"],
        method="hash",
        date_column="date_col",
        days=2 * 365,
    ).to_dict()
    assert output == expected_output


def test_anonymize_df_various_date_formats():
    data = pd.DataFrame(
        {
            "date_col1": ["2021-01-01", "2022-01-02", "2023-01-03"],
            "date_col2": [
                "2021-01-01 10:00:00",
                "2022-01-02 11:30:00",
                "2023-01-03 12:45:00",
            ],
            "date_col3": ["01/01/2021", "02/01/2022", "03/01/2023"],
            "date_col4": ["20210101", "20220102", "20230103"],
            "date_col5": [
                "2021-01-01T20:17:46.384Z",
                "2022-01-02T20:17:46.384Z",
                "2023-01-03T20:17:46.384Z",
            ],
            "date_col6": [
                "2021-01-01T20:17:46.384",
                "2022-01-02T20:17:46.384",
                "2023-01-03T20:17:46.384",
            ],
            "email": [
                "john.malkovich@bing.com",
                "hannah.montana@disney.com",
                "ilovedogs33@dyvenia.com",
            ],
            "active": [1, 0, 0],
        }
    )

    output1 = anonymize_df.run(
        data, ["email"], value=None, date_column="date_col1", days=2 * 365
    ).to_dict()
    output2 = anonymize_df.run(
        data, ["email"], value=None, date_column="date_col2", days=2 * 365
    ).to_dict()
    output3 = anonymize_df.run(
        data, ["email"], value=None, date_column="date_col3", days=2 * 365
    ).to_dict()
    output4 = anonymize_df.run(
        data, ["email"], value=None, date_column="date_col4", days=2 * 365
    ).to_dict()
    output5 = anonymize_df.run(
        data, ["email"], value=None, date_column="date_col5", days=2 * 365
    ).to_dict()
    output6 = anonymize_df.run(
        data, ["email"], value=None, date_column="date_col6", days=2 * 365
    ).to_dict()
    assert output1 == output2 == output3 == output4 == output5 == output6


def test_wrong_column():
    data = pd.DataFrame(
        {
            "date_col": ["2005-01-01", "2010-01-02", "2023-01-03"],
            "email": [
                "john.malkovich@bing.com",
                "hannah.montana@disney.com",
                "ilovedogs33@dyvenia.com",
            ],
            "name": ["John", "Hannah", "Patryk"],
            "last_name": ["Malkovich", "Montana", "Dyvenian"],
            "active": [1, 0, 0],
        }
    )
    with pytest.raises(ValueError, match="column names"):
        anonymize_df.run(data, ["first_name", "last_name", "email"])


def test_wrong_method():
    data = pd.DataFrame(
        {
            "date_col": ["2005-01-01", "2010-01-02", "2023-01-03"],
            "email": [
                "john.malkovich@bing.com",
                "hannah.montana@disney.com",
                "ilovedogs33@dyvenia.com",
            ],
            "name": ["John", "Hannah", "Patryk"],
            "last_name": ["Malkovich", "Montana", "Dyvenian"],
            "active": [1, 0, 0],
        }
    )
    with pytest.raises(ValueError, match="Method not found"):
        anonymize_df.run(data, ["name", "last_name", "email"], method="anonymize")


def test_validate_df_column_size_pass():
    df = pd.DataFrame({"col1": ["a", "bb", "ccc"]})
    tests = {"column_size": {"col1": 3}}
    try:
        validate_df.run(df, tests)
    except ValidationError:
        assert False, "Validation failed but was expected to pass"


def test_validate_df_column_size_fail():
    df = pd.DataFrame({"col1": ["a", "bb", "cccc"]})
    tests = {"column_size": {"col1": 3}}
    with pytest.raises(ValidationError):
        validate_df.run(df, tests)


def test_validate_df_column_unique_values_pass():
    df = pd.DataFrame({"col1": [1, 2, 3]})
    tests = {"column_unique_values": ["col1"]}
    try:
        validate_df.run(df, tests)
    except ValidationError:
        assert False, "Validation failed but was expected to pass"


def test_validate_df_column_unique_values_fail():
    df = pd.DataFrame({"col1": [1, 2, 2]})
    tests = {"column_unique_values": ["col1"]}
    with pytest.raises(ValidationError):
        validate_df.run(df, tests)


def test_validate_df_column_list_to_match_pass():
    df = pd.DataFrame({"col1": [1], "col2": [2]})
    tests = {"column_list_to_match": ["col1", "col2"]}
    try:
        validate_df.run(df, tests)
    except ValidationError:
        assert False, "Validation failed but was expected to pass"


def test_validate_df_column_list_to_match_fail():
    df = pd.DataFrame({"col1": [1]})
    tests = {"column_list_to_match": ["col1", "col2"]}
    with pytest.raises(ValidationError):
        validate_df.run(df, tests)


def test_validate_df_dataset_row_count_pass():
    df = pd.DataFrame({"col1": [1, 2, 3]})
    tests = {"dataset_row_count": {"min": 1, "max": 5}}
    try:
        validate_df.run(df, tests)
    except ValidationError:
        assert False, "Validation failed but was expected to pass"


def test_validate_df_dataset_row_count_fail():
    df = pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6]})
    tests = {"dataset_row_count": {"min": 1, "max": 5}}
    with pytest.raises(ValidationError):
        validate_df.run(df, tests)


def test_validate_df_column_match_regex_pass():
    df = pd.DataFrame({"col1": ["A12", "B34", "C45"]})
    tests = {"column_match_regex": {"col1": "^[A-Z][0-9]{2}$"}}
    try:
        validate_df.run(df, tests)
    except ValidationError:
        assert False, "Validation failed but was expected to pass"


def test_validate_df_column_match_regex_fail():
    df = pd.DataFrame({"col1": ["A123", "B34", "C45"]})
    tests = {"column_match_regex": {"col1": "^[A-Z][0-9]{2}$"}}
    with pytest.raises(ValidationError):
        validate_df.run(df, tests)


def test_validate_df_column_sum_pass():
    df = pd.DataFrame({"col1": [1, 2, 3]})
    tests = {"column_sum": {"col1": {"min": 5, "max": 10}}}
    try:
        validate_df.run(df, tests)
    except ValidationError:
        assert False, "Validation failed but was expected to pass"


def test_validate_df_column_sum_fail():
    df = pd.DataFrame({"col1": [1, 2, 3, 4]})
    tests = {"column_sum": {"col1": {"min": 5, "max": 6}}}
    with pytest.raises(ValidationError):
        validate_df.run(df, tests)
