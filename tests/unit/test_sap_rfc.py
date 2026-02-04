from collections import OrderedDict
import importlib.util
from unittest.mock import patch

from pandas import DataFrame
import pendulum
import pytest

from viadot.utils import skip_test_on_missing_extra


try:
    from viadot.sources import SAPRFC
except ImportError:
    skip_test_on_missing_extra(source_name="SAPRFC", extra="sap")

credentials = {"a": "b"}

sap = SAPRFC(credentials=credentials)

sql1 = "SELECT a AS a_renamed, b FROM table1 WHERE table1.c = 1"
sql2 = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) LIMIT 5 OFFSET 3"
sql3 = "SELECT b FROM c WHERE testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
sql4 = "SELECT c FROM d WHERE testLIMIT = 1 AND testOFFSET = 2 AND LIMITtest=3 AND OFFSETtest=4"
sql5 = sql3 + " AND longword123=5"
sql6 = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) AND longcolname=3 AND otherlongcolname=5 LIMIT 5 OFFSET 3"
sql7 = """
SELECT a, b
FROM b
WHERE c = 1
AND d = 2
AND longcolname = 12345
AND otherlongcolname = 6789
AND thirdlongcolname = 01234
LIMIT 5
OFFSET 10
"""


def test__get_table_name():
    assert sap._get_table_name(sql1) == "table1"
    assert sap._get_table_name(sql2) == "fake_schema.fake_table", sap._get_table_name(
        sql2
    )
    assert sap._get_table_name(sql7) == "b"


def test__get_columns():
    assert sap._get_columns(sql1) == ["a", "b"]
    assert sap._get_columns(sql1, aliased=True) == ["a_renamed", "b"], sap._get_columns(
        sql1, aliased=True
    )
    assert sap._get_columns(sql2) == ["a"]
    assert sap._get_columns(sql7) == ["a", "b"]


def test__get_where_condition():
    assert sap._get_where_condition(sql1) == "table1.c = 1", sap._get_where_condition(
        sql1
    )
    assert (
        sap._get_where_condition(sql2) == "a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2)"
    ), sap._get_where_condition(sql2)
    assert (
        sap._get_where_condition(sql3)
        == "testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
    ), sap._get_where_condition(sql3)
    assert (
        sap._get_where_condition(sql4)
        == "testLIMIT = 1 AND testOFFSET = 2 AND LIMITtest=3 AND OFFSETtest=4"
    ), sap._get_where_condition(sql4)
    assert (
        sap._get_where_condition(sql7)
        == "c = 1 AND d = 2 AND longcolname = 12345 AND otherlongcolname = 6789"
    ), sap._get_where_condition(sql7)


def test__get_limit():
    assert sap._get_limit(sql1) is None
    assert sap._get_limit(sql2) == 5
    assert sap._get_limit(sql7) == 5


def test__get_offset():
    assert sap._get_offset(sql1) is None
    assert sap._get_offset(sql2) == 3
    assert sap._get_offset(sql7) == 10


def test_client_side_filters_simple():
    _ = sap._get_where_condition(sql5)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "longword123=5"}
    ), sap.client_side_filters


def test_client_side_filters_with_limit_offset():
    _ = sap._get_where_condition(sql6)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "otherlongcolname=5"}
    ), sap.client_side_filters

    _ = sap._get_where_condition(sql7)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "thirdlongcolname = 01234"}
    ), sap.client_side_filters


def test___build_pandas_filter_query():
    _ = sap._get_where_condition(sql6)
    assert (
        sap._build_pandas_filter_query(sap.client_side_filters)
        == "otherlongcolname == 5"
    ), sap._build_pandas_filter_query(sap.client_side_filters)
    _ = sap._get_where_condition(sql7)
    assert (
        sap._build_pandas_filter_query(sap.client_side_filters)
        == "thirdlongcolname == 01234"
    ), sap._build_pandas_filter_query(sap.client_side_filters)


def test__adjust_whitespaces():
    sap.rfc_unique_id = ["column1", "column2"]
    sap._rfc_unique_id_len = {"column1": 5, "column2": 4}
    data = {"column1": ["xyz  ", "oiu"], "column2": ["yrt ", "lkj"]}
    df = DataFrame(data)
    df = sap._adjust_whitespaces(df)
    col_values_len = df.applymap(lambda x: len(x))
    check_if_length_match = col_values_len == sap._rfc_unique_id_len.values()
    assert check_if_length_match.all().all()


def test_parse_dates_success():
    """Test `_parse_dates` function."""
    query = "SELECT * FROM table WHERE date_column = <<today>>"
    result = sap._parse_dates(query)
    assert (
        result
        == f"SELECT * FROM table WHERE date_column = {pendulum.today().strftime('%Y%m%d')}"  # noqa: S608
    )


def test_parse_dates_raises_typeerror():
    """Test `_parse_dates` for not supported dynamic date type."""
    query = "SELECT * FROM table WHERE date_column in (<<last_3_years>>)"

    with pytest.raises(TypeError):
        sap._parse_dates(query)


def test_to_df_returns_dataframe_with_mocked_call():
    """Test if returns a DataFrame with expected columns and viadot metadata."""
    sap.client_side_filters = None
    sap.extract_values(sql1)
    sap._query = {"FIELDS": [["a", "b"]], "DELIMITER": "|"}
    mock_response = {"DATA": [{"WA": "val1|val2"}, {"WA": "val3|val4"}]}

    with (
        patch.object(SAPRFC, "call", return_value=mock_response),
        patch.object(SAPRFC, "close_connection"),
    ):
        df = sap.to_df()

    assert isinstance(df, DataFrame)
    assert list(df.columns) == [
        "a_renamed",
        "b",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]
    assert len(df) == 2
    assert list(df["a_renamed"]) == ["val1", "val3"]
    assert list(df["b"]) == ["val2", "val4"]
    assert df["_viadot_source"].iloc[0] == "SAPRFC"


def test_to_arrow_returns_table_with_mocked_call():
    """Test if returns an Arrow table with expected columns and viadot metadata."""
    if importlib.util.find_spec("pyarrow") is None:
        pytest.skip("pyarrow not installed")
    import pyarrow as pa

    sap.client_side_filters = None
    sap.extract_values(sql1)
    sap._query = {"FIELDS": [["a", "b"]], "DELIMITER": "|"}
    mock_response = {"DATA": [{"WA": "foo|bar"}, {"WA": "baz|qux"}]}

    with (
        patch.object(SAPRFC, "call", return_value=mock_response),
        patch.object(SAPRFC, "close_connection"),
    ):
        table = sap.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.column_names == [
        "a_renamed",
        "b",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]
    assert table.num_rows == 2
    assert table.column("a_renamed").to_pylist() == ["foo", "baz"]
    assert table.column("b").to_pylist() == ["bar", "qux"]
    assert table.column("_viadot_source").to_pylist()[0] == "SAPRFC"


def test_to_arrow_with_tests_calls_validate():
    """Test to_arrow(tests=...) runs validation on the table (same as to_df)."""
    if importlib.util.find_spec("pyarrow") is None:
        pytest.skip("pyarrow not installed")

    sap.client_side_filters = None
    sap.extract_values(sql1)
    sap._query = {"FIELDS": [["a", "b"]], "DELIMITER": "|"}
    mock_response = {"DATA": [{"WA": "x|y"}, {"WA": "z|w"}]}
    tests = {"dataset_row_count": {"min": 1, "max": 10}}

    with (
        patch.object(SAPRFC, "call", return_value=mock_response),
        patch.object(SAPRFC, "close_connection"),
    ):
        table = sap.to_arrow(tests=tests)

    assert table.num_rows == 2
    # Validation passed (no exception); dataset_row_count 1-10 is satisfied
