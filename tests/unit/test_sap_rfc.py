from collections import OrderedDict

from viadot.sources import SAPRFC

import pytest
import pandas as pd


@pytest.fixture(scope="session")
def sap():
    sap = SAPRFC(config_key="sap_rfc")
    yield sap


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
sql8 = "INSERT INTO customers name VALUES 'John Doe'"
sql9 = " SELECT * FROM table1"
sql10 = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) AND longcolname=3 OR otherlongcolname=5 LIMIT 5 OFFSET 3"
sql11 = "SELECT col FROM table1, table2"


def test_get_function_parameters(sap):
    assert sap.get_function_parameters("RFC_READ_TABLE", description=None) == [
        "ET_DATA",
        "DELIMITER",
        "GET_SORTED",
        "NO_DATA",
        "QUERY_TABLE",
        "ROWCOUNT",
        "ROWSKIPS",
        "USE_ET_DATA_4_RETURN",
        "DATA",
        "FIELDS",
        "OPTIONS",
    ]
    assert (
        pd.DataFrame.to_json(sap.get_function_parameters("RFC_READ_TABLE", "short"))
        == '{"name":{"0":"ET_DATA","1":"DELIMITER","2":"GET_SORTED","3":"NO_DATA","4":"QUERY_TABLE","5":"ROWCOUNT","6":"ROWSKIPS","7":"USE_ET_DATA_4_RETURN","8":"DATA","9":"FIELDS","10":"OPTIONS"},"parameter_type":{"0":"RFCTYPE_TABLE","1":"RFCTYPE_CHAR","2":"RFCTYPE_CHAR","3":"RFCTYPE_CHAR","4":"RFCTYPE_CHAR","5":"RFCTYPE_INT","6":"RFCTYPE_INT","7":"RFCTYPE_CHAR","8":"RFCTYPE_TABLE","9":"RFCTYPE_TABLE","10":"RFCTYPE_TABLE"},"default_value":{"0":"","1":"SPACE","2":"","3":"SPACE","4":"","5":"0","6":"0","7":"","8":"","9":"","10":""},"optional":{"0":false,"1":true,"2":true,"3":true,"4":false,"5":true,"6":true,"7":true,"8":true,"9":true,"10":true},"parameter_text":{"0":"","1":"Sign for indicating field limits in DATA","2":"","3":"If <> SPACE, only FIELDS is filled","4":"Table read","5":"","6":"","7":"","8":"Data read (out)","9":"Names (in) and structure (out) of fields read","10":"Selection entries, \\"WHERE clauses\\" (in)"}}'
    )
    with pytest.raises(
        ValueError,
        match="Incorrect value for 'description'. Correct values",
    ):
        sap.get_function_parameters("RFC_READ_TABLE", "x")


def test__get_table_name(sap):
    assert sap._get_table_name(sql1) == "table1"
    assert sap._get_table_name(sql2) == "fake_schema.fake_table", sap._get_table_name(
        sql2
    )
    assert sap._get_table_name(sql3) == "c"
    assert sap._get_table_name(sql4) == "d"
    assert sap._get_table_name(sql5) == "c"
    assert sap._get_table_name(sql6) == "fake_schema.fake_table"
    assert sap._get_table_name(sql7) == "b"
    with pytest.raises(
        ValueError, match="Querying more than one table is not supported."
    ):
        sap._get_table_name(sql11)


def test__get_columns(sap):
    assert sap._get_columns(sql1) == ["a", "b"]
    assert sap._get_columns(sql1, aliased=True) == ["a_renamed", "b"], sap._get_columns(
        sql1, aliased=True
    )
    assert sap._get_columns(sql2) == ["a"]
    assert sap._get_columns(sql7) == ["a", "b"]


def test__get_where_condition(sap):
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
        sap._get_where_condition(sql5)
        == "testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
    ), sap._get_where_condition(sql5)
    assert (
        sap._get_where_condition(sql6)
        == "a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) AND longcolname=3"
    ), sap._get_where_condition(sql6)
    assert (
        sap._get_where_condition(sql7)
        == "c = 1 AND d = 2 AND longcolname = 12345 AND otherlongcolname = 6789"
    ), sap._get_where_condition(sql7)
    assert sap._get_where_condition(sql8) == None, sap._get_where_condition(sql8)
    assert sap._get_where_condition(sql9) == None, sap._get_where_condition(sql9)
    with pytest.raises(
        ValueError,
        match="WHERE conditions after the 75 character limit can only be combined with the AND keyword.",
    ):
        sap._get_where_condition(sql10)


def test__get_limit(sap):
    assert sap._get_limit(sql1) is None
    assert sap._get_limit(sql2) == 5
    assert sap._get_limit(sql7) == 5


def test__get_offset(sap):
    assert sap._get_offset(sql1) is None
    assert sap._get_offset(sql2) == 3
    assert sap._get_offset(sql7) == 10


def test_client_side_filters_simple(sap):
    _ = sap._get_where_condition(sql5)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "longword123=5"}
    ), sap.client_side_filters


def test_client_side_filters_with_limit_offset(sap):
    _ = sap._get_where_condition(sql6)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "otherlongcolname=5"}
    ), sap.client_side_filters
    _ = sap._get_where_condition(sql7)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "thirdlongcolname = 01234"}
    ), sap.client_side_filters


def test___build_pandas_filter_query(sap):
    _ = sap._get_where_condition(sql5)
    assert (
        sap._build_pandas_filter_query(sap.client_side_filters) == "longword123 == 5"
    ), sap._build_pandas_filter_query(sap.client_side_filters)
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


def test_query(sap):
    sap = SAPRFC(config_key="sap_rfc")
    sap.query(sql="SELECT NAMEV FROM KNVK LIMIT 3", sep="|")
    assert sap._query == {
        "QUERY_TABLE": "KNVK",
        "FIELDS": [["NAMEV"]],
        "ROWCOUNT": 3,
        "DELIMITER": "|",
    }
    with pytest.raises(ValueError, match="Only SELECT queries are supported."):
        sap.query(sql8)


def test_to_df(sap):
    sap = SAPRFC(config_key="sap_rfc")
    sap.query(sql="SELECT ERSDA FROM MARA LIMIT 3", sep="|")
    assert (
        pd.DataFrame.to_json(sap.to_df())
        == '{"ERSDA":{"0":"20181107","1":"20170713","2":"20170713"}}'
    )
