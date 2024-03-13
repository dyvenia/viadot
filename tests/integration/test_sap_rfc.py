from collections import OrderedDict

from viadot.sources import SAPRFC, SAPRFCV2

sap = SAPRFC()
sap2 = SAPRFCV2()

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


def test__get_table_name_v2():
    assert sap2._get_table_name(sql1) == "table1"
    assert sap2._get_table_name(sql2) == "fake_schema.fake_table", sap2._get_table_name(
        sql2
    )
    assert sap2._get_table_name(sql7) == "b"


def test__get_columns_v2():
    assert sap2._get_columns(sql1) == ["a", "b"]
    assert sap2._get_columns(sql1, aliased=True) == [
        "a_renamed",
        "b",
    ], sap2._get_columns(sql1, aliased=True)
    assert sap2._get_columns(sql2) == ["a"]
    assert sap2._get_columns(sql7) == ["a", "b"]


def test__get_where_condition_v2():
    assert sap2._get_where_condition(sql1) == "table1.c = 1", sap2._get_where_condition(
        sql1
    )
    assert (
        sap2._get_where_condition(sql2) == "a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2)"
    ), sap2._get_where_condition(sql2)
    assert (
        sap2._get_where_condition(sql3)
        == "testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
    ), sap2._get_where_condition(sql3)
    assert (
        sap2._get_where_condition(sql4)
        == "testLIMIT = 1 AND testOFFSET = 2 AND LIMITtest=3 AND OFFSETtest=4"
    ), sap2._get_where_condition(sql4)
    assert (
        sap2._get_where_condition(sql7)
        == "c = 1 AND d = 2 AND longcolname = 12345 AND otherlongcolname = 6789"
    ), sap2._get_where_condition(sql7)


def test__get_limit_v2():
    assert sap2._get_limit(sql1) is None
    assert sap2._get_limit(sql2) == 5
    assert sap2._get_limit(sql7) == 5


def test__get_offset_v2():
    assert sap2._get_offset(sql1) is None
    assert sap2._get_offset(sql2) == 3
    assert sap2._get_offset(sql7) == 10


def test_client_side_filters_simple_v2():
    _ = sap2._get_where_condition(sql5)
    assert sap2.client_side_filters == OrderedDict(
        {"AND": "longword123=5"}
    ), sap2.client_side_filters


def test_client_side_filters_with_limit_offset_v2():
    _ = sap2._get_where_condition(sql6)
    assert sap2.client_side_filters == OrderedDict(
        {"AND": "otherlongcolname=5"}
    ), sap2.client_side_filters

    _ = sap2._get_where_condition(sql7)
    assert sap2.client_side_filters == OrderedDict(
        {"AND": "thirdlongcolname = 01234"}
    ), sap2.client_side_filters


def test___build_pandas_filter_query_v2():
    _ = sap2._get_where_condition(sql6)
    assert (
        sap2._build_pandas_filter_query(sap2.client_side_filters)
        == "otherlongcolname == 5"
    ), sap2._build_pandas_filter_query(sap2.client_side_filters)
    _ = sap2._get_where_condition(sql7)
    assert (
        sap2._build_pandas_filter_query(sap2.client_side_filters)
        == "thirdlongcolname == 01234"
    ), sap2._build_pandas_filter_query(sap2.client_side_filters)
