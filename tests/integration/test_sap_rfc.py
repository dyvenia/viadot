from viadot.sources import SAPRFC
from collections import OrderedDict

sap = SAPRFC()

sql1 = "SELECT a AS a_renamed, b FROM table1 WHERE table1.c = 1"
sql2 = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) LIMIT 5 OFFSET 3"
sql3 = "SELECT b FROM c WHERE testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
sql4 = "SELECT c FROM d WHERE testLIMIT = 1 AND testOFFSET = 2 AND LIMITtest=3 AND OFFSETtest=4"
sql5 = sql3 + " AND longword123=5"
sql6 = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) AND longcolname=3 AND otherlongcolname=5 LIMIT 5 OFFSET 3"


def test__get_table_name():
    assert sap._get_table_name(sql1) == "table1"
    assert sap._get_table_name(sql2) == "fake_schema.fake_table", sap._get_table_name(
        sql2
    )


def test__get_columns():
    assert sap._get_columns(sql1) == ["a", "b"]
    assert sap._get_columns(sql1, aliased=True) == ["a_renamed", "b"], sap._get_columns(
        sql1, aliased=True
    )
    assert sap._get_columns(sql2) == ["a"]


def test__get_where_condition():
    assert sap._get_where_condition(sql1) == "table1.c = 1", sap._get_where_condition(
        sql1
    )
    print(sap._get_where_condition(sql2))
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


def test__get_limit():
    assert sap._get_limit(sql1) is None
    assert sap._get_limit(sql2) == 5


def test__get_offset():
    assert sap._get_offset(sql1) is None
    assert sap._get_offset(sql2) == 3


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
