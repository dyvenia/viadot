from collections import OrderedDict
from viadot.sources import SAPRFC

import pytest
import pandas as pd


@pytest.fixture(scope="session")
def sap():
    sap = SAPRFC(config_key="sap_rfc")
    yield sap


simple_select_sql = "SELECT a AS a_renamed, b FROM table1 WHERE table1.c = 1"
sql_limit_offset = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) LIMIT 5 OFFSET 3"
sql_faked_limit_offset = "SELECT b FROM c WHERE testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
sql_faked_limit_offset_2 = "SELECT c FROM d WHERE testLIMIT = 1 AND testOFFSET = 2 AND LIMITtest=3 AND OFFSETtest=4"
too_long_sql_faked_limit_offset = sql_faked_limit_offset + " AND longword123=5"
too_long_sql = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) AND longcolname=3 AND otherlongcolname=5 LIMIT 5 OFFSET 3"
multiple_line_too_long_sql = """
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
customer_name_insert_sql = "INSERT INTO customers name VALUES 'John Doe'"
all_rows_sql = " SELECT * FROM table1"
too_long_sql_OR_statment = "SELECT a FROM fake_schema.fake_table WHERE a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) AND longcolname=3 OR otherlongcolname=5 LIMIT 5 OFFSET 3"
multiple_table_sql = "SELECT col FROM table1, table2"


# Testing "get_function_parameters" function which returns SAP RFC functions parameters
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


# Testing "_get_table_name" function which returns table name from the query
def test__get_table_name(sap):
    assert sap._get_table_name(simple_select_sql) == "table1"
    assert (
        sap._get_table_name(sql_limit_offset) == "fake_schema.fake_table"
    ), sap._get_table_name(sql_limit_offset)
    assert sap._get_table_name(sql_faked_limit_offset) == "c"
    assert sap._get_table_name(sql_faked_limit_offset_2) == "d"
    assert sap._get_table_name(too_long_sql_faked_limit_offset) == "c"
    assert sap._get_table_name(too_long_sql) == "fake_schema.fake_table"
    assert sap._get_table_name(multiple_line_too_long_sql) == "b"
    with pytest.raises(
        ValueError, match="Querying more than one table is not supported."
    ):
        sap._get_table_name(multiple_tables_sql)


# Testing "_get_columns" function which returns all columns name from the query
def test__get_columns(sap):
    assert sap._get_columns(simple_select_sql) == ["a", "b"]
    assert sap._get_columns(simple_select_sql, aliased=True) == [
        "a_renamed",
        "b",
    ], sap._get_columns(simple_select_sql, aliased=True)
    assert sap._get_columns(sql_limit_offset) == ["a"]
    assert sap._get_columns(multiple_line_too_long_sql) == ["a", "b"]


# Testing "_get_where_condition" function which returns WHERE clause trimmed to 75 characters
def test__get_where_condition(sap):
    assert (
        sap._get_where_condition(simple_select_sql) == "table1.c = 1"
    ), sap._get_where_condition(simple_select_sql)
    assert (
        sap._get_where_condition(sql_limit_offset)
        == "a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2)"
    ), sap._get_where_condition(sql_limit_offset)
    assert (
        sap._get_where_condition(sql_faked_limit_offset)
        == "testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
    ), sap._get_where_condition(sql_faked_limit_offset)
    assert (
        sap._get_where_condition(sql_faked_limit_offset_2)
        == "testLIMIT = 1 AND testOFFSET = 2 AND LIMITtest=3 AND OFFSETtest=4"
    ), sap._get_where_condition(sql_faked_limit_offset_2)
    assert (
        sap._get_where_condition(too_long_sql_faked_limit_offset)
        == "testORword=1 AND testANDword=2 AND testLIMITword=3 AND testOFFSETword=4"
    ), sap._get_where_condition(too_long_sql_faked_limit_offset)
    assert (
        sap._get_where_condition(too_long_sql)
        == "a=1 AND b=2 OR c LIKE 'a%' AND d IN (1, 2) AND longcolname=3"
    ), sap._get_where_condition(too_long_sql)
    assert (
        sap._get_where_condition(multiple_line_too_long_sql)
        == "c = 1 AND d = 2 AND longcolname = 12345 AND otherlongcolname = 6789"
    ), sap._get_where_condition(multiple_line_too_long_sql)
    assert (
        sap._get_where_condition(customer_name_insert_sql) == None
    ), sap._get_where_condition(customer_name_insert_sql)
    assert sap._get_where_condition(all_rows_sql) == None, sap._get_where_condition(
        all_rows_sql
    )
    with pytest.raises(
        ValueError,
        match="WHERE conditions after the 75 character limit can only be combined with the AND keyword.",
    ):
        sap._get_where_condition(too_long_sql_OR_statment)


#  Testing "_get_limit" function which returns parameters from LIMIT clause
def test__get_limit(sap):
    assert sap._get_limit(simple_select_sql) is None
    assert sap._get_limit(sql_limit_offset) == 5
    assert sap._get_limit(multiple_line_too_long_sql) == 5


#  Testing "_get_offset" function which returns parameters from OFFSET clause
def test__get_offset(sap):
    assert sap._get_offset(simple_select_sql) is None
    assert sap._get_offset(sql_limit_offset) == 3
    assert sap._get_offset(multiple_line_too_long_sql) == 10


# Testing "_client_side_filter" function which returns Dict with last AND clause {"AND": "statment"}
def test_client_side_filters_simple(sap):
    _ = sap._get_where_condition(too_long_sql_faked_limit_offset)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "longword123=5"}
    ), sap.client_side_filters


# Testing "_client_side_filter" function around OFFSET/LIMIT clause which returns Dict with last AND clause {"AND": "statment"}
def test_client_side_filters_with_limit_offset(sap):
    _ = sap._get_where_condition(too_long_sql)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "otherlongcolname=5"}
    ), sap.client_side_filters
    _ = sap._get_where_condition(multiple_line_too_long_sql)
    assert sap.client_side_filters == OrderedDict(
        {"AND": "thirdlongcolname = 01234"}
    ), sap.client_side_filters


# Testing "_build_pandas_filter_query" function which returns the WHERE clause reformatted to fit the format required by DataFrame.query()
def test___build_pandas_filter_query(sap):
    _ = sap._get_where_condition(too_long_sql_faked_limit_offset)
    assert (
        sap._build_pandas_filter_query(sap.client_side_filters) == "longword123 == 5"
    ), sap._build_pandas_filter_query(sap.client_side_filters)
    _ = sap._get_where_condition(too_long_sql)
    assert (
        sap._build_pandas_filter_query(sap.client_side_filters)
        == "otherlongcolname == 5"
    ), sap._build_pandas_filter_query(sap.client_side_filters)
    _ = sap._get_where_condition(multiple_line_too_long_sql)
    assert (
        sap._build_pandas_filter_query(sap.client_side_filters)
        == "thirdlongcolname == 01234"
    ), sap._build_pandas_filter_query(sap.client_side_filters)


# Testing "_query" function which returns prased SQL query into pyRFC commands and save it into an internal dictionary
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
        sap.query(customer_name_insert_sql)


# Testing "to_df" function which returns pd.DataFrame: A DataFrame representing the result of the query provided in `PyRFC.query()`
def test_to_df(sap):
    sap = SAPRFC(config_key="sap_rfc")
    sap.query(sql="SELECT ERSDA FROM MARA LIMIT 3", sep="|")
    assert (
        pd.DataFrame.to_json(sap.to_df())
        == '{"ERSDA":{"0":"20101105","1":"20101105","2":"20160411"}}'
    )
