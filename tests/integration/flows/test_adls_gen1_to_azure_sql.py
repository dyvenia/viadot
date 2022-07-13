from unittest import mock

from viadot.flows import ADLSGen1ToAzureSQL


def test_adls_gen1_to_azure_sql_new_init(
    TEST_CSV_FILE_BLOB_PATH, TEST_PARQUET_FILE_PATH
):
    instance = ADLSGen1ToAzureSQL(
        name="test_adls_gen1_azure_sql_flow",
        path=TEST_PARQUET_FILE_PATH,
        blob_path=TEST_CSV_FILE_BLOB_PATH,
        schema="sandbox",
        table="test_bcp",
        dtypes={"country": "VARCHAR(25)", "sales": "INT"},
        if_exists="replace",
    )
    assert instance


def test_adls_gen1_to_azure_sql_new_mock(
    TEST_CSV_FILE_BLOB_PATH, TEST_PARQUET_FILE_PATH
):
    with mock.patch.object(ADLSGen1ToAzureSQL, "run", return_value=True) as mock_method:
        instance = ADLSGen1ToAzureSQL(
            name="test_adls_gen1_azure_sql_flow",
            path=TEST_PARQUET_FILE_PATH,
            blob_path=TEST_CSV_FILE_BLOB_PATH,
            schema="sandbox",
            table="test_bcp",
            dtypes={"country": "VARCHAR(25)", "sales": "INT"},
            if_exists="replace",
        )
        instance.run()
        mock_method.assert_called_with()
