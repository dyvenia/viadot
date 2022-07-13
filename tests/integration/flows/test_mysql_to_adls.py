from unittest import mock

from viadot.flows.mysql_to_adls import MySqlToADLS

query = """SELECT * FROM `example-views`.`sales`"""


def test_instance_mysqltoadls():
    flow = MySqlToADLS("test_flow", country_short="DE")
    assert flow


def test_adls_gen1_to_azure_sql_new_mock(TEST_PARQUET_FILE_PATH):
    with mock.patch.object(MySqlToADLS, "run", return_value=True) as mock_method:
        flow = MySqlToADLS(
            "test_flow_de",
            country_short="DE",
            query=query,
            file_path=TEST_PARQUET_FILE_PATH,
            to_path=f"raw/examples/{TEST_PARQUET_FILE_PATH}",
            sp_credentials_secret="App-Azure-CR-DatalakeGen2-AIA-DEV",
            overwrite_adls=True,
        )
        flow.run()
        mock_method.assert_called_with()
