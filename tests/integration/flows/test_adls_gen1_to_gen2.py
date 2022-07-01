from unittest import mock

from viadot.flows import ADLSGen1ToGen2


def test_adls_gen1_gen2_init(TEST_PARQUET_FILE_PATH_2):

    flow = ADLSGen1ToGen2(
        "test_adls_gen1_gen2_init",
        gen1_path=TEST_PARQUET_FILE_PATH_2,
        gen2_path=TEST_PARQUET_FILE_PATH_2,
    )
    assert flow


def test_adls_gen1_to_azure_sql_new_mock(
    TEST_PARQUET_FILE_PATH, TEST_PARQUET_FILE_PATH_2
):
    with mock.patch.object(ADLSGen1ToGen2, "run", return_value=True) as mock_method:
        instance = ADLSGen1ToGen2(
            "test_adls_gen1_gen2_init",
            gen1_path=TEST_PARQUET_FILE_PATH,
            gen2_path=TEST_PARQUET_FILE_PATH_2,
        )
        instance.run()
        mock_method.assert_called_with()
