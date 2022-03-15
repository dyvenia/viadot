from viadot.task_utils import generate_table_dtypes

SCHEMA = "sandbox"
TABLE = "test"


def test_generate_dtypes():
    test_dict = generate_table_dtypes.run(
        config_key="AZURE_SQL", table_name=TABLE, schema=SCHEMA
    )

    assert isinstance(test_dict, dict)
