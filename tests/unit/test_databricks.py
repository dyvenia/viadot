import pandas as pd
import pytest
from pyspark.sql.utils import AnalysisException

from viadot.exceptions import TableDoesNotExist
from viadot.sources import Databricks
from viadot.utils import add_viadot_metadata_columns

TEST_SCHEMA = "viadot_test_schema"
TEST_SCHEMA_2 = "viadot_test_schema_2"
TEST_TABLE = "test_table"
FQN = f"{TEST_SCHEMA}.{TEST_TABLE}"

SOURCE_DATA = [
    {
        "Id": "wRACnHTeuw",
        "AccountId": 123,
        "Name": "Scott-Merritt",
        "FirstName": "Melody",
        "LastName": "Cook",
        "ContactEmail": "Melody.Cook@ScottMerritt.com",
        "MailingCity": "Palo Alto",
    },
    {
        "Id": "CFfTlqagNlpDu",
        "AccountId": 456,
        "Name": "Mann-Warren",
        "FirstName": "Wayne",
        "LastName": "Morrison",
        "ContactEmail": "Wayne.Morrison@MannWarren.com",
        "MailingCity": "Bournemouth",
    },
]
TEST_DF = pd.DataFrame(SOURCE_DATA)
ADDITIONAL_TEST_DATA = [
    {
        "Id": "UpsertTest2",
        "AccountId": 789,
        "Name": "new upsert-2",
        "FirstName": "Updated",
        "LastName": "Carter2",
        "ContactEmail": "Adam.Carter@TurnerBlack.com",
        "MailingCity": "Updated!Jamestown",
        "NewField": "New field value",
    }
]
ADDITIONAL_DATA_NEW_FIELD_DF = pd.DataFrame(ADDITIONAL_TEST_DATA)
ADDITIONAL_DATA_DF = ADDITIONAL_DATA_NEW_FIELD_DF.copy().drop("NewField", axis=1)


@pytest.fixture(scope="session")
def databricks():

    databricks = Databricks(
        config_key="databricks-qa-elt",
    )

    try:
        databricks.drop_schema(TEST_SCHEMA)
        databricks.drop_table(TEST_TABLE)
    except:
        pass

    databricks.create_schema(TEST_SCHEMA)

    yield databricks

    try:
        databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    except TableDoesNotExist:
        pass
    try:
        databricks.drop_schema(TEST_SCHEMA)
    except:
        pass
    databricks.session.stop()


@pytest.mark.dependency()
def test_create_schema(databricks):

    try:
        databricks.drop_schema(TEST_SCHEMA_2)
    except AnalysisException:
        pass

    exists = databricks._check_if_schema_exists(TEST_SCHEMA_2)
    assert exists is False

    created = databricks.create_schema(TEST_SCHEMA_2)
    assert created is True

    exists = databricks._check_if_schema_exists(TEST_SCHEMA_2)
    assert exists is True

    try:
        databricks.drop_schema(TEST_SCHEMA_2)
    except:
        pass


@pytest.mark.dependency(depends=["test_create_schema"])
def test_drop_schema(databricks):

    exists = databricks._check_if_schema_exists(TEST_SCHEMA)
    assert exists is True

    dropped = databricks.drop_schema(TEST_SCHEMA)
    assert dropped is True

    exists = databricks._check_if_schema_exists(TEST_SCHEMA)
    assert exists is False

    databricks.create_schema(TEST_SCHEMA)


@pytest.mark.dependency()
def test_create_table(databricks):

    exists = databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert exists is False

    created = databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF
    )
    assert created is True

    exists = databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert exists is True

    # Cleanup.
    try:
        databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    except:
        pass


@pytest.mark.dependency(depends=["test_create_table"])
def test_drop_table(databricks):

    exists = databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert exists is False

    created = databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF
    )
    assert created is True

    dropped = databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert dropped is True

    exists = databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert exists is False


# @pytest.mark.dependency(depends=["test_create_table", "test_drop_table"])
def test_to_df(databricks):

    # Assumptions.
    exists = databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert exists is False

    databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="skip"
    )

    df = databricks.to_df(f"SELECT * FROM {FQN}")

    # Note that all `to_df()` methods are decorated with `@add_viadot_metadata_columns`.
    # This means that we need to add the metadata columns to the test DataFrame as well
    # before comparing the two.
    def fake_test_df_to_df():
        class Fake:
            @add_viadot_metadata_columns
            def to_df(self):
                return TEST_DF

        test_df = Fake().to_df()
        return test_df

    test_df = fake_test_df_to_df()
    assert df.shape == test_df.shape

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)


@pytest.mark.dependency()
def test_create_table_replace(databricks):

    # Setup.
    try:
        databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    except:
        pass

    exists = databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert exists is False

    created = databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF
    )
    assert created is True

    exists = databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert exists is True

    replaced = databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="replace"
    )
    assert replaced is True

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    databricks.drop_schema(TEST_SCHEMA)


def test_replace_different_column_schema(databricks):

    assert not databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    databricks.create_schema(TEST_SCHEMA)
    databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF
    )

    # Adds an additional column causing differences
    TEST_DF["extra_column"] = "test"
    replaced = databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="replace"
    )
    assert replaced is True

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    databricks.drop_schema(TEST_SCHEMA)


def test_snakecase_column_names(databricks):

    assert not databricks._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)

    # Calling the to_df() method without the wrapper adding metadata.
    to_df_no_metadata_cols = databricks.to_df.__wrapped__

    databricks.create_schema(TEST_SCHEMA)

    TEST_DF["Column TO   SNake   case"] = "test"

    # Creating a table, testing the case when the table does not exist.
    created = databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="replace"
    )

    assert created is True
    retrieved_value = to_df_no_metadata_cols(
        databricks, query=f"SELECT column_to___snake___case FROM {FQN}"
    )
    assert list(retrieved_value) == ["column_to___snake___case"]

    TEST_DF["Column TO   SNake   case 22"] = "test22"

    # Overwriting a table, testing the case when the table already exists.
    updated = databricks.create_table_from_pandas(
        schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="replace"
    )
    assert updated is True

    retrieved_value_update = to_df_no_metadata_cols(
        databricks, query=f"SELECT column_to___snake___case_22 FROM {FQN}"
    )
    assert list(retrieved_value_update) == ["column_to___snake___case_22"]

    databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)
    databricks.drop_schema(TEST_SCHEMA)


# @pytest.mark.dependency(depends=["test_create_table", "test_drop_table", "test_to_df"])
# def test_insert_into_append(databricks):

#     databricks.create_table_from_pandas(
#         schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="skip"
#     )

#     appended = databricks.insert_into(
#         schema=TEST_SCHEMA, table=TEST_TABLE, df=ADDITIONAL_DATA_DF
#     )
#     assert appended is True

#     expected_result = TEST_DF.copy().append(ADDITIONAL_DATA_DF)
#     result = databricks.to_df(f"SELECT * FROM {FQN}")
#     assert result.shape == expected_result.shape

#     databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)


# @pytest.mark.dependency(depends=["test_create_table", "test_drop_table", "test_to_df"])
# def test_insert_into_replace(databricks):

#     databricks.create_table_from_pandas(
#         schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="skip"
#     )

#     refreshed = databricks.insert_into(
#         schema=TEST_SCHEMA, table=TEST_TABLE, df=ADDITIONAL_DATA_DF, mode="replace"
#     )
#     assert refreshed is True

#     result = databricks.to_df(f"SELECT * FROM {FQN}")
#     assert result.shape == ADDITIONAL_DATA_DF.shape

#     databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)


# @pytest.mark.dependency(depends=["test_insert_into_append"])
# def test_insert_into_wrong_schema(databricks):
#     with pytest.raises(ValueError):
#         inserted = databricks.insert_into(
#             schema="test_incorrect_schema",
#             table=TEST_TABLE,
#             df=ADDITIONAL_DATA_DF,
#         )


# @pytest.mark.dependency(depends=["test_insert_into_append"])
# def test_insert_into_nonexistent_table(databricks):
#     with pytest.raises(ValueError):
#         inserted = databricks.insert_into(
#             schema=TEST_SCHEMA,
#             table="test_nonexistent_table",
#             df=ADDITIONAL_DATA_DF,
#             mode="append",
#         )


# @pytest.mark.dependency(depends=["test_create_table", "test_drop_table", "test_to_df"])
# def test_upsert(databricks):

#     databricks.create_table_from_pandas(
#         schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF
#     )

#     changed_record = copy.deepcopy(SOURCE_DATA[0])
#     changed_record["ContactEmail"] = "new_email@new_domain.com"
#     updated_data_df = pd.DataFrame([changed_record])
#     primary_key = "Id"

#     upserted = databricks.upsert(
#         schema=TEST_SCHEMA,
#         table=TEST_TABLE,
#         df=updated_data_df,
#         primary_key=primary_key,
#     )
#     assert upserted is True

#     expected_result = pd.DataFrame([changed_record, SOURCE_DATA[1]])
#     result = databricks.to_df(f"SELECT * FROM {FQN}")
#     assert result.shape == expected_result.shape

#     databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)


# @pytest.mark.dependency(depends=["test_create_table", "test_drop_table"])
# def test_discover_schema(databricks):

#     databricks.create_table_from_pandas(
#         schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF, if_exists="skip"
#     )

#     expected_schema = {
#         "Id": "string",
#         "AccountId": "bigint",
#         "Name": "string",
#         "FirstName": "string",
#         "LastName": "string",
#         "ContactEmail": "string",
#         "MailingCity": "string",
#     }
#     schema = databricks.discover_schema(schema=TEST_SCHEMA, table=TEST_TABLE)
#     assert schema == expected_schema

#     databricks.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)


# @pytest.mark.dependency(depends=["test_create_table", "test_drop_table", "test_to_df"])
# def test_rollback(databricks):

#     databricks.create_table_from_pandas(schema=TEST_SCHEMA, table=TEST_TABLE, df=TEST_DF)

#     # Get the version of the table before applying any changes
#     version_number = databricks.get_table_version(schema=TEST_SCHEMA, table=TEST_TABLE)

#     # Append to the table
#     appended = databricks.insert_into(
#         schema=TEST_SCHEMA, table=TEST_TABLE, df=ADDITIONAL_DATA_DF, mode="append"
#     )
#     assert appended

#     # Rollback to the previous table version
#     databricks.rollback(schema=TEST_SCHEMA, table=TEST_TABLE, version_number=version_number)
#     result = databricks.to_df(f"SELECT * FROM {FQN}")

#     assert df.shape == result.shape

#     databricks.drop_table(schema=SCHEMA, table=table)
