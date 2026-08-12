"""'test_salesforce.py'."""

import pandas as pd
import pytest
from simple_salesforce import Salesforce as SimpleSalesforce

from viadot.sources import Salesforce
from viadot.sources.salesforce import SalesforceCredentials


variables = {
    "credentials": {
        "consumer_key": "test_user",
        "consumer_secret": "test_password",  # pragma: allowlist secret
    },
    "records_1": [
        {
            "Id": "001",
            "Name": "Test Record",
            "attributes": {
                "type": "Account",
                "url": "/services/data/v50.0/sobjects/Account/001",
            },
        },
    ],
    "records_2": [
        {
            "Id": "001",
            "Name": "Test Record",
            "attributes": {
                "type": "Account",
                "url": "/services/data/v50.0/sobjects/Account/001",
            },
        },
    ],
    "data": [
        {
            "Id": "001",
            "Name": "Test Record",
            "attributes": {
                "type": "Account",
                "url": "/services/data/v50.0/sobjects/Account/001",
            },
        },
        {
            "Id": "002",
            "Name": "Another Record",
            "attributes": {
                "type": "Account",
                "url": "/services/data/v50.0/sobjects/Account/001",
            },
        },
    ],
}


@pytest.fixture
def mock_generate_token(mocker):
    """Fixture to mock Salesforce.generate_token so no real HTTP call is made."""
    return mocker.patch.object(
        Salesforce,
        "generate_token",
        return_value={"access_token": "fake-token-123"},
    )


@pytest.fixture
def mock_sf_instance(mocker):
    """Fixture to mock the SimpleSalesforce instance."""
    mock_sf_instance = mocker.MagicMock(spec=SimpleSalesforce)

    mocker.patch(
        "viadot.sources.salesforce.SimpleSalesforce", return_value=mock_sf_instance
    )
    return mock_sf_instance


@pytest.fixture
def salesforce_instance(mock_sf_instance, mock_generate_token):  # noqa: ARG001
    """Fixture providing a Salesforce instance with mocked auth and API client."""
    return Salesforce(
        credentials=variables["credentials"],
        instance_url="https://test.salesforce.com",
    )


@pytest.mark.basic
def test_salesforce_init_dev_env(mock_sf_instance, mock_generate_token):  # noqa: ARG001
    """Test Salesforce, starting in dev mode."""
    sf_instance = Salesforce(
        credentials=variables["credentials"], instance_url="https://test.salesforce.com"
    )

    assert sf_instance.salesforce == mock_sf_instance


class TestSalesforceCredentials:
    """Test Salesforce Credentials Class."""

    @pytest.mark.basic
    def test_salesforce_credentials(self):
        """Test Salesforce credentials."""
        SalesforceCredentials(
            consumer_key="consumer_key",
            consumer_secret="consumer_secret",  # noqa: S106 # pragma: allowlist secret
        )


@pytest.mark.basic
def test_salesforce_init_prod_env(mock_sf_instance, mock_generate_token):  # noqa: ARG001
    """Test Salesforce, starting in prod mode."""
    sf_instance = Salesforce(credentials=variables["credentials"], env="PROD")

    assert sf_instance.salesforce == mock_sf_instance


@pytest.mark.basic
def test_salesforce_invalid_env(mock_generate_token):  # noqa: ARG001
    """Test Salesforce, invalid `env` parameter."""
    with pytest.raises(
        ValueError, match="The only available environments are DEV, QA, and PROD."
    ):
        Salesforce(credentials=variables["credentials"], env="INVALID")


@pytest.mark.connect
def test_salesforce_to_df_with_columns(mock_sf_instance, salesforce_instance):
    """Test Salesforce `to_df` method with columns."""
    mock_sf_instance.query_all.return_value = {"records": variables["records_2"]}

    result_df = salesforce_instance.to_df(table="Account", columns=["Id", "Name"])

    result_df.drop(
        columns=["_viadot_source", "_viadot_downloaded_at_utc"],
        inplace=True,
    )

    pd.testing.assert_frame_equal(
        result_df, pd.DataFrame([{"Id": "001", "Name": "Test Record"}])
    )
    mock_sf_instance.query_all.assert_called_once_with("SELECT Id, Name FROM Account")


@pytest.mark.functions
def test_salesforce_to_df(mock_sf_instance, salesforce_instance):
    """Test Salesforce `to_df` method."""
    mock_sf_instance.query_all.return_value = {"records": variables["data"]}

    df = salesforce_instance.to_df()
    assert not df.empty
    assert df.shape == (2, 4)
    assert list(df.columns) == [
        "Id",
        "Name",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]
    assert df.iloc[0]["Id"] == "001"


@pytest.mark.functions
def test_salesforce_to_df_empty_data(salesforce_instance):
    """Test Salesforce `to_df` method with empty df."""
    salesforce_instance.data = []

    with pytest.raises(ValueError, match="The response does not contain any data."):
        salesforce_instance.to_df(if_empty="fail")


@pytest.mark.functions
def test_salesforce_to_df_warn_empty_data(salesforce_instance):
    """Test Salesforce `to_df` method with empty df, warn."""
    salesforce_instance.data = []

    df = salesforce_instance.to_df(if_empty="warn")

    assert df.empty


@pytest.mark.functions
def test_salesforce_upsert_with_empty_data(salesforce_instance):
    df = pd.DataFrame(columns=["a", "b", "c"])
    result = salesforce_instance.upsert(df=df, table="Contact", external_id="a")

    assert result is None


@pytest.mark.functions
def test_salesforce_upsert_with_missing_external_key(salesforce_instance):
    df = pd.DataFrame([{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}])
    with pytest.raises(
        ValueError, match="Passed DataFrame does not contain column 'c'."
    ):
        salesforce_instance.upsert(df=df, table="test", external_id="c")
