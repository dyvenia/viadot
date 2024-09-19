"""'test_salesforce.py'."""

import pytest
from simple_salesforce import Salesforce

from viadot.exceptions import CredentialError
from viadot.sources import SalesForce
from viadot.sources.salesforce import SalesForceCredentials

variables = {
    "credentials": {
        "username": "test_user",
        "password": "test_password",
        "token": "test_token",
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
        {"Id": "001", "Name": "Test Record"},
        {"Id": "002", "Name": "Another Record"},
    ],
}


@pytest.mark.basic()
def test_salesforce_init_dev_env(mocker):
    """Test SalesForce, starting in dev mode."""
    mock_sf_instance = mocker.MagicMock(spec=Salesforce)
    mocker.patch("viadot.sources.salesforce.Salesforce", return_value=mock_sf_instance)
    sf_instance = SalesForce(credentials=variables["credentials"], env="DEV")

    assert sf_instance.salesforce == mock_sf_instance


class TestSalesForceCredentials:
    """Test SalesForce Credentials Class."""

    @pytest.mark.basic()
    def test_salesforce_credentials(self):
        """Test SalesForce credentials."""
        SalesForceCredentials(
            username="test_user",
            password="test_password",
            token="test_token",
        )


@pytest.mark.basic()
def test_salesforce_init_prod_env(mocker):
    """Test SalesForce, starting in prod mode."""
    mock_sf_instance = mocker.MagicMock(spec=Salesforce)
    mocker.patch("viadot.sources.salesforce.Salesforce", return_value=mock_sf_instance)
    sf_instance = SalesForce(credentials=variables["credentials"], env="PROD")

    assert sf_instance.salesforce == mock_sf_instance


@pytest.mark.basic()
def test_salesforce_invalid_env():
    """Test SalesForce, invalid `env` parameter."""
    with pytest.raises(
        ValueError, match="The only available environments are DEV, QA, and PROD."
    ):
        SalesForce(credentials=variables["credentials"], env="INVALID")


@pytest.mark.basic()
def test_salesforce_missing_credentials():
    """Test SalesForce missing credentials."""
    incomplete_creds = {"username": "user", "password": "pass"}
    with pytest.raises(CredentialError):
        SalesForce(credentials=incomplete_creds)


@pytest.mark.connect()
def test_salesforce_api_connection(mocker):
    """Test SalesForce `api_connection` method with a query."""
    mock_sf_instance = mocker.MagicMock(spec=Salesforce)
    mocker.patch("viadot.sources.salesforce.Salesforce", return_value=mock_sf_instance)
    salesforce_instance = SalesForce(credentials=variables["credentials"])

    mock_sf_instance.query.return_value = {"records": variables["records_1"]}

    salesforce_instance.api_connection(query="SELECT Id, Name FROM Account")

    assert salesforce_instance.data == [{"Id": "001", "Name": "Test Record"}]
    mock_sf_instance.query.assert_called_once_with("SELECT Id, Name FROM Account")


@pytest.mark.connect()
def test_salesforce_api_connection_with_columns(mocker):
    """Test SalesForce `api_connection` method with columns."""
    mock_sf_instance = mocker.MagicMock(spec=Salesforce)
    mocker.patch("viadot.sources.salesforce.Salesforce", return_value=mock_sf_instance)
    salesforce_instance = SalesForce(credentials=variables["credentials"])

    mock_sf_instance.query.return_value = {"records": variables["records_2"]}

    salesforce_instance.api_connection(table="Account", columns=["Id", "Name"])

    assert salesforce_instance.data == [{"Id": "001", "Name": "Test Record"}]
    mock_sf_instance.query.assert_called_once_with("SELECT Id, Name FROM Account")


@pytest.mark.functions()
def test_salesforce_to_df(mocker):
    """Test SalesForce `to_df` method."""
    mock_sf_instance = mocker.MagicMock(spec=Salesforce)
    mocker.patch("viadot.sources.salesforce.Salesforce", return_value=mock_sf_instance)
    salesforce_instance = SalesForce(credentials=variables["credentials"])
    salesforce_instance.data = variables["data"]

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


@pytest.mark.functions()
def test_salesforce_to_df_empty_data(mocker):
    """Test SalesForce `to_df` method with empty df."""
    mock_sf_instance = mocker.MagicMock(spec=Salesforce)
    mocker.patch("viadot.sources.salesforce.Salesforce", return_value=mock_sf_instance)
    salesforce_instance = SalesForce(credentials=variables["credentials"])
    salesforce_instance.data = []

    with pytest.raises(ValueError, match="The response does not contain any data."):
        salesforce_instance.to_df(if_empty="fail")


@pytest.mark.functions()
def test_salesforce_to_df_warn_empty_data(mocker):
    """Test SalesForce `to_df` method with empty df, warn."""
    mock_sf_instance = mocker.MagicMock(spec=Salesforce)
    mocker.patch("viadot.sources.salesforce.Salesforce", return_value=mock_sf_instance)
    salesforce_instance = SalesForce(credentials=variables["credentials"])
    salesforce_instance.data = []

    df = salesforce_instance.to_df(if_empty="warn")

    assert df.empty