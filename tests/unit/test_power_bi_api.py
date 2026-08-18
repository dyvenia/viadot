from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from viadot.sources.power_bi import (
    PowerBIActivityEvents,
    PowerBiAuth,
    PowerBICredentials,
)


MODULE = "viadot.sources.power_bi"


@pytest.fixture
def creds_dict():
    return {
        "tenant_id": "tenant-123",
        "client_id": "client-456",
        "client_secret": "secret-789",
    }


@pytest.fixture
def creds_model(creds_dict):
    return PowerBICredentials(**creds_dict)


def _make_token_response(token="fake-token"):  # noqa: S107
    resp = MagicMock()
    resp.json.return_value = {"access_token": token}
    return resp


def _make_events_response(events, continuation_uri=None, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {}
    resp.json.return_value = {
        "activityEventEntities": events,
        "continuationUri": continuation_uri,
    }
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def mock_handle_api_response():
    """Patch handle_api_response with a sensible default token response.

    Tests that need a specific token or behavior can override
    `mock_handle_api_response.return_value` directly, no `with` needed.
    """
    with patch(f"{MODULE}.handle_api_response") as mock_handle:
        mock_handle.return_value = _make_token_response()
        yield mock_handle


@pytest.fixture
def source(creds_dict, mock_handle_api_response):  # noqa: ARG001
    src = PowerBIActivityEvents(credentials=creds_dict)
    src.logger = MagicMock()
    return src


def test_uses_explicit_credentials_dict_input(creds_dict):
    with patch(f"{MODULE}.get_credentials") as mock_get_creds:
        auth = PowerBiAuth(credentials=creds_dict)

    mock_get_creds.assert_not_called()
    assert auth.credentials == creds_dict


def test_uses_explicit_credentials_model_input(creds_model, creds_dict):
    with patch(f"{MODULE}.get_credentials") as mock_get_creds:
        auth = PowerBiAuth(credentials=creds_model)

    mock_get_creds.assert_not_called()
    assert auth.credentials == creds_dict


def test_validates_credentials_from_credential_secret():
    bad_creds = {"tenant_id": "only-one-field"}
    with (
        patch(f"{MODULE}.get_credentials", return_value=bad_creds),
        pytest.raises(Exception, match="validation error"),  # pydantic ValidationError
    ):
        PowerBiAuth(credential_secret="my-secret")  # noqa: S106


def test_headers_property_builds_bearer_header(creds_dict, mock_handle_api_response):
    mock_handle_api_response.return_value = _make_token_response("xyz")
    auth = PowerBiAuth(credentials=creds_dict)

    headers = auth.headers

    assert headers == {"Authorization": "Bearer xyz"}


def test_build_url_format():
    url = PowerBIActivityEvents.build_url("2024-01-15")
    assert url == (
        "https://api.powerbi.com/v1.0/myorg/admin/activityevents"
        "?startDateTime='2024-01-15T00:00:00.000'"
        "&endDateTime='2024-01-15T23:59:59.999'"
    )


def test_query_rate_limit_defaults_to_60s_without_retry_after_header(source):
    limited = MagicMock()
    limited.status_code = 429
    limited.headers = {}

    success = _make_events_response([])

    with (
        patch(f"{MODULE}.requests.get", side_effect=[limited, success]),
        patch(f"{MODULE}.time.sleep") as mock_sleep,
    ):
        source.query("2024-01-15")

    mock_sleep.assert_called_once_with(60)


def test_to_df_defaults_to_yesterday_utc_when_no_date_given(source):
    fixed_now = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
    mock_dt = MagicMock(wraps=datetime)
    mock_dt.now.return_value = fixed_now

    with (
        patch(f"{MODULE}.datetime", mock_dt),
        patch.object(source, "query", return_value=[]) as mock_query,
        patch.object(source, "_handle_if_empty"),
    ):
        source.to_df()

    mock_query.assert_called_once_with("2024-01-14")


def test_to_df_returns_dataframe_of_records(source):
    events = [{"Id": 1, "Operation": "ViewReport"}, {"Id": 2, "Operation": "Export"}]
    with patch.object(source, "query", return_value=events):
        df = source.to_df(date="2024-01-15")

    assert isinstance(df, pd.DataFrame)
    assert list(df["Id"]) == [1, 2]
    assert list(df["Operation"]) == ["ViewReport", "Export"]


def test_to_df_nested_records_flattened(source):
    events = [{"Id": 1, "meta": {"user": "Ted", "action": "view"}}]
    with patch.object(source, "query", return_value=events):
        df = source.to_df(date="2024-01-15")

    assert "meta.user" in df.columns
    assert df["meta.user"].iloc[0] == "Ted"
