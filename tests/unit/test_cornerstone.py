"""Tests for the Cornerstone source connector."""

import logging
from unittest.mock import MagicMock

import pytest
import requests

from viadot.sources.cornerstone import Cornerstone


BASE_URL = "https://stage.csod.com"


def _credentials():
    return {
        "base_url": BASE_URL,
        "client_id": "client-id",
        "client_secret": "client-secret",  # pragma: allowlist secret
        "sa_name": "service-account",
    }


def _response(body=None, status_code=200, headers=None):
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {}
    response.json.return_value = body or {}
    response.raise_for_status.side_effect = (
        requests.HTTPError(f"HTTP {status_code}") if status_code >= 400 else None
    )
    return response


def _source(api="deapi", resource="users_core", **kwargs):
    session = MagicMock()
    session.post.return_value = _response(
        {"access_token": "test-token", "expires_in": 3600}
    )
    source = Cornerstone(
        resource=resource,
        api=api,
        credentials=_credentials(),
        session=session,
        **kwargs,
    )
    source._wait_for_throttle = MagicMock()
    return source


def test_credentials_can_be_read_from_environment(monkeypatch):
    monkeypatch.setenv("BASE_URL", BASE_URL)
    monkeypatch.setenv("CLIENT_ID", "client-id")
    monkeypatch.setenv("CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SA_NAME", "service-account")

    source = Cornerstone(resource="vw_rpt_user", api="rapi")

    assert source.credentials["base_url"] == BASE_URL
    assert source.credentials["sa_name"] == "service-account"


def test_environment_credentials_report_missing_names(monkeypatch):
    monkeypatch.delenv("BASE_URL", raising=False)
    monkeypatch.delenv("CLIENT_ID", raising=False)
    monkeypatch.delenv("CLIENT_SECRET", raising=False)

    with pytest.raises(ValueError, match="BASE_URL, CLIENT_ID, CLIENT_SECRET"):
        Cornerstone(resource="vw_rpt_user", api="rapi")


def test_token_request_uses_cornerstone_json_fields():
    source = _source()

    assert source.headers == {
        "Authorization": "Bearer test-token",
        "Accept": "application/json",
    }
    source._session.post.assert_called_once_with(
        f"{BASE_URL}/services/api/oauth2/token",
        json={
            "clientId": "client-id",
            "clientSecret": "client-secret",  # pragma: allowlist secret
            "grantType": "client_credentials",
            "scope": "obj_users_core:read",
        },
        headers={"Content-Type": "application/json"},
        timeout=60,
    )


def test_access_token_is_reused_until_shortly_before_expiry(monkeypatch):
    source = _source()
    source._session.post.side_effect = [
        _response({"access_token": "token-1", "expires_in": 100}),
        _response({"access_token": "token-2", "expires_in": 100}),
    ]
    now = [0.0]
    monkeypatch.setattr("viadot.sources.cornerstone.time.monotonic", lambda: now[0])

    assert source._get_access_token() == "token-1"
    assert source._get_access_token() == "token-1"
    assert source._session.post.call_count == 1

    now[0] = 91.0
    assert source._get_access_token() == "token-2"
    assert source._session.post.call_count == 2


@pytest.mark.parametrize(
    ("api", "resource", "expected_scope"),
    [
        ("rapi", "vw_rpt_user", "vw_rpt_user:read"),
        ("deapi", "users_core", "obj_users_core:read"),
        ("employees", "employees", "employee:read"),
    ],
)
def test_default_scope_matches_api_resource(api, resource, expected_scope):
    source = _source(api=api, resource=resource)

    assert source._credentials.scope == expected_scope


def test_odata_pages_are_fetched_lazily_and_follow_next_link():
    source = _source(api="rapi", resource="vw_rpt_user", query_params={"$top": 5})
    next_link = f"{BASE_URL}/opaque?skiptoken=keep%2Bunchanged"
    source._session.get.side_effect = [
        _response(
            {"value": [{"id": 1}, {"id": 2}, {"id": 3}], "@odata.nextLink": next_link}
        ),
        _response({"value": [{"id": 4}]}),
    ]

    record_batches = source._iter_odata_records(batch_size=2)
    source._session.get.assert_not_called()

    assert [list(batch) for batch in record_batches] == [
        [{"id": 1}, {"id": 2}, {"id": 3}],
        [{"id": 4}],
    ]
    first_call, second_call = source._session.get.call_args_list
    assert first_call.args[0] == (
        f"{BASE_URL}/services/api/x/odata/api/views/vw_rpt_user"
    )
    assert first_call.kwargs["params"] == {"$top": 5}
    assert first_call.kwargs["headers"]["Prefer"] == "odata.maxpagesize=2"
    assert second_call.args[0] == next_link
    assert second_call.kwargs["params"] is None


def test_deapi_uses_objects_endpoint_and_caps_page_preference():
    source = _source(api="deapi", resource="users_core")
    source._session.get.return_value = _response({"value": []})

    list(source._to_records(batch_size=25_000))

    assert source._session.get.call_args.args[0] == (
        f"{BASE_URL}/services/api/x/dataexporter/api/objects/users_core"
    )
    assert source._session.get.call_args.kwargs["headers"]["Prefer"] == (
        "odata.maxpagesize=10000"
    )


def test_employees_endpoint_follows_next_page_token():
    source = _source(api="employees", query_params={"$select": "id"})
    source._session.get.side_effect = [
        _response({"employees": [{"id": 1}], "nextPageToken": "opaque-token"}),
        _response({"employees": [{"id": 2}]}),
    ]

    assert list(source._to_records()) == [[{"id": 1}], [{"id": 2}]]

    first_call, second_call = source._session.get.call_args_list
    assert first_call.args[0] == f"{BASE_URL}/services/api/x/users/v2/employees"
    assert first_call.kwargs["params"] == {"$select": "id"}
    assert second_call.kwargs["params"] == {
        "$select": "id",
        "nextPageToken": "opaque-token",
    }


def test_source_implements_main_dataframe_and_query_contract():
    dataframe_source = _source(api="rapi", resource="vw_rpt_user")
    dataframe_source._session.get.return_value = _response(
        {"value": [{"id": 1, "name": "Ada"}]}
    )

    assert dataframe_source.to_df().to_dict("records") == [{"id": 1, "name": "Ada"}]

    query_source = _source(api="rapi", resource="vw_rpt_user")
    query_source._session.get.return_value = _response(
        {"value": [{"id": 1, "name": "Ada"}]}
    )

    assert query_source.query() == [(1, "Ada")]


def test_429_retries_honoring_retry_after(monkeypatch):
    source = _source()
    source._session.get.side_effect = [
        _response(status_code=429, headers={"Retry-After": "0"}),
        _response({"value": [{"id": 1}]}),
    ]
    sleep = MagicMock()
    monkeypatch.setattr("viadot.sources.cornerstone.time.sleep", sleep)

    assert source._request_json("https://stage.csod.com/api") == {"value": [{"id": 1}]}
    assert source._session.get.call_count == 2
    sleep.assert_called_once_with(0.0)


def test_non_retryable_client_error_is_not_retried():
    source = _source()
    source._session.get.return_value = _response(status_code=400)

    with pytest.raises(requests.HTTPError, match="HTTP 400"):
        source._request_json("https://stage.csod.com/api")

    source._session.get.assert_called_once()


def test_http_failure_logs_endpoint_and_safe_error_without_query_or_secret(caplog):
    source = _source(api="rapi", resource="vw_rpt_user")
    response = _response(
        {
            "error": {
                "code": "invalid_request",
                "message": "Rejected request containing client-secret",
            }
        },
        status_code=400,
    )
    response.request = MagicMock(
        method="GET",
        url=f"{BASE_URL}/services/api/x/odata/api/views/vw_rpt_user?filter=private",
    )
    response.url = response.request.url
    source._session.get.return_value = response

    with (
        caplog.at_level(logging.DEBUG, logger="viadot.sources.base"),
        pytest.raises(requests.HTTPError),
    ):
        source._request_json(
            response.request.url,
            params={"$filter": "private"},
        )

    assert "endpoint=/services/api/x/odata/api/views/vw_rpt_user" in caplog.text
    assert "status=400" in caplog.text
    assert "error_code='invalid_request'" in caplog.text
    assert "Rejected request containing [redacted]" in caplog.text
    assert "private" not in caplog.text
    assert "client-secret" not in caplog.text
