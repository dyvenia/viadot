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


def _source(resource="users_core", **kwargs):
    session = MagicMock()
    session.post.return_value = _response(
        {"access_token": "test-token", "expires_in": 3600}
    )
    source = Cornerstone(
        resource=resource,
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

    source = Cornerstone(resource="users_core")

    assert source.credentials["base_url"] == BASE_URL
    assert source.credentials["sa_name"] == "service-account"


def test_environment_credentials_report_missing_names(monkeypatch):
    monkeypatch.delenv("BASE_URL", raising=False)
    monkeypatch.delenv("CLIENT_ID", raising=False)
    monkeypatch.delenv("CLIENT_SECRET", raising=False)

    with pytest.raises(ValueError, match="BASE_URL, CLIENT_ID, CLIENT_SECRET"):
        Cornerstone(resource="users_core")


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


def test_api_parameter_uses_api_keyed_path_and_throttle_maps():
    source = _source(api="deapi")

    assert source.api == "deapi"
    assert source._url() == (
        f"{BASE_URL}/services/api/x/dataexporter/api/objects/users_core"
    )
    assert source.REQUESTS_PER_MINUTE[source.api] == 120


def test_throttle_respects_configured_requests_per_minute(monkeypatch):
    """Honor the configured interval and sleep only for its remaining time."""
    # Four requests per minute means request starts must be at least 15 seconds apart.
    monkeypatch.setattr(Cornerstone, "REQUESTS_PER_MINUTE", {"deapi": 4})
    session = MagicMock()
    session.post.return_value = _response(
        {"access_token": "test-token", "expires_in": 3600}
    )
    source = Cornerstone(
        resource="users_core",
        credentials=_credentials(),
        session=session,
    )
    clock = [10.0]
    sleep_durations = []

    def fake_sleep(duration):
        # Advance simulated time.
        sleep_durations.append(duration)
        clock[0] += duration

    monkeypatch.setattr("viadot.sources.cornerstone.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("viadot.sources.cornerstone.time.sleep", fake_sleep)

    source._wait_for_throttle()
    first_request_at = source._last_request_at
    assert first_request_at is not None

    # Exactly one interval later, the next request should not need to wait.
    clock[0] += 15.0
    source._wait_for_throttle()
    second_request_at = source._last_request_at
    assert second_request_at is not None
    assert sleep_durations == []

    # After another second, throttle for the remaining 14 seconds.
    clock[0] += 1.0
    source._wait_for_throttle()

    assert sleep_durations == [14.0]
    assert second_request_at - first_request_at == 15.0
    assert source._last_request_at is not None
    assert source._last_request_at - second_request_at == 15.0


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
    ("resource", "expected_scope"),
    [
        ("users_core", "obj_users_core:read"),
        ("users_cf_core", "obj_users_cf_core:read"),
        ("user_ou_pivoted_core", "obj_user_ou_pivoted_core:read"),
        ("user_ou_core", "obj_user_ou_core:read"),
        ("ou_core", "obj_ou_core:read"),
        ("user_cf_enum_local2_core", "obj_user_cf_enum_local2_core:read"),
        ("posmgmt_position", "obj_posmgmt_position:read"),
        (
            "posmgmt_position_user_assignment",
            "obj_posmgmt_position_user_assignment:read",
        ),
        ("posmgmt_position_relation", "obj_posmgmt_position_relation:read"),
        (
            "posmgmt_position_unit_assignment",
            "obj_posmgmt_position_unit_assignment:read",
        ),
        ("posmgmt_position_status", "obj_posmgmt_position_status:read"),
        ("posmgmt_position_cf", "obj_posmgmt_position_cf:read"),
        ("$metadata", "obj_metadata:read"),
    ],
)
def test_default_scope_matches_deapi_resource(resource, expected_scope):
    source = _source(resource=resource)

    assert source._credentials.scope == expected_scope


def test_odata_pages_are_fetched_lazily_and_follow_next_link():
    source = _source(resource="users_core", query_params={"$top": 5})
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
        f"{BASE_URL}/services/api/x/dataexporter/api/objects/users_core"
    )
    assert first_call.kwargs["params"] == {"$top": 5}
    assert first_call.kwargs["headers"]["Prefer"] == "odata.maxpagesize=2"
    assert second_call.args[0] == next_link
    assert second_call.kwargs["params"] is None


def test_deapi_uses_objects_endpoint_and_caps_page_preference():
    source = _source(resource="users_core")
    source._session.get.return_value = _response({"value": []})

    list(source._to_records(batch_size=25_000))

    assert source._session.get.call_args.args[0] == (
        f"{BASE_URL}/services/api/x/dataexporter/api/objects/users_core"
    )
    assert source._session.get.call_args.kwargs["headers"]["Prefer"] == (
        "odata.maxpagesize=10000"
    )


def test_metadata_resource_requests_and_returns_odata_xml():
    source = _source(resource="$metadata")
    response = _response()
    response.text = '<edmx:Edmx Version="4.0" />'
    source._session.get.return_value = response

    assert source.get_metadata() == '<edmx:Edmx Version="4.0" />'
    source._session.post.assert_called_once()
    assert source._session.post.call_args.kwargs["json"]["scope"] == (
        "obj_metadata:read"
    )
    assert source._session.get.call_args.args[0] == (
        f"{BASE_URL}/services/api/x/dataexporter/api/objects/$metadata"
    )
    assert source._session.get.call_args.kwargs["headers"]["Accept"] == (
        "application/xml"
    )


def test_metadata_resource_rejects_record_iteration():
    source = _source(resource="$metadata")

    with pytest.raises(ValueError, match="Use get_metadata"):
        list(source._to_records())


def test_source_implements_main_dataframe_and_query_contract():
    dataframe_source = _source(resource="users_core")
    dataframe_source._session.get.return_value = _response(
        {"value": [{"id": 1, "name": "Ada"}]}
    )

    assert dataframe_source.to_df().to_dict("records") == [{"id": 1, "name": "Ada"}]

    query_source = _source(resource="users_core")
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
    source = _source(resource="users_core")
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
        url=f"{BASE_URL}/services/api/x/dataexporter/api/objects/users_core?filter=private",
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

    assert "endpoint=/services/api/x/dataexporter/api/objects/users_core" in caplog.text
    assert "status=400" in caplog.text
    assert "error_code='invalid_request'" in caplog.text
    assert "Rejected request containing [redacted]" in caplog.text
    assert "private" not in caplog.text
    assert "client-secret" not in caplog.text
