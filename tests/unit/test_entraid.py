"""Tests for the EntraID source."""

import asyncio
from unittest.mock import MagicMock

import aiohttp
import pandas as pd
import pytest

from viadot.sources.entraid import EntraID


def _base_credentials():
    return {
        "tenant_id": "tenant",
        "client_id": "client",
        "client_secret": "secret",
        "scope": "https://graph.microsoft.com/.default",
    }


class _ReqInfo:
    def __init__(self, url: str = "https://example.com"):
        """Store request URL for fake responses."""
        self.real_url = url


class MockResponse:
    def __init__(self, status=200, json_data=None, headers=None):
        """Create a fake response with status, JSON and headers."""
        self.status = status
        self._json_data = json_data or {}
        self.headers = headers or {}

    async def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=_ReqInfo(),
                history=(),
                status=self.status,
                message="error",
                headers=self.headers,
            )


class MockCtxMgr:
    def __init__(self, response: MockResponse):
        """Wrap a response in an async context manager."""
        self._response = response

    async def __aenter__(self):
        """Enter async context and return the mock response."""
        return self._response

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context without suppressing exceptions."""
        return False


class MockSession:
    def __init__(self, get_responses=None, post_response=None):
        """Fake session yielding pre-set GET/POST responses."""
        self.headers = {}
        self._get_iter = iter(get_responses or [])
        self._post_response = post_response

    def get(self, _url):
        return MockCtxMgr(next(self._get_iter))

    def post(self, _url, _data=None):
        return MockCtxMgr(self._post_response)


@pytest.mark.asyncio
async def test_get_access_token_success():
    """Return token from the token endpoint."""
    e = EntraID(credentials=_base_credentials())
    session = MockSession(
        post_response=MockResponse(status=200, json_data={"access_token": "tok"})
    )
    result = await e._get_access_token(session)  # type: ignore[arg-type]
    assert result == "tok"


@pytest.mark.asyncio
async def test_authorize_session_sets_header():
    """Set Authorization header after token retrieval."""
    e = EntraID(credentials=_base_credentials())
    session = MockSession(
        post_response=MockResponse(status=200, json_data={"access_token": "tok"})
    )
    await e._authorize_session(session)  # type: ignore[arg-type]
    assert session.headers.get("Authorization") == "Bearer tok"


@pytest.mark.asyncio
async def test_fetch_all_pages_pagination_with_retry():
    """Retry on 429 and follow @odata.nextLink."""
    e = EntraID(credentials=_base_credentials())
    # 1st call -> 429 with Retry-After=0, then 2 pages 200
    r1 = MockResponse(status=429, headers={"Retry-After": "0"})
    r2 = MockResponse(
        status=200, json_data={"value": [{"a": 1}], "@odata.nextLink": "next"}
    )
    r3 = MockResponse(status=200, json_data={"value": [{"a": 2}]})
    session = MockSession(get_responses=[r1, r2, r3])
    items = await e._fetch_all_pages(session, "http://test")  # type: ignore[arg-type]
    assert items == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_get_all_users_calls_fetch_with_url(monkeypatch):
    """Call users endpoint with expected select fields."""
    e = EntraID(credentials=_base_credentials())
    captured = {}

    async def fake_fetch(_session, url, *_args, **_kwargs):
        captured["url"] = url
        return [{"id": "1", "mail": "a@b.com"}]

    monkeypatch.setattr(e, "_fetch_all_pages", fake_fetch)  # type: ignore[attr-defined]

    session = MagicMock()
    res = await e._get_all_users(session)  # type: ignore[arg-type]
    assert res == [{"id": "1", "mail": "a@b.com"}]
    assert captured["url"] == "https://graph.microsoft.com/v1.0/users?$select=id,mail"


@pytest.mark.asyncio
async def test_get_user_group_membership_maps_fields(monkeypatch):
    """Map group entries to user_email and group_name fields."""
    e = EntraID(credentials=_base_credentials())

    async def fake_fetch(_session, _url, *_args, **_kwargs):
        return [{"displayName": "G1"}, {"displayName": "G2"}]

    monkeypatch.setattr(e, "_fetch_all_pages", fake_fetch)  # type: ignore[attr-defined]
    session = MagicMock()
    user = {"id": "u1", "mail": "x@y.com"}
    res = await e._get_user_group_membership(session, user)  # type: ignore[arg-type]
    assert res == [
        {"user_email": "x@y.com", "group_name": "G1"},
        {"user_email": "x@y.com", "group_name": "G2"},
    ]


@pytest.mark.asyncio
async def test_build_user_groups_df_async_aggregates(monkeypatch):
    """Aggregate group rows for provided users."""
    e = EntraID(credentials=_base_credentials())

    async def fake_get_groups(_session, user):
        if user["id"] == "1":
            return [{"user_email": user["mail"], "group_name": "A"}]
        return [{"user_email": user["mail"], "group_name": "B"}]

    monkeypatch.setattr(e, "_get_user_group_membership", fake_get_groups)  # type: ignore[attr-defined]

    users_df = pd.DataFrame(
        [{"id": "1", "mail": "a@b.com"}, {"id": "2", "mail": "c@d.com"}]
    )
    session = MagicMock()
    df = await e._build_user_groups_df_async(
        session, users_df, ["user_email", "group_name"], max_concurrent=5
    )  # type: ignore[arg-type]
    assert list(df.columns) == ["user_email", "group_name"]
    assert set(df["group_name"]) == {"A", "B"}


@pytest.mark.asyncio
async def test_build_users_groups_df_composes(monkeypatch):
    """Compose authorize, get users, and build groups steps."""
    e = EntraID(credentials=_base_credentials())

    async def fake_authorize(_session):
        return None

    async def fake_get_users(_session):
        return [{"id": "1", "mail": "a@b.com"}]

    async def fake_build(_session, _users, _output_fields, _max_concurrent):
        return pd.DataFrame([{"user_email": "a@b.com", "group_name": "A"}])

    monkeypatch.setattr(e, "_authorize_session", fake_authorize)  # type: ignore[attr-defined]
    monkeypatch.setattr(e, "_get_all_users", fake_get_users)  # type: ignore[attr-defined]
    monkeypatch.setattr(e, "_build_user_groups_df_async", fake_build)  # type: ignore[attr-defined]

    df = await e._build_users_groups_df(max_concurrent=3)
    assert not df.empty
    assert set(df.columns) == {"user_email", "group_name"}


def test_run_fallback_when_event_loop_running(monkeypatch):
    """Use fallback loop when an event loop is already running."""
    e = EntraID(credentials=_base_credentials())

    async def sample_coro():
        return 123

    class FakeLoop:
        def __init__(self):
            self.closed = False

        def run_until_complete(self, _coro):
            return 123

        def close(self):
            self.closed = True

    def fake_new_loop():
        return FakeLoop()

    def fake_set_loop(_loop):
        return None

    def fake_asyncio_run(_coro):
        msg = "asyncio.run() cannot be called from a running event loop"
        raise RuntimeError(msg)

    monkeypatch.setattr(asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(asyncio, "new_event_loop", fake_new_loop)
    monkeypatch.setattr(asyncio, "set_event_loop", fake_set_loop)

    result = e._run(sample_coro())
    assert result == 123


def test_handle_if_empty_policies():
    """Follow warn, skip, and fail policies for empty DataFrames."""
    e = EntraID(credentials=_base_credentials())
    empty = pd.DataFrame()
    non_empty = pd.DataFrame([{"x": 1}])

    # warn -> returns original (empty)
    out_warn = e._handle_if_empty(empty, "warn")
    assert out_warn.empty

    # skip -> returns empty df
    out_skip = e._handle_if_empty(empty, "skip")
    assert out_skip.empty

    # fail -> raises
    with pytest.raises(ValueError, match="Resulting DataFrame is empty."):
        e._handle_if_empty(empty, "fail")

    # non-empty unchanged
    out_ok = e._handle_if_empty(non_empty, "warn")
    assert not out_ok.empty


def test_to_df_calls_cleanup_and_validate(monkeypatch):
    """Call cleanup_df and validate during to_df."""
    e = EntraID(credentials=_base_credentials())

    # Return a small DataFrame from the async builder
    async def fake_build(_max_concurrent):
        return pd.DataFrame([{"user_email": "a@b.com", "group_name": "A"}])

    monkeypatch.setattr(e, "_build_users_groups_df", fake_build)  # type: ignore[attr-defined]

    called = {"cleanup": False, "validate": False}

    def fake_cleanup(df):
        called["cleanup"] = True
        return df

    def fake_validate(_df, tests):
        called["validate"] = True
        assert tests == {"k": "v"}

    # Patch symbols as imported in module namespace
    import viadot.sources.entraid as ent_mod

    monkeypatch.setattr(ent_mod, "cleanup_df", fake_cleanup)
    monkeypatch.setattr(ent_mod, "validate", fake_validate)

    out = e.to_df(if_empty="warn", tests={"k": "v"}, max_concurrent=2)
    assert not out.empty
    assert called["cleanup"] is True
    assert called["validate"] is True


