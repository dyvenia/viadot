"""Cornerstone RAPI, DEAPI, and Employees API source connector."""

from collections.abc import Iterator
import os
import time
from typing import Any, ClassVar, Literal, cast
from urllib.parse import quote, urlsplit

import pandas as pd
from pydantic import BaseModel, field_validator
import requests

from viadot.config import get_source_credentials
from viadot.sources.base import Record, Source


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
HTTP_TOO_MANY_REQUESTS = 429
HTTP_ERROR_STATUS = 400


class CornerstoneCredentials(BaseModel):
    """Credentials for a registered Cornerstone OAuth application."""

    base_url: str
    client_id: str
    client_secret: str
    sa_name: str | None = None
    scope: str | None = None

    @field_validator("base_url")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        """Require a portal URL and remove trailing slashes."""
        value = value.strip().rstrip("/")
        if not value.startswith(("https://", "http://")):
            message = "base_url must include an http:// or https:// scheme"
            raise ValueError(message)
        return value


class Cornerstone(Source):
    """Read paged records from Cornerstone reporting and employee endpoints.

    Use ``api="rapi"`` with a Reporting API view, ``api="deapi"`` with a
    Data Exporter core object, or ``api="employees"`` with the Employees v2
    endpoint. RAPI and DEAPI use OData parameters and opaque ``@odata.nextLink``
    paging; Employees v2 uses ``nextPageToken``.

    Credentials may be passed explicitly, loaded from a viadot ``config_key``, or
    temporarily read from the environment variables ``BASE_URL``, ``CLIENT_ID``,
    and ``CLIENT_SECRET``. ``SA_NAME`` is accepted as metadata; the OAuth client
    credentials grant is bound to its service account at application registration.
    """

    API_PATHS: ClassVar[dict[Literal["rapi", "deapi", "employees"], str]] = {
        "rapi": "/services/api/x/odata/api/views",
        "deapi": "/services/api/x/dataexporter/api/objects",
        "employees": "/services/api/x/users/v2/employees",
    }
    REQUESTS_PER_MINUTE: ClassVar[dict[Literal["rapi", "deapi", "employees"], int]] = {
        "rapi": 400,
        "deapi": 120,
        "employees": 120,
    }

    def __init__(
        self,
        resource: str,
        api: Literal["rapi", "deapi", "employees"] = "deapi",
        query_params: dict[str, Any] | None = None,
        credentials: CornerstoneCredentials | dict[str, Any] | None = None,
        config_key: str | None = None,
        session: requests.Session | None = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        *args,
        **kwargs,
    ) -> None:
        """Initialize a Cornerstone API source.

        Args:
            resource: RAPI view or DEAPI object name. Employees sources use the
                default ``employees`` resource value, which is ignored in the URL.
            api: API family, one of ``rapi``, ``deapi``, or ``employees``.
            query_params: Optional OData or endpoint query parameters.
            credentials: Explicit Cornerstone credentials.
            config_key: viadot configuration key used when credentials are omitted.
            session: Optional requests session, useful for connection reuse/testing.
            max_retries: Maximum retries for 429 and transient 5xx responses.
            backoff_factor: Base exponential retry delay in seconds.
        """
        if api not in self.API_PATHS:
            message = f"Unsupported Cornerstone API family: {api!r}"
            raise ValueError(message)
        if api != "employees" and not resource:
            message = "resource is required for RAPI and DEAPI"
            raise ValueError(message)
        if max_retries < 0:
            message = "max_retries cannot be negative"
            raise ValueError(message)

        raw_credentials = credentials
        if raw_credentials is None and config_key:
            raw_credentials = get_source_credentials(config_key)
        if raw_credentials is None:
            raw_credentials = self._credentials_from_environment()

        credential_values = self._normalize_credential_keys(raw_credentials)
        validated_credentials = CornerstoneCredentials(**credential_values)
        if not validated_credentials.scope:
            default_scope = {
                "rapi": f"{resource}:read",
                "deapi": f"obj_{resource}:read",
                "employees": "employee:read",
            }[api]
            validated_credentials = validated_credentials.model_copy(
                update={"scope": default_scope}
            )
        self.api: Literal["rapi", "deapi", "employees"] = api
        self.resource = resource
        self.query_params = dict(query_params or {})
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._session = session or requests.Session()
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._last_request_at: float | None = None
        self._credentials = validated_credentials
        super().__init__(
            *args, credentials=validated_credentials.model_dump(), **kwargs
        )
        self.logger.debug(
            "Initialized Cornerstone source api=%s resource=%s scope=%s endpoint=%s",
            self.api,
            self.resource,
            self._credentials.scope,
            urlsplit(self._url()).path,
        )

    @staticmethod
    def _credentials_from_environment() -> dict[str, str | None]:
        """Read Cornerstone credentials from the process environment."""
        names = {
            "base_url": "BASE_URL",
            "client_id": "CLIENT_ID",
            "client_secret": "CLIENT_SECRET",  # pragma: allowlist secret
            "sa_name": "SA_NAME",
        }
        values = {key: os.environ.get(env_name) for key, env_name in names.items()}
        required = ("base_url", "client_id", "client_secret")
        missing = [key for key in required if not values[key]]
        if missing:
            raise ValueError(
                "Cornerstone credentials were not provided; set or pass: "
                + ", ".join(names[key] for key in missing)
            )
        return values

    @staticmethod
    def _normalize_credential_keys(
        credentials: CornerstoneCredentials | dict[str, Any],
    ) -> dict[str, Any]:
        """Accept lowercase config keys and the environment variable names."""
        if isinstance(credentials, CornerstoneCredentials):
            return credentials.model_dump()

        values = dict(credentials)
        for key, env_name in (
            ("base_url", "BASE_URL"),
            ("client_id", "CLIENT_ID"),
            ("client_secret", "CLIENT_SECRET"),
            ("sa_name", "SA_NAME"),
        ):
            if key not in values and env_name in values:
                values[key] = values.pop(env_name)
        return values

    @property
    def headers(self) -> dict[str, str]:
        """Return authenticated JSON headers, refreshing the token when needed."""
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
        }

    def _get_access_token(self) -> str:
        """Return a cached access token or acquire one before it expires."""
        now = time.monotonic()
        if self._token and now < self._token_expires_at:
            self.logger.debug("Reusing cached Cornerstone token api=%s", self.api)
            return self._token

        token_url = f"{self._credentials.base_url}/services/api/oauth2/token"
        payload: dict[str, str] = {
            "clientId": self._credentials.client_id,
            "clientSecret": self._credentials.client_secret,
            "grantType": "client_credentials",
        }
        if self._credentials.scope:
            payload["scope"] = self._credentials.scope

        self.logger.debug(
            "Requesting Cornerstone OAuth token api=%s endpoint=%s scope=%s",
            self.api,
            urlsplit(token_url).path,
            self._credentials.scope,
        )
        try:
            response = self._session.post(
                token_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=60,
            )
        except requests.HTTPError as exc:
            if exc.response is not None:
                self._log_http_failure("OAuth token request", exc.response)
            raise
        if response.status_code >= HTTP_ERROR_STATUS:
            self._log_http_failure("OAuth token request", response)
        response.raise_for_status()
        body = response.json()
        token = body.get("access_token")
        if not token:
            message = "Cornerstone token response did not contain access_token"
            raise ValueError(message)

        expires_in = float(body.get("expires_in", 3600))
        if expires_in <= 0:
            message = "Cornerstone token expires_in must be positive"
            raise ValueError(message)
        refresh_buffer = min(60.0, expires_in * 0.1)
        self._token = token
        self._token_expires_at = time.monotonic() + expires_in - refresh_buffer
        self.logger.info(
            "Cornerstone OAuth token acquired api=%s expires_in_seconds=%s",
            self.api,
            expires_in,
        )
        return token

    def _log_http_failure(self, operation: str, response: requests.Response) -> None:
        """Log endpoint and safe API error fields, never response records or secrets."""
        request = response.request
        request_url = request.url if request is not None else response.url
        path = urlsplit(request_url).path if isinstance(request_url, str) else "UNKNOWN"
        try:
            body = response.json()
        except ValueError:
            body = {}

        error = body.get("error", {}) if isinstance(body, dict) else {}
        if isinstance(error, dict):
            error_code = error.get("code")
            detail = (
                error.get("message")
                or error.get("description")
                or body.get("error_description")
            )
        else:
            error_code = error or None
            detail = body.get("error_description") if isinstance(body, dict) else None

        safe_detail = " ".join(str(detail).split()) if detail else ""
        for secret in (self._credentials.client_id, self._credentials.client_secret):
            if secret:
                safe_detail = safe_detail.replace(secret, "[redacted]")
        self.logger.error(
            "Cornerstone %s failed api=%s method=%s endpoint=%s status=%s "
            "error_code=%r error_message=%r",
            operation,
            self.api,
            request.method
            if request is not None and isinstance(request.method, str)
            else "UNKNOWN",
            path,
            response.status_code,
            error_code,
            safe_detail[:300] or None,
        )

    def _url(self) -> str:
        """Build the initial API URL for this source."""
        base = f"{self._credentials.base_url}{self.API_PATHS[self.api]}"
        if self.api == "employees":
            return base
        return f"{base}/{quote(self.resource, safe='$_-')}"

    def _wait_for_throttle(self) -> None:
        """Pace requests from this source instance to the API-family limit."""
        interval = 60.0 / self.REQUESTS_PER_MINUTE[self.api]
        now = time.monotonic()
        if self._last_request_at is not None:
            delay = interval - (now - self._last_request_at)
            if delay > 0:
                time.sleep(delay)
        self._last_request_at = time.monotonic()

    def _request_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """GET JSON with bounded retries for throttling and transient server errors."""
        request_headers = self.headers
        if headers:
            request_headers.update(headers)
        endpoint = urlsplit(url).path
        parameter_names = sorted(params) if params else []

        for attempt in range(self.max_retries + 1):
            self._wait_for_throttle()
            started_at = time.monotonic()
            self.logger.debug(
                "Cornerstone API request api=%s method=GET endpoint=%s "
                "parameter_names=%s prefer=%s attempt=%s/%s",
                self.api,
                endpoint,
                parameter_names,
                request_headers.get("Prefer"),
                attempt + 1,
                self.max_retries + 1,
            )
            response = self._session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=120,
            )
            elapsed = time.monotonic() - started_at
            self.logger.debug(
                "Cornerstone API response api=%s endpoint=%s status=%s "
                "elapsed_seconds=%.3f",
                self.api,
                endpoint,
                response.status_code,
                elapsed,
            )
            if response.status_code not in RETRYABLE_STATUS_CODES:
                if response.status_code >= HTTP_ERROR_STATUS:
                    self._log_http_failure("API request", response)
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict):
                    message = "Cornerstone API response must be a JSON object"
                    raise ValueError(message)
                return body

            if attempt >= self.max_retries:
                self._log_http_failure("API request", response)
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after is not None else None
            except ValueError:
                delay = None
            if delay is None:
                delay = self.backoff_factor * (2**attempt)
                if response.status_code == HTTP_TOO_MANY_REQUESTS:
                    delay = max(delay, 60.0)
            self.logger.warning(
                "Cornerstone request returned HTTP %s api=%s endpoint=%s; "
                "retrying in %.2f seconds (attempt %s/%s).",
                response.status_code,
                self.api,
                endpoint,
                delay,
                attempt + 1,
                self.max_retries,
            )
            time.sleep(max(delay, 0))

        message = "Cornerstone request retry loop ended unexpectedly"
        raise RuntimeError(message)

    @staticmethod
    def _records_from_body(body: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract the record array used by OData and Employees responses."""
        for key in ("value", "employees", "data", "records"):
            records = body.get(key)
            if isinstance(records, list):
                return records
        return []

    def _iter_odata_records(
        self, batch_size: int | None
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield RAPI/DEAPI pages, following each opaque nextLink verbatim."""
        url = self._url()
        params = dict(self.query_params)
        headers: dict[str, str] = {}
        seen_urls: set[str] = set()
        page_number = 0
        if batch_size:
            headers["Prefer"] = f"odata.maxpagesize={batch_size}"

        while url:
            if url in seen_urls:
                message = "Cornerstone returned a cyclic @odata.nextLink"
                raise ValueError(message)
            seen_urls.add(url)
            page_number += 1
            body = self._request_json(url, params=params, headers=headers)
            records = self._records_from_body(body)
            next_url = body.get("@odata.nextLink") or body.get("@odata.nextlink")
            self.logger.info(
                "Cornerstone page received api=%s resource=%s page=%s "
                "records=%s has_next_page=%s",
                self.api,
                self.resource,
                page_number,
                len(records),
                bool(next_url),
            )
            if records:
                yield records
            url = next_url
            params = None
            headers = {}

    def _iter_employee_records(self) -> Iterator[list[dict[str, Any]]]:
        """Yield Employees v2 pages using the server-provided nextPageToken."""
        url = self._url()
        params = dict(self.query_params)
        seen_tokens: set[str] = set()
        page_number = 0

        while url:
            page_number += 1
            body = self._request_json(url, params=params)
            records = self._records_from_body(body)
            next_token = body.get("nextPageToken")
            self.logger.info(
                "Cornerstone page received api=%s resource=%s page=%s "
                "records=%s has_next_page=%s",
                self.api,
                self.resource,
                page_number,
                len(records),
                bool(next_token),
            )
            if records:
                yield records

            if not next_token:
                break
            if next_token in seen_tokens:
                message = "Cornerstone returned a repeated nextPageToken"
                raise ValueError(message)
            seen_tokens.add(next_token)
            params = {**self.query_params, "nextPageToken": next_token}

    def _to_records(
        self, batch_size: int | None = 1000
    ) -> Iterator[list[dict[str, Any]]]:
        """Lazily fetch and yield one server page at a time."""
        if self.api == "employees":
            yield from self._iter_employee_records()
        else:
            # Cornerstone caps OData pages at 10,000 records.
            page_size = min(batch_size, 10_000) if batch_size is not None else None
            yield from self._iter_odata_records(batch_size=page_size)

    def _query_records(self) -> list[dict[str, Any]]:
        """Materialize endpoint records for DataFrame and JSON consumers."""
        return [record for page in self._to_records() for record in page]

    def query(self) -> list[Record] | bool:
        """Return materialized rows in viadot's standard tuple-based format."""
        rows = [tuple(record.values()) for record in self._query_records()]
        return cast(list[Record], rows)

    def to_df(self, if_empty: Literal["warn", "skip", "fail"] = "warn") -> pd.DataFrame:
        """Fetch records into a DataFrame, following the base Source empty policy."""
        records = self._query_records()
        if not records:
            self._handle_if_empty(if_empty=if_empty)
        return pd.DataFrame.from_records(records)

    def to_json(self) -> dict[str, Any]:
        """Return all records in a JSON-compatible OData-style envelope."""
        return {"value": self._query_records()}
