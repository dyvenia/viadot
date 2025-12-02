"""Entra ID (Azure AD) connector."""

import asyncio
from collections.abc import Coroutine
from typing import Any, Literal, TypeVar

import aiohttp
import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import (
    add_viadot_metadata_columns,
    cleanup_df,
    handle_if_empty,
    validate,
)


HTTP_ERROR_429 = 429
HTTP_ERROR_503 = 503
HTTP_ERROR_504 = 504
HTTP_ERROR_500 = 500
HTTP_ERROR_600 = 600

T = TypeVar("T")


class EntraIDCredentials(BaseModel):
    """Entra ID credentials.

    Three key values are held in the Entra ID connector:
        - tenant_id: The tenant ID of the organization.
        - client_id: The client ID of the application.
        - client_secret: The client secret of the application.
        - scope: The scope of the application.

    Args:
        BaseModel (pydantic.BaseModel): Base class for data validation.

    """

    tenant_id: str
    client_id: str
    client_secret: str
    scope: str = "https://graph.microsoft.com/.default"


class EntraID(Source):
    """Entra ID (Azure AD) connector.

    It is responsible for fetching user-to-group memberships from Microsoft Graph.
    It uses the client credentials flow to authenticate and fetch the data.
    It supports pagination and retry logic for failed requests.

    Exposes a to_df() method that returns user-to-group memberships from
        Microsoft Graph.
    """

    def __init__(
        self,
        credentials: EntraIDCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the Entra ID source.

        Args:
            credentials (EntraIDCredentials | None): Explicit credentials for Entra ID.
                When None, credentials are loaded from the viadot config
                using `config_key`. Defaults to None.
            config_key (str | None): The key in the viadot config holding Entra ID
                credentials. Used only when `credentials` is None. Defaults to None.
            *args: Additional positional arguments forwarded to `Source`.
            **kwargs: Additional keyword arguments forwarded to `Source`.
        """
        credentials = credentials or get_source_credentials(config_key)
        validated_creds = dict(EntraIDCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

    async def _get_access_token(self, session: aiohttp.ClientSession) -> str:
        """Obtain an access token from Microsoft Identity Platform for authentication.

        Args:
            session (aiohttp.ClientSession): The aiohttp session for HTTP requests.

        Returns:
            str: Bearer token for Microsoft Graph API access.

        Raises:
            aiohttp.ClientResponseError: If the token endpoint responds with
            a non-2xx status.
            aiohttp.ClientError: If a network-related error occurs.
        """
        token_url = f'https://login.microsoftonline.com/{self.credentials.get("tenant_id")}/oauth2/v2.0/token'

        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.credentials.get("client_id"),
            "client_secret": self.credentials.get("client_secret"),
            "scope": self.credentials.get("scope"),
        }
        async with session.post(token_url, data=token_data) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("access_token")

    async def _authorize_session(self, session: aiohttp.ClientSession) -> None:
        """Authorize the provided session by attaching a Bearer token to headers.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to authorize.

        Raises:
            aiohttp.ClientResponseError:
                If obtaining an access token fails with non-2xx status.
            aiohttp.ClientError:
                If a network-related error occurs during token retrieval.
        """
        token = await self._get_access_token(session)
        session.headers.update({"Authorization": f"Bearer {token}"})

    async def _fetch_all_pages(
        self,
        session: aiohttp.ClientSession,
        url: str,
        max_retries: int = 5,
        backoff_factor: float = 0.5,
    ) -> list:
        """Fetch all results from paginated Microsoft Graph API endpoint async.

        Uses the '@odata.nextLink' property in the JSON response, which provides the URL
        for the next page of data. Iterates until no further pages remain.

        Args:
            session (aiohttp.ClientSession): The aiohttp session for HTTP requests.
            url (str): Initial Microsoft Graph API endpoint URL.
            max_retries (int): Maximum number of retries for 429/5xx responses.
            backoff_factor (float): Base delay for exponential backoff between retries.

        Returns:
            list: Aggregated list of JSON objects from all pages of the API response.

        Raises:
            aiohttp.ClientResponseError: If the endpoint keeps failing after retries.
            aiohttp.ClientError: If a network-related error occurs.
        """
        items: list = []
        while url:
            attempt = 0
            while True:
                async with session.get(url) as resp:
                    # Retry on throttling and server errors
                    if (
                        resp.status
                        in (
                            HTTP_ERROR_429,
                            HTTP_ERROR_503,
                            HTTP_ERROR_504,
                        )
                        or HTTP_ERROR_500 <= resp.status < HTTP_ERROR_600
                    ):
                        attempt += 1
                        if attempt > max_retries:
                            resp.raise_for_status()
                        retry_after_header = resp.headers.get("Retry-After")
                        if retry_after_header:
                            try:
                                delay = float(retry_after_header)
                            except ValueError:
                                delay = backoff_factor * (2 ** (attempt - 1))
                        else:
                            delay = backoff_factor * (2 ** (attempt - 1))
                        self.logger.warning(
                            f"GET {url} -> HTTP {resp.status}. Retrying in {delay:.2f}s (attempt {attempt}/{max_retries})."
                        )
                        await asyncio.sleep(delay)
                        continue

                    # Raise immediately on other client errors
                    resp.raise_for_status()
                    data = await resp.json()
                    break

            chunk = data.get("value", [])
            items.extend(chunk)
            url = data.get("@odata.nextLink")

        return items

    async def _get_all_users(self, session: aiohttp.ClientSession) -> list:
        """Retrieve all users from Microsoft Graph API asynchronously.

        Args:
            session (aiohttp.ClientSession): The aiohttp session for HTTP requests.

        Returns:
            list: List of user JSON objects with at least id and mail.

        Raises:
            aiohttp.ClientResponseError: If listing users fails after retries.
            aiohttp.ClientError: If a network-related error occurs.
        """
        users_url = "https://graph.microsoft.com/v1.0/users?$select=id,mail"

        return await self._fetch_all_pages(session=session, url=users_url)

    async def _get_user_group_membership(
        self, session: aiohttp.ClientSession, user: dict
    ) -> list:
        """Retrieve all group memberships for a single user asynchronously.

        Args:
            session (aiohttp.ClientSession): The aiohttp session for HTTP requests.
            user (dict): User JSON object with at least 'id', 'displayName', and 'mail'.

        Returns:
            list: List of dicts with user info and group membership details.

        Raises:
            aiohttp.ClientResponseError: If the membership endpoint fails after retries.
            aiohttp.ClientError: If a network-related error occurs.
        """
        user_id = user.get("id", "")
        user_email = user.get("mail", "")
        groups_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/memberOf"
        groups = await self._fetch_all_pages(session, groups_url)
        results = []
        for group in groups:
            results.append(
                {
                    "user_email": user_email,
                    "group_name": group.get("displayName", ""),
                }
            )
        return results

    async def _build_user_groups_df_async(
        self,
        session: aiohttp.ClientSession,
        users: pd.DataFrame,
        output_fields: list,
        max_concurrent: int = 50,
    ) -> pd.DataFrame:
        """Build a pandas DataFrame containing user-group membership.

        Args:
            session (aiohttp.ClientSession): The aiohttp session for HTTP requests.
            users (pd.DataFrame): DataFrame containing user records.
            output_fields (list): List of fields (columns) to include
                in the resulting DataFrame.
            max_concurrent (int): Maximum number of concurrent HTTP requests.
                Increased to 50 for faster execution.

        Returns:
            pd.DataFrame: DataFrame with one row per user-group membership record.

        Raises:
            None: Errors during per-user fetching are logged and skipped.
        """
        all_rows: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(max_concurrent)

        async def sem_get_user_groups(user: dict) -> list:
            async with semaphore:
                try:
                    return await self._get_user_group_membership(session, user)
                except Exception:
                    self.logger.exception(
                        "Failed to get groups for user %s",
                        user.get("displayName", ""),
                    )
                    return []

        # Convert DataFrame rows to dicts to reduce overhead during async processing
        user_records = (
            users[["id", "mail"]].to_dict(orient="records") if not users.empty else []
        )
        tasks = [sem_get_user_groups(user) for user in user_records]
        for task in asyncio.as_completed(tasks):
            user_groups = await task
            if user_groups:
                all_rows.extend(user_groups)

        return pd.DataFrame(all_rows, columns=output_fields)

    async def _build_users_groups_df(self, max_concurrent: int = 50) -> pd.DataFrame:
        """Build a user-group membership DataFrame by querying users and groups.

        Args:
            max_concurrent (int): Maximum number of concurrent HTTP requests
                to Microsoft Graph.

        Returns:
            pd.DataFrame: A DataFrame with columns `user_email` and `group_name`.

        Raises:
            aiohttp.ClientResponseError: If any Graph request fails after retries.
            aiohttp.ClientError: If a network-related error occurs.
        """
        timeout = aiohttp.ClientTimeout(total=120)
        connector = aiohttp.TCPConnector(limit=max_concurrent)
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:
            await self._authorize_session(session)
            users_list = await self._get_all_users(session)
            users_df = pd.DataFrame(users_list)
            output_fields = ["user_email", "group_name"]
            return await self._build_user_groups_df_async(
                session=session,
                users=users_df,
                output_fields=output_fields,
                max_concurrent=max_concurrent,
            )

    def _run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Safe runner to execute async coroutines from sync contexts.

        Args:
            coro (Coroutine): The coroutine to execute.

        Returns:
            Any: The result of the coroutine.

        Raises:
            RuntimeError: If the coroutine execution fails for reasons other than an
                already running event loop. When an event loop is running, a dedicated
                temporary loop is used to run the coroutine.
        """
        try:
            return asyncio.run(coro)
        except RuntimeError as e:
            # Fallback when already inside a running event loop
            if "asyncio.run() cannot be called from a running event loop" in str(e):
                loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(loop)
                    return loop.run_until_complete(coro)
                finally:
                    try:
                        loop.close()
                    finally:
                        asyncio.set_event_loop(None)
            raise

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        tests: dict[str, Any] | None = None,
        max_concurrent: int = 50,
    ) -> pd.DataFrame:
        """Produce a pandas DataFrame containing one row per user-group membership.

        Args:
            if_empty (Literal["warn", "skip", "fail"]): Policy for handling results.
            tests (dict[str, Any] | None): Optional tests to validate the resulting
                DataFrame.
                Passed through to `viadot.utils.validate`.
            max_concurrent (int): Maximum number of concurrent HTTP requests
                 to Microsoft Graph.

        Returns:
            pd.DataFrame: Cleaned DataFrame with user-group memberships
                and viadot metadata columns.

        Raises:
            ValueError: If `if_empty="fail"` and the resulting DataFrame is empty.
            aiohttp.ClientResponseError: If any Graph request fails after retries.
            aiohttp.ClientError: If a network-related error occurs.
            Exception: Any exception raised by provided `tests` validation.
        """
        df = self._run(self._build_users_groups_df(max_concurrent=max_concurrent))

        if df.empty:
            handle_if_empty(
                if_empty=if_empty,
                message="Resulting DataFrame is empty.",
                logger=self.logger,
            )

        df_clean = cleanup_df(df)

        if tests:
            validate(df=df_clean, tests=tests)

        return df_clean
