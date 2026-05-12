"""Deployment state management."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
import functools
import json
import logging
import random
import time
from typing import Any, ClassVar
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


def retry_on_s3_error(
    retryable_codes: tuple[str, ...],
    max_retries: int = 3,
    reraise_as: dict[str, type[Exception]] | None = None,
) -> Callable:
    """Retry an S3 operation on specified ClientError codes with exponential backoff.

    Args:
        retryable_codes: ClientError codes that trigger a retry.
        max_retries: Maximum number of attempts.
        reraise_as: Maps ClientError codes to exception types to raise immediately.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except ClientError as e:
                    code = e.response["Error"]["Code"]
                    if reraise_as and code in reraise_as:
                        raise reraise_as[code] from e
                    if code in retryable_codes:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} of '{func.__name__}' "
                            f"failed ({code}). Retrying..."
                        )
                        time.sleep((0.1 * (2**attempt)) + random.random() * 0.1)  # noqa: S311
                        continue
                    raise
            msg = f"Max retries ({max_retries}) exceeded for '{func.__name__}'."
            raise RuntimeError(msg)

        return wrapper

    return decorator


def _merge_node_state(data: dict, node_state: dict) -> dict:
    """Merge node state into the existing state dict."""
    table_name = node_state["table_name"]
    status = node_state["status"]
    logger.info(f"Updating state for node '{table_name}' to status '{status}'.")

    if table_name not in data:
        data[table_name] = node_state
        return data

    existing = data[table_name]
    updated = {**existing, **node_state}
    # For status other than success, preserve the existing fresh_until
    # to avoid prematurely marking the data as stale. This allows for retries without
    # losing the original freshness information.
    if status != "success" and existing.get("fresh_until") is not None:
        updated["fresh_until"] = existing["fresh_until"]
    data[table_name] = updated
    return data


class StateStore(ABC):
    """A class for managing deployment state storage."""

    _registry: ClassVar[dict[str, type]] = {}

    def __init_subclass__(cls, store_type: str | None = None, **kwargs: dict) -> None:
        """Register subclasses with a store_type for dynamic instantiation."""
        super().__init_subclass__(**kwargs)
        if store_type is not None:
            StateStore._registry[store_type] = cls

    def __new__(cls, store_type: str, *_args: list, **_kwargs: dict) -> StateStore:
        """Create an instance of the appropriate subclass based on the store_type."""
        if cls is StateStore:
            subclass = cls._registry.get(store_type)
            if subclass is None:
                msg = f"State store type '{store_type}' is not supported."
                raise NotImplementedError(msg)
            return super().__new__(subclass)
        return super().__new__(cls)

    def __init__(
        self,
        store_type: str,
        state_path: str,
        credentials: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the StateStore."""
        self.store_type = store_type
        self.state_path = state_path
        self.credentials = credentials

    @abstractmethod
    def write(self, node_state: dict) -> None:
        """Store node state to the state file."""
        msg = "This method should be implemented by subclasses of StateStore."
        raise NotImplementedError(msg)

    @abstractmethod
    def create(self, node_state: dict) -> None:
        """Create a new state object with the initial state for the given table_name."""
        msg = "This method should be implemented by subclasses of StateStore."
        raise NotImplementedError(msg)

    @abstractmethod
    def update(self, node_state: dict) -> None:
        """Update the state file with the new state for the given table_name."""
        msg = "This method should be implemented by subclasses of StateStore."
        raise NotImplementedError(msg)


class S3StateStore(StateStore, store_type="s3"):
    """A class for managing deployment state storage in S3."""

    def __init__(self, *args, **kwargs: dict) -> None:
        """Initialize the S3StateStore with the given state path and credentials."""
        super().__init__(*args, **kwargs)
        self.bucket, self.key = self._parse_path(self.state_path)
        self.client = self._create_client()

    def _create_client(self):
        logger.info("Creating S3 client with provided AWS credentials.")

        if not self.credentials:
            logger.warning(
                "No credentials provided for S3StateStore. Ensure that the environment has appropriate AWS credentials configured."
            )
            return boto3.Session().client("s3")

        session = boto3.Session(
            aws_access_key_id=self.credentials.get("aws_access_key_id"),
            aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
            aws_session_token=self.credentials.get("aws_session_token"),
            region_name=self.credentials.get("region_name"),
        )
        logger.info("S3 client created successfully.")
        return session.client("s3", endpoint_url=self.credentials.get("endpoint_url"))

    @staticmethod
    def _parse_path(s3_uri: str) -> tuple[str, str]:
        """Parse an S3 URI into (bucket, key).

        Args:
            s3_uri: S3 URI like "s3://bucket/path/to/object".

        Returns:
            A tuple of ``(bucket, key)`` where ``key`` is the object path without
            a leading slash.

        Raises:
            ValueError: If the provided URI does not use the "s3" scheme.
        """
        logger.info("Parsing S3 URI ...")
        parsed = urlparse(s3_uri)

        if parsed.scheme != "s3":
            msg = f"Invalid S3 URI: {s3_uri}"
            raise ValueError(msg)

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        return bucket, key

    def _read(self) -> tuple[dict, str]:
        """Read the full state file from S3, returning etag for optimistic locking."""
        logger.info("Reading state file...")
        resp = self.client.get_object(Bucket=self.bucket, Key=self.key)
        body = resp["Body"].read()
        data = json.loads(body.decode("utf-8"))
        etag = resp["ETag"].strip('"')
        return data, etag

    @retry_on_s3_error(
        retryable_codes=("PreconditionFailed", "ConditionalRequestConflict"),
        max_retries=5,
        reraise_as={"NoSuchKey": FileNotFoundError},
    )
    def update(self, node_state: dict) -> dict:
        """Update the state file in S3 with the given node state."""
        data, etag = self._read()
        data = _merge_node_state(data=data, node_state=node_state)
        body = json.dumps(
            data, indent=4, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        return self.client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=body,
            ContentType="application/json",
            IfMatch=etag,
        )

    @retry_on_s3_error(retryable_codes=("ConditionalRequestFailed",), max_retries=3)
    def create(self, node_state: dict) -> dict:
        """Create a new state file in S3 with the initial state for the given table."""
        logger.info("Creating a new state file...")
        body = json.dumps(
            node_state, indent=4, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        return self.client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=body,
            ContentType="application/json",
            ConditionExpression="attribute_not_exists(Key)",
        )

    def write(self, node_state: dict) -> None:
        """Store node state in the state store.

        The file is expected to contain a mapping of table_name -> payload. This
        function will update the entry for ``node_state["table_name"]``.

        Args:
            node_state: A dictionary representing the state of a single node.
        """
        try:
            self.update(node_state=node_state)
            logger.info("Deployment status updated in S3.")
        except FileNotFoundError:
            self.create(node_state)
