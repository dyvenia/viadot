"""A module for managing dbt manifest.json storage."""

from abc import ABC, abstractmethod
import logging
from typing import Any, ClassVar
from urllib.parse import urlparse


try:
    from viadot.sources.s3 import S3
except ImportError:
    S3 = None

logger = logging.getLogger(__name__)


class ManifestStore(ABC):
    """A class for managing dbt manifest storage.

    Example usage:

    ```python
    from prefect import flow
    from viadot.orchestration.prefect.tasks.dbt_orchestration import ManifestStore

    store = ManifestStore(store_type="s3")
    manifest = store.read(
        credentials={
            "aws_access_key_id": "your_access_key",
            "aws_secret_access_key":
                "your_secret_key",
            },
        path="s3://your-bucket/path/to/manifest.json",
    )
    return manifest
    """

    _registry: ClassVar[dict[str, type["ManifestStore"]]] = {}

    def __new__(cls, store_type: str) -> "ManifestStore":
        """Create an instance of the appropriate subclass based on the store_type."""
        if cls is ManifestStore:
            subcls = ManifestStore._registry.get(store_type)
            if subcls is None:
                msg = f"Manifest store type '{store_type}' is not supported."
                raise NotImplementedError(msg)
            return super().__new__(subcls)
        return super().__new__(cls)

    def __init_subclass__(cls, store_type: str | None = None, **kwargs: dict) -> None:
        """Register subclasses with a store_type for dynamic instantiation."""
        super().__init_subclass__(**kwargs)
        if store_type is not None:
            ManifestStore._registry[store_type] = cls

    @abstractmethod
    def read(self, credentials: dict[str, Any], path: str) -> dict:
        """Read the dbt manifest from the store and return it as a dict."""
        msg = "This method should be implemented by subclasses of ManifestStore."
        raise NotImplementedError(msg)


class S3ManifestStore(ManifestStore, store_type="s3"):
    """A class for managing dbt manifest storage in S3."""

    def __init__(self) -> None:
        """Initialize S3ManifestStore, raising ImportError if `aws` extra is missing."""
        if S3 is None:
            msg = "S3ManifestStore requires the 'aws' extra: pip install 'viadot[aws]'"
            raise ImportError(msg)

    def read(self, credentials: dict[str, Any], path: str) -> dict:
        """Read the dbt manifest.json file from S3 and return it as a dict.

        Args:
            credentials: AWS credentials with access to the S3 bucket.
            path: S3 path where manifest is stored (e.g. "my-bucket/manifest.json").
        """
        short_path = urlparse(path).path.lstrip("/")
        logger.info(f"Reading manifest from S3 path: {short_path}")
        s3 = S3(credentials=credentials)
        manifest = s3.to_dict(path=path)
        logger.info("Manifest succesfully loaded.")
        return manifest
