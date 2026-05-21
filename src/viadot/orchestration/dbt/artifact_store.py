"""Storage helpers for dbt artifacts."""

from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import urlparse


try:
    from viadot.sources.s3 import S3
except ImportError:
    S3 = None

logger = logging.getLogger(__name__)


class ArtifactStore(ABC):
    """A class for managing dbt artifact storage."""

    MANIFEST_FILENAME = "manifest.json"
    PARTIAL_PARSE_FILENAME = "partial_parse.msgpack"
    RUN_RESULTS_DIR = "run_results"
    RUN_RESULTS_FILENAME = "run_results.json"

    _registry: ClassVar[dict[str, type["ArtifactStore"]]] = {}

    def __new__(cls, store_type: str) -> "ArtifactStore":
        """Create an instance of the appropriate subclass based on the store type."""
        if cls is ArtifactStore:
            subcls = ArtifactStore._registry.get(store_type)
            if subcls is None:
                msg = f"Artifact store type '{store_type}' is not supported."
                raise NotImplementedError(msg)
            return super().__new__(subcls)
        return super().__new__(cls)

    def __init_subclass__(cls, store_type: str | None = None, **kwargs: dict) -> None:
        """Register subclasses with a store type for dynamic instantiation."""
        super().__init_subclass__(**kwargs)
        if store_type is not None:
            ArtifactStore._registry[store_type] = cls

    @staticmethod
    def join_path(base_path: str, *parts: str) -> str:
        """Join artifact path parts while preserving URI schemes such as ``s3://``."""
        return "/".join(
            [base_path.rstrip("/"), *(str(part).strip("/") for part in parts)]
        )

    def manifest_path(self, artifact_store_path: str) -> str:
        """Return the manifest path for an artifact store root."""
        return self.join_path(artifact_store_path, self.MANIFEST_FILENAME)

    def partial_parse_path(self, artifact_store_path: str) -> str:
        """Return the partial parse path for an artifact store root."""
        return self.join_path(artifact_store_path, self.PARTIAL_PARSE_FILENAME)

    def run_results_path(
        self,
        artifact_store_path: str,
        date_str: str,
        timestamp: float,
    ) -> str:
        """Return the timestamped run results path for an artifact store root."""
        file_name = f"{Path(self.RUN_RESULTS_FILENAME).stem}_{timestamp}.json"
        return self.join_path(
            artifact_store_path,
            self.RUN_RESULTS_DIR,
            date_str,
            file_name,
        )

    @abstractmethod
    def read_json(self, credentials: dict[str, Any] | None, path: str) -> dict:
        """Read a JSON artifact from the store and return it as a dict."""
        msg = "This method should be implemented by subclasses of ArtifactStore."
        raise NotImplementedError(msg)

    @abstractmethod
    def download_file(
        self,
        credentials: dict[str, Any] | None,
        from_path: str,
        to_path: str,
    ) -> None:
        """Download an artifact from the store."""
        msg = "This method should be implemented by subclasses of ArtifactStore."
        raise NotImplementedError(msg)

    @abstractmethod
    def upload_file(
        self,
        credentials: dict[str, Any] | None,
        from_path: str,
        to_path: str,
    ) -> None:
        """Upload an artifact to the store."""
        msg = "This method should be implemented by subclasses of ArtifactStore."
        raise NotImplementedError(msg)

    def read_manifest(
        self,
        credentials: dict[str, Any] | None,
        artifact_store_path: str | None = None,
        path: str | None = None,
    ) -> dict:
        """Read a dbt ``manifest.json`` artifact."""
        if path is None:
            if artifact_store_path is None:
                msg = "Either artifact_store_path or path must be provided."
                raise ValueError(msg)
            path = self.manifest_path(artifact_store_path)
        return self.read_json(credentials=credentials, path=path)

    def read(self, credentials: dict[str, Any] | None, path: str) -> dict:
        """Read a JSON artifact.

        This method preserves the old ``ManifestStore.read`` API while new callers use
        the artifact-specific methods.
        """
        return self.read_json(credentials=credentials, path=path)

    def download_partial_parse(
        self,
        credentials: dict[str, Any] | None,
        artifact_store_path: str,
        target_dir_path: str | Path,
    ) -> None:
        """Download ``partial_parse.msgpack`` into a dbt target directory."""
        target_path = Path(target_dir_path) / self.PARTIAL_PARSE_FILENAME
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self.download_file(
            credentials=credentials,
            from_path=self.partial_parse_path(artifact_store_path),
            to_path=str(target_path),
        )

    def upload_run_results(
        self,
        credentials: dict[str, Any] | None,
        artifact_store_path: str,
        local_run_results_file_path: str | Path,
        date_str: str,
        timestamp: float,
    ) -> str:
        """Upload ``run_results.json`` and return its artifact store path."""
        to_path = self.run_results_path(
            artifact_store_path=artifact_store_path,
            date_str=date_str,
            timestamp=timestamp,
        )
        self.upload_file(
            credentials=credentials,
            from_path=str(local_run_results_file_path),
            to_path=to_path,
        )
        return to_path


class S3ArtifactStore(ArtifactStore, store_type="s3"):
    """A class for managing dbt artifact storage in S3."""

    def __init__(self, *_args: list, **_kwargs: object) -> None:
        """Initialize S3ArtifactStore, raising ImportError if `aws` extra is missing."""
        if S3 is None:
            msg = "S3ArtifactStore requires the 'aws' extra: pip install 'viadot[aws]'"
            raise ImportError(msg)

    def _s3(self, credentials: dict[str, Any] | None) -> S3:
        return S3(credentials=credentials)

    def read_json(self, credentials: dict[str, Any] | None, path: str) -> dict:
        """Read a JSON file from S3 and return it as a dict."""
        short_path = urlparse(path).path.lstrip("/")
        logger.info(f"Reading dbt artifact from S3 path: {short_path}")
        artifact = self._s3(credentials).to_dict(path=path)
        logger.info("dbt artifact successfully loaded.")
        return artifact

    def download_file(
        self,
        credentials: dict[str, Any] | None,
        from_path: str,
        to_path: str,
    ) -> None:
        """Download a file from S3."""
        logger.info(f"Downloading dbt artifact from {from_path} to {to_path}.")
        self._s3(credentials).download(from_path=from_path, to_path=to_path)
        logger.info("dbt artifact successfully downloaded.")

    def upload_file(
        self,
        credentials: dict[str, Any] | None,
        from_path: str,
        to_path: str,
    ) -> None:
        """Upload a file to S3."""
        logger.info(f"Uploading dbt artifact from {from_path} to {to_path}.")
        self._s3(credentials).upload(from_path=from_path, to_path=to_path)
        logger.info("dbt artifact successfully uploaded.")
