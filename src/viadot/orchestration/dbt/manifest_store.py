"""Backward-compatible imports for dbt artifact storage."""

from viadot.orchestration.dbt.artifact_store import (
    ArtifactStore,
    S3ArtifactStore,
)


ManifestStore = ArtifactStore
S3ManifestStore = S3ArtifactStore
