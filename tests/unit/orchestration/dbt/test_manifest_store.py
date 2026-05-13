from unittest.mock import MagicMock, patch

import pytest

from viadot.orchestration.dbt.manifest_store import (
    ManifestStore,
    S3ManifestStore,
)


pytest.importorskip("s3fs", reason="s3fs not installed")

_PATH = "s3://my-bucket/dbt/manifest.json"
_CREDS = {
    "aws_access_key_id": "k",
    "aws_secret_access_key": "s",
}  # pragma: allowlist secret
_MANIFEST = {
    "metadata": {"dbt_schema_version": "v10"},
    "nodes": {
        "model.lakehouse.int_orders": {
            "resource_type": "model",
            "name": "int_orders",
        }
    },
    "sources": {},
}


class TestManifestStoreFactory:
    def test_returns_s3_manifest_store_for_s3_type(self):
        store = ManifestStore(store_type="s3")
        assert isinstance(store, S3ManifestStore)

    def test_raises_for_unsupported_store_type(self):
        with pytest.raises(NotImplementedError, match="not supported"):
            ManifestStore(store_type="gcs")


class TestS3ManifestStoreRead:
    def _make_store(self) -> S3ManifestStore:
        store = ManifestStore(store_type="s3")
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = _MANIFEST
        store._s3 = mock_s3
        return store

    def test_returns_manifest_dict(self):
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = _MANIFEST

        with patch("viadot.orchestration.dbt.manifest_store.S3", return_value=mock_s3):
            store = ManifestStore(store_type="s3")
            result = store.read(credentials=_CREDS, path=_PATH)

        assert result == _MANIFEST

    def test_calls_s3_with_correct_path(self):
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = _MANIFEST

        with patch(
            "viadot.orchestration.dbt.manifest_store.S3", return_value=mock_s3
        ) as mock_s3_cls:
            store = ManifestStore(store_type="s3")
            store.read(credentials=_CREDS, path=_PATH)

        mock_s3_cls.assert_called_once_with(credentials=_CREDS)
        mock_s3.to_dict.assert_called_once_with(path=_PATH)

    def test_propagates_s3_exceptions(self):
        mock_s3 = MagicMock()
        mock_s3.to_dict.side_effect = RuntimeError("S3 unavailable")

        with patch("viadot.orchestration.dbt.manifest_store.S3", return_value=mock_s3):
            store = ManifestStore(store_type="s3")
            with pytest.raises(RuntimeError, match="S3 unavailable"):
                store.read(credentials=_CREDS, path=_PATH)
