from unittest.mock import MagicMock, patch

import pytest

from viadot.orchestration.dbt.artifact_store import (
    ArtifactStore,
    S3ArtifactStore,
)


_ARTIFACT_ROOT = "s3://my-bucket/dbt/artifacts"
_MANIFEST_PATH = f"{_ARTIFACT_ROOT}/manifest.json"
_PARTIAL_PARSE_PATH = f"{_ARTIFACT_ROOT}/partial_parse.msgpack"
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


@pytest.fixture(autouse=True)
def _mock_s3_class():
    with patch("viadot.orchestration.dbt.artifact_store.S3", MagicMock()):
        yield


class TestArtifactStoreFactory:
    def test_returns_s3_artifact_store_for_s3_type(self):
        store = ArtifactStore(store_type="s3")
        assert isinstance(store, S3ArtifactStore)

    def test_raises_for_unsupported_store_type(self):
        with pytest.raises(NotImplementedError, match="not supported"):
            ArtifactStore(store_type="gcs")


class TestArtifactStorePaths:
    def test_derives_manifest_path(self):
        store = ArtifactStore(store_type="s3")

        assert store.manifest_path(_ARTIFACT_ROOT) == _MANIFEST_PATH

    def test_derives_partial_parse_path(self):
        store = ArtifactStore(store_type="s3")

        assert store.partial_parse_path(_ARTIFACT_ROOT) == _PARTIAL_PARSE_PATH

    def test_derives_partitioned_run_results_path(self):
        store = ArtifactStore(store_type="s3")

        assert (
            store.run_results_path(_ARTIFACT_ROOT, "20260519", 123.456)
            == f"{_ARTIFACT_ROOT}/run_results/20260519/run_results_123.456.json"
        )


class TestS3ArtifactStore:
    def test_reads_manifest_from_derived_path(self):
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = _MANIFEST

        with patch("viadot.orchestration.dbt.artifact_store.S3", return_value=mock_s3):
            store = ArtifactStore(store_type="s3")
            result = store.read_manifest(
                credentials=_CREDS,
                artifact_store_path=_ARTIFACT_ROOT,
            )

        assert result == _MANIFEST
        mock_s3.to_dict.assert_called_once_with(path=_MANIFEST_PATH)

    def test_reads_manifest_from_explicit_path(self):
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = _MANIFEST

        with patch(
            "viadot.orchestration.dbt.artifact_store.S3",
            return_value=mock_s3,
        ) as mock_s3_cls:
            store = ArtifactStore(store_type="s3")
            result = store.read_manifest(credentials=_CREDS, path=_MANIFEST_PATH)

        assert result == _MANIFEST
        mock_s3_cls.assert_called_once_with(credentials=_CREDS)
        mock_s3.to_dict.assert_called_once_with(path=_MANIFEST_PATH)

    def test_downloads_partial_parse_to_target_dir(self, tmp_path):
        mock_s3 = MagicMock()
        target_dir_path = tmp_path / "target"

        with patch("viadot.orchestration.dbt.artifact_store.S3", return_value=mock_s3):
            store = ArtifactStore(store_type="s3")
            store.download_partial_parse(
                credentials=_CREDS,
                artifact_store_path=_ARTIFACT_ROOT,
                target_dir_path=target_dir_path,
            )

        mock_s3.download.assert_called_once_with(
            from_path=_PARTIAL_PARSE_PATH,
            to_path=str(target_dir_path / "partial_parse.msgpack"),
        )

    def test_uploads_run_results_to_partitioned_path(self, tmp_path):
        mock_s3 = MagicMock()
        run_results_path = tmp_path / "run_results.json"

        with patch("viadot.orchestration.dbt.artifact_store.S3", return_value=mock_s3):
            store = ArtifactStore(store_type="s3")
            result = store.upload_run_results(
                credentials=_CREDS,
                artifact_store_path=_ARTIFACT_ROOT,
                run_results_file_path=run_results_path,
                date_str="20260519",
                timestamp=123.456,
            )

        expected_path = (
            f"{_ARTIFACT_ROOT}/run_results/20260519/run_results_123.456.json"
        )
        assert result == expected_path
        mock_s3.upload.assert_called_once_with(
            from_path=str(run_results_path),
            to_path=expected_path,
        )

    def test_propagates_s3_exceptions(self):
        mock_s3 = MagicMock()
        mock_s3.to_dict.side_effect = RuntimeError("S3 unavailable")

        with patch("viadot.orchestration.dbt.artifact_store.S3", return_value=mock_s3):
            store = ArtifactStore(store_type="s3")
            with pytest.raises(RuntimeError, match="S3 unavailable"):
                store.read_manifest(credentials=_CREDS, path=_MANIFEST_PATH)
