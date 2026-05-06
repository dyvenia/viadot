from unittest.mock import MagicMock

import pytest
import read_dbt_manifest as _rdm


@pytest.fixture(autouse=True)
def mock_prefect_logger(monkeypatch):
    mock_log = MagicMock()
    monkeypatch.setattr(_rdm, "get_run_logger", lambda: mock_log)
    return mock_log


@pytest.fixture
def credentials():
    return {
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",  # pragma: allowlist secret
        "region_name": "eu-west-1",
    }


class TestReadDbtManifest:
    def test_delegates_to_s3_reader(self, monkeypatch, credentials):
        expected_manifest = {"nodes": {}, "sources": {}}
        mock_read = MagicMock(return_value=expected_manifest)
        monkeypatch.setattr(_rdm, "_read_manifest_from_s3", mock_read)

        result = _rdm.read_dbt_manifest.fn(
            credentials=credentials,
            path="s3://bucket/manifest.json",
        )

        assert result == expected_manifest
        mock_read.assert_called_once_with(
            credentials=credentials,
            path="s3://bucket/manifest.json",
        )

    def test_raises_for_unsupported_store_type(self, credentials):
        with pytest.raises(
            NotImplementedError,
            match="Manifest store type 'local' is not supported",
        ):
            _rdm.read_dbt_manifest.fn(
                credentials=credentials,
                store_type="local",
                path="s3://bucket/manifest.json",
            )


class TestReadManifestFromS3:
    def test_reads_manifest_via_s3_to_dict(self, monkeypatch, credentials):
        expected_manifest = {"nodes": {"a": {}}, "sources": {"b": {}}}
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = expected_manifest
        mock_s3_cls = MagicMock(return_value=mock_s3)
        monkeypatch.setattr(_rdm, "S3", mock_s3_cls)

        result = _rdm._read_manifest_from_s3(
            credentials=credentials,
            path="s3://bucket/manifest.json",
        )

        assert result == expected_manifest
        mock_s3_cls.assert_called_once_with(credentials=credentials)
        mock_s3.to_dict.assert_called_once_with(path="s3://bucket/manifest.json")

    def test_propagates_s3_read_errors(self, monkeypatch, credentials):
        mock_s3 = MagicMock()
        mock_s3.to_dict.side_effect = FileNotFoundError("manifest.json missing")
        monkeypatch.setattr(_rdm, "S3", MagicMock(return_value=mock_s3))

        with pytest.raises(FileNotFoundError, match="manifest.json missing"):
            _rdm._read_manifest_from_s3(
                credentials=credentials,
                path="s3://bucket/manifest.json",
            )
