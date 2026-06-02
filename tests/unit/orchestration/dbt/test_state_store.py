from io import BytesIO
import json
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
import pytest

from viadot.orchestration.dbt.state_store import (
    S3StateStore,
    _merge_node_state,
)


_PATH = "s3://my-bucket/state/state.json"
_CREDS = {
    "aws_access_key_id": "k",
    "aws_secret_access_key": "s",
}  # pragma: allowlist secret


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


def _make_store(data: dict | None = None, etag: str = "etag1") -> S3StateStore:
    """Return an S3StateStore with a mocked boto3 client."""
    body = json.dumps(data or {}).encode()
    mock_client = MagicMock()
    mock_client.get_object.side_effect = lambda **kwargs: {
        "Body": BytesIO(body),
        "ETag": f'"{etag}"',
    }

    with patch.object(S3StateStore, "_create_client", return_value=mock_client):
        return S3StateStore(
            store_type="s3",
            state_path=_PATH,
            credentials=_CREDS,
        )


# ---------------------------------------------------------------------------
# _merge_node_state
# ---------------------------------------------------------------------------


class TestMergeNodeState:
    def test_inserts_new_node(self):
        node_state = {
            "table_name": "orders",
            "status": "success",
            "fresh_until": "2026-01-01",
        }
        result = _merge_node_state({}, node_state)
        assert result == {"orders": node_state}

    def test_merges_existing_node(self):
        existing = {
            "table_name": "orders",
            "status": "success",
            "fresh_until": "2026-01-01",
            "owners": ["a"],
        }
        node_state = {"table_name": "orders", "status": "failed", "fresh_until": None}
        result = _merge_node_state({"orders": existing}, node_state)
        # fresh_until from existing is preserved on non-success
        assert result["orders"]["fresh_until"] == "2026-01-01"
        assert result["orders"]["status"] == "failed"

    def test_overwrites_fresh_until_on_success(self):
        existing = {"table_name": "orders", "status": "success", "fresh_until": "old"}
        node_state = {"table_name": "orders", "status": "success", "fresh_until": "new"}
        result = _merge_node_state({"orders": existing}, node_state)
        assert result["orders"]["fresh_until"] == "new"

    def test_preserves_fresh_until_when_none_in_existing(self):
        """If existing has no fresh_until, do not restore it on non-success."""
        existing = {"table_name": "orders", "status": "success", "fresh_until": None}
        node_state = {"table_name": "orders", "status": "failed", "fresh_until": None}
        result = _merge_node_state({"orders": existing}, node_state)
        assert result["orders"]["fresh_until"] is None

    def test_merges_without_status(self):
        """When status is absent, existing status and fresh_until are preserved."""
        existing = {
            "table_name": "orders",
            "status": "success",
            "fresh_until": "2026-01-01",
        }
        node_state = {"table_name": "orders", "_sla_breach_notification_sent": True}
        result = _merge_node_state({"orders": existing}, node_state)
        assert result["orders"]["status"] == "success"
        assert result["orders"]["fresh_until"] == "2026-01-01"
        assert result["orders"]["_sla_breach_notification_sent"] is True


# ---------------------------------------------------------------------------
# S3StateStore._parse_path
# ---------------------------------------------------------------------------


class TestParseS3Path:
    def test_returns_bucket_and_key(self):
        bucket, key = S3StateStore._parse_path("s3://my-bucket/path/to/file.json")
        assert bucket == "my-bucket"
        assert key == "path/to/file.json"

    def test_strips_leading_slash_from_key(self):
        _, key = S3StateStore._parse_path("s3://bucket/file.json")
        assert not key.startswith("/")

    def test_rejects_non_s3_scheme(self):
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            S3StateStore._parse_path("https://bucket/file.json")


# ---------------------------------------------------------------------------
# S3StateStore.read
# ---------------------------------------------------------------------------


class TestS3StateStoreRead:
    def test_returns_data_and_etag(self):
        data = {"orders": {"status": "success"}}
        store = _make_store(data=data, etag="abc123")
        result_data, result_etag = store._read()
        assert result_data == data
        assert result_etag == "abc123"

    def test_calls_get_object_with_correct_bucket_and_key(self):
        store = _make_store()
        store._read()
        store.client.get_object.assert_called_once_with(
            Bucket="my-bucket", Key="state/state.json"
        )


# ---------------------------------------------------------------------------
# S3StateStore.update
# ---------------------------------------------------------------------------


class TestS3StateStoreUpdate:
    def test_updates_existing_node(self):
        existing = {
            "orders": {"table_name": "orders", "status": "success", "fresh_until": "x"}
        }
        store = _make_store(data=existing)
        node_state = {"table_name": "orders", "status": "failed", "fresh_until": None}
        store.update(node_state=node_state)
        _args, kwargs = store.client.put_object.call_args
        written = json.loads(kwargs["Body"].decode())
        assert written["orders"]["status"] == "failed"
        assert written["orders"]["fresh_until"] == "x"  # preserved on non-success

    def test_retries_on_precondition_failed(self):
        store = _make_store(
            data={
                "orders": {
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                }
            }
        )
        store.client.put_object.side_effect = [
            _client_error("PreconditionFailed"),
            MagicMock(),  # succeeds on second attempt
        ]
        with patch("time.sleep"):
            store.update(
                node_state={
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                },
            )
        assert store.client.put_object.call_count == 2

    def test_raises_file_not_found_on_no_such_key(self):
        store = _make_store()
        store.client.get_object.side_effect = _client_error("NoSuchKey")
        with pytest.raises(FileNotFoundError):
            store.update(node_state={"table_name": "orders", "status": "success"})

    def test_raises_runtime_error_when_retries_exhausted(self):
        store = _make_store(
            data={
                "orders": {
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                }
            }
        )
        store.client.put_object.side_effect = _client_error("PreconditionFailed")
        with patch("time.sleep"), pytest.raises(RuntimeError, match="Max retries"):
            store.update(
                node_state={
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                },
            )

    def test_rereads_with_updated_etag_on_retry(self):
        """On retry, _read is called again and the new ETag is used.

        This is the core optimistic-locking guarantee: a concurrent writer changed the
        file between our read and write, so we must re-read to pick up both the latest
        data and the latest ETag before retrying the write.
        """
        data = {
            "orders": {
                "table_name": "orders",
                "status": "success",
                "fresh_until": None,
            }
        }
        body = json.dumps(data).encode()
        store = _make_store(data=data, etag="etag1")

        store.client.get_object.side_effect = [
            {"Body": BytesIO(body), "ETag": '"etag1"'},  # initial read
            {"Body": BytesIO(body), "ETag": '"etag2"'},  # re-read after conflict
        ]
        store.client.put_object.side_effect = [
            _client_error("PreconditionFailed"),  # concurrent writer changed the file
            MagicMock(),  # succeeds with fresh etag2
        ]

        with patch("time.sleep"):
            store.update(
                node_state={
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                },
            )

        assert store.client.get_object.call_count == 2
        first_if_match = store.client.put_object.call_args_list[0].kwargs["IfMatch"]
        second_if_match = store.client.put_object.call_args_list[1].kwargs["IfMatch"]
        assert first_if_match == "etag1"
        assert second_if_match == "etag2"

    def test_reraises_unknown_client_error(self):
        store = _make_store(
            data={
                "orders": {
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                }
            }
        )
        store.client.put_object.side_effect = _client_error("AccessDenied")
        with pytest.raises(ClientError):
            store.update(
                node_state={
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                },
            )


# ---------------------------------------------------------------------------
# S3StateStore.create
# ---------------------------------------------------------------------------


class TestS3StateStoreCreate:
    def test_creates_state_file(self):
        store = _make_store()
        node_state = {"table_name": "orders", "status": "success"}
        store.create(node_state=node_state)
        store.client.put_object.assert_called_once()
        _args, kwargs = store.client.put_object.call_args
        written = json.loads(kwargs["Body"].decode())
        assert written == {"orders": node_state}
        assert kwargs["IfNoneMatch"] == "*"

    def test_retries_on_conditional_request_conflict(self):
        store = _make_store()
        store.client.put_object.side_effect = [
            _client_error("ConditionalRequestConflict"),
            MagicMock(),
        ]
        with patch("time.sleep"):
            store.create(node_state={"table_name": "orders", "status": "success"})
        assert store.client.put_object.call_count == 2

    def test_raises_runtime_error_when_retries_exhausted(self):
        store = _make_store()
        store.client.put_object.side_effect = _client_error(
            "ConditionalRequestConflict"
        )
        with patch("time.sleep"), pytest.raises(RuntimeError, match="Max retries"):
            store.create(node_state={"table_name": "orders", "status": "success"})

    def test_reraises_precondition_failed_as_file_exists(self):
        store = _make_store()
        store.client.put_object.side_effect = _client_error("PreconditionFailed")
        with pytest.raises(FileExistsError):
            store.create(node_state={"table_name": "orders", "status": "success"})

    def test_reraises_unknown_client_error(self):
        store = _make_store()
        store.client.put_object.side_effect = _client_error("AccessDenied")
        with pytest.raises(ClientError):
            store.create(node_state={"table_name": "orders", "status": "success"})


# ---------------------------------------------------------------------------
# S3StateStore.write
# ---------------------------------------------------------------------------


class TestS3StateStoreWrite:
    def test_calls_update_when_file_exists(self):
        store = _make_store(
            data={
                "orders": {
                    "table_name": "orders",
                    "status": "success",
                    "fresh_until": None,
                }
            }
        )
        node_state = {"table_name": "orders", "status": "failed", "fresh_until": None}
        with patch.object(store, "update") as mock_update:
            store.write(node_state=node_state)
        mock_update.assert_called_once_with(node_state=node_state)

    def test_falls_back_to_create_when_file_missing(self):
        store = _make_store()
        node_state = {"table_name": "orders", "status": "success", "fresh_until": None}
        with (
            patch.object(store, "update", side_effect=FileNotFoundError),
            patch.object(store, "create") as mock_create,
        ):
            store.write(node_state=node_state)
        mock_create.assert_called_once_with(node_state)

    def test_retries_update_when_create_hits_race(self):
        store = _make_store()
        node_state = {"table_name": "orders", "status": "success", "fresh_until": None}
        with (
            patch.object(
                store, "update", side_effect=[FileNotFoundError, None]
            ) as mock_update,
            patch.object(store, "create", side_effect=FileExistsError) as mock_create,
        ):
            store.write(node_state=node_state)
        assert mock_update.call_count == 2
        mock_create.assert_called_once_with(node_state)
