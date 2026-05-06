from datetime import datetime, timedelta, timezone
from datetime import time as dt_time
from io import BytesIO
import json
import textwrap
from unittest.mock import MagicMock

from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
import pytest
import update_node_state as _uns


_FROZEN_NOW = datetime(2026, 3, 19, 10, 30, 0, tzinfo=timezone.utc)
_CREDS = {"aws_access_key_id": "k", "aws_secret_access_key": "s"}
_STATE = {"table_name": "my_table", "status": "success", "fresh_until": None}


@pytest.fixture(autouse=True)
def mock_prefect_logger(monkeypatch):
    mock_log = MagicMock()
    monkeypatch.setattr(_uns, "get_run_logger", lambda: mock_log)
    return mock_log


@pytest.fixture
def credentials():
    return {
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",  # pragma: allowlist secret
        "region_name": "eu-west-1",
    }


@pytest.fixture
def manifest():
    return {
        "nodes": {
            "model.lakehouse.int_orders": {
                "resource_type": "model",
                "name": "int_orders",
                "meta": {"SLA": "24h", "owners": [{"email": "eng@example.com"}]},
            },
        },
        "sources": {
            "source.lakehouse.staging.raw_orders": {
                "resource_type": "source",
                "name": "raw_orders",
                "meta": {"owners": [{"email": "source@example.com"}]},
            },
        },
    }


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


def _make_mock_s3(data: dict | None = None, etag: str = "etag1") -> MagicMock:
    mock_s3 = MagicMock()
    body = json.dumps(data or {}).encode()
    mock_s3.get_object.return_value = {"Body": BytesIO(body), "ETag": f'"{etag}"'}
    return mock_s3


class TestParseS3Path:
    def test_returns_bucket_and_key(self):
        bucket, key = _uns._parse_s3_path("s3://my-bucket/path/to/file.json")
        assert bucket == "my-bucket"
        assert key == "path/to/file.json"

    def test_single_level_key(self):
        bucket, key = _uns._parse_s3_path("s3://bucket/file.json")
        assert bucket == "bucket"
        assert key == "file.json"

    def test_leading_slash_is_stripped(self):
        _, key = _uns._parse_s3_path("s3://bucket//double-slash/file")
        assert not key.startswith("/")
        assert key == "double-slash/file"

    def test_invalid_scheme_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            _uns._parse_s3_path("https://bucket/path")

    def test_non_s3_scheme_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            _uns._parse_s3_path("gs://bucket/path")


class TestCreateS3Client:
    def test_builds_boto3_session_and_s3_client(self, monkeypatch):
        creds = {
            "aws_access_key_id": "key",
            "aws_secret_access_key": "secret",  # pragma: allowlist secret
            "aws_session_token": "token",
            "region_name": "eu-west-1",
            "endpoint_url": "http://minio:9000",
        }
        mock_session = MagicMock()
        mock_client = object()
        mock_session.client.return_value = mock_client
        mock_session_ctor = MagicMock(return_value=mock_session)
        monkeypatch.setattr(_uns.boto3, "Session", mock_session_ctor)

        result = _uns._create_s3_client(creds)

        assert result is mock_client
        mock_session_ctor.assert_called_once_with(
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            aws_session_token=creds["aws_session_token"],
            region_name=creds["region_name"],
        )
        kwargs = mock_session.client.call_args.kwargs
        assert kwargs["endpoint_url"] == "http://minio:9000"
        assert kwargs["config"] is not None


class TestReadJsonWithEtag:
    def test_returns_data_and_etag_without_quotes(self):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": BytesIO(b'{"key": "value"}'),
            "ETag": '"abc123"',
        }

        data, etag = _uns._read_json_with_etag(mock_s3, "my-bucket", "path/file.json")

        assert data == {"key": "value"}
        assert etag == "abc123"

    def test_calls_get_object_with_expected_args(self):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": BytesIO(b"{}"), "ETag": '"x"'}

        _uns._read_json_with_etag(mock_s3, "bucket", "key/file.json")

        mock_s3.get_object.assert_called_once_with(Bucket="bucket", Key="key/file.json")


class TestWriteJsonIfMatch:
    def test_calls_put_object_with_if_match(self):
        mock_s3 = MagicMock()
        _uns._write_json_if_match(mock_s3, "bucket", "key.json", {"a": 1}, "etag123")

        kwargs = mock_s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "bucket"
        assert kwargs["Key"] == "key.json"
        assert kwargs["IfMatch"] == "etag123"
        assert kwargs["ContentType"] == "application/json"

    def test_body_is_valid_utf8_json(self):
        mock_s3 = MagicMock()
        data = {"x": 42, "label": "state"}

        _uns._write_json_if_match(mock_s3, "bucket", "key.json", data, "etag")

        body = mock_s3.put_object.call_args.kwargs["Body"]
        assert json.loads(body.decode("utf-8")) == data


class TestApplyStateUpdate:
    def test_new_table_is_inserted_as_is(self):
        state = {"table_name": "t1", "status": "success", "fresh_until": "2026-06-01"}
        result = _uns._apply_state_update({}, state)
        assert result["t1"] == state

    def test_success_status_overwrites_fresh_until(self):
        existing = {
            "table_name": "t1",
            "status": "running",
            "fresh_until": "2025-01-01",
        }
        state = {"table_name": "t1", "status": "success", "fresh_until": "2026-06-01"}
        result = _uns._apply_state_update({"t1": existing}, state)

        assert result["t1"]["fresh_until"] == "2026-06-01"
        assert result["t1"]["status"] == "success"

    def test_non_success_preserves_existing_fresh_until(self):
        existing = {
            "table_name": "t1",
            "status": "success",
            "fresh_until": "2026-06-01",
        }
        state = {"table_name": "t1", "status": "failure", "fresh_until": None}
        result = _uns._apply_state_update({"t1": existing}, state)

        assert result["t1"]["fresh_until"] == "2026-06-01"
        assert result["t1"]["status"] == "failure"

    def test_non_success_without_existing_fresh_until_keeps_none(self):
        existing = {"table_name": "t1", "status": "running", "fresh_until": None}
        state = {"table_name": "t1", "status": "failure", "fresh_until": None}
        result = _uns._apply_state_update({"t1": existing}, state)

        assert result["t1"]["fresh_until"] is None

    def test_other_fields_are_updated(self):
        existing = {
            "table_name": "t1",
            "status": "running",
            "fresh_until": None,
            "batch_id": 1,
        }
        state = {
            "table_name": "t1",
            "status": "failure",
            "fresh_until": None,
            "batch_id": 2,
        }
        result = _uns._apply_state_update({"t1": existing}, state)

        assert result["t1"]["batch_id"] == 2


class TestUpdateJson:
    def test_success_on_first_attempt_writes_state(self, monkeypatch):
        mock_s3 = _make_mock_s3({"other_table": {}})
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))

        _uns._update_json("s3://bucket/state.json", _CREDS, _STATE)

        mock_s3.put_object.assert_called_once()
        body = json.loads(mock_s3.put_object.call_args.kwargs["Body"].decode())
        assert "my_table" in body

    def test_retries_on_precondition_failed_then_succeeds(self, monkeypatch):
        mock_s3 = MagicMock()
        call_count = 0

        def _put(**_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                error_code = "PreconditionFailed"
                raise _client_error(error_code)

        mock_s3.get_object.side_effect = lambda **_kwargs: {
            "Body": BytesIO(b"{}"),
            "ETag": '"e"',
        }
        mock_s3.put_object.side_effect = _put
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))
        monkeypatch.setattr(_uns.time, "sleep", MagicMock())
        monkeypatch.setattr(_uns.random, "random", MagicMock(return_value=0.0))

        _uns._update_json("s3://bucket/state.json", _CREDS, _STATE, max_retries=3)

        assert mock_s3.put_object.call_count == 2

    def test_retries_on_conditional_request_conflict(self, monkeypatch):
        mock_s3 = MagicMock()
        call_count = 0

        def _put(**_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                error_code = "ConditionalRequestConflict"
                raise _client_error(error_code)

        mock_s3.get_object.side_effect = lambda **_kwargs: {
            "Body": BytesIO(b"{}"),
            "ETag": '"e"',
        }
        mock_s3.put_object.side_effect = _put
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))
        monkeypatch.setattr(_uns.time, "sleep", MagicMock())
        monkeypatch.setattr(_uns.random, "random", MagicMock(return_value=0.0))

        _uns._update_json("s3://bucket/state.json", _CREDS, _STATE, max_retries=3)

        assert mock_s3.put_object.call_count == 2

    def test_raises_runtime_error_when_retries_exhausted(self, monkeypatch):
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = lambda **_kwargs: {
            "Body": BytesIO(b"{}"),
            "ETag": '"e"',
        }
        mock_s3.put_object.side_effect = _client_error("PreconditionFailed")
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))
        monkeypatch.setattr(_uns.time, "sleep", MagicMock())
        monkeypatch.setattr(_uns.random, "random", MagicMock(return_value=0.0))

        with pytest.raises(RuntimeError, match="Max retries exceeded"):
            _uns._update_json("s3://bucket/state.json", _CREDS, _STATE, max_retries=2)

    def test_raises_file_not_found_for_missing_key(self, monkeypatch):
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = _client_error("NoSuchKey")
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))

        with pytest.raises(FileNotFoundError):
            _uns._update_json("s3://bucket/state.json", _CREDS, _STATE)

    def test_reraises_unexpected_client_error(self, monkeypatch):
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = _client_error("AccessDenied")
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))

        with pytest.raises(ClientError):
            _uns._update_json("s3://bucket/state.json", _CREDS, _STATE)


class TestCreateNewStateFile:
    def test_wraps_state_under_table_name_key(self, monkeypatch):
        mock_s3 = MagicMock()
        state = {"table_name": "t1", "status": "success"}
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))

        _uns._create_new_state_file("s3://bucket/file.json", "t1", state, {})

        body = json.loads(mock_s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
        assert "t1" in body
        assert body["t1"] == state

    def test_writes_to_expected_bucket_and_key(self, monkeypatch):
        mock_s3 = MagicMock()
        monkeypatch.setattr(_uns, "_create_s3_client", MagicMock(return_value=mock_s3))

        _uns._create_new_state_file("s3://my-bucket/path/file.json", "t1", {}, {})

        kwargs = mock_s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "my-bucket"
        assert kwargs["Key"] == "path/file.json"


class TestGetNodeMeta:
    def test_returns_meta_for_model_node(self, manifest):
        assert _uns.get_node_meta("int_orders", manifest) == {
            "SLA": "24h",
            "owners": [{"email": "eng@example.com"}],
        }

    def test_returns_meta_for_source_node(self, manifest):
        assert _uns.get_node_meta("raw_orders", manifest) == {
            "owners": [{"email": "source@example.com"}],
        }

    def test_returns_empty_dict_when_node_missing(self, manifest):
        assert _uns.get_node_meta("nonexistent", manifest) == {}

    def test_returns_empty_dict_when_meta_is_none(self, manifest):
        manifest["nodes"]["model.lakehouse.int_orders"]["meta"] = None
        assert _uns.get_node_meta("int_orders", manifest) == {}


class TestGetNodeSla:
    def test_returns_sla_when_present(self):
        assert _uns._get_node_sla({"SLA": "24h"}) == "24h"

    def test_returns_none_when_missing(self):
        assert _uns._get_node_sla({"owners": []}) is None


class TestGetNodeOwners:
    def test_returns_owners_when_present(self):
        owners = [{"email": "eng@example.com", "type": "Technical Owner"}]
        assert _uns._get_node_owners({"owners": owners}) == owners

    def test_returns_empty_list_when_missing(self):
        assert _uns._get_node_owners({"SLA": "24h"}) == []


class TestGetSourceConfigPrefectYaml:
    @pytest.fixture
    def deployments_dir(self, tmp_path):
        (tmp_path / "extract").mkdir()
        (tmp_path / "transform").mkdir()
        (tmp_path / "prefect_base.yaml").write_text(
            textwrap.dedent(
                """\
                definitions:
                  default_schedule: &default_schedule
                    cron: "0 0 * * *"
                    timezone: "Europe/Madrid"
                    active: false
                """
            )
        )
        (tmp_path / "extract" / "ingestion.yaml").write_text(
            textwrap.dedent(
                """\
                deployments:
                  - name: ingest-raw-orders
                    parameters:
                      table: raw_orders
                    schedules:
                      - cron: "0 6 * * *"
                        timezone: Europe/Madrid
                      - cron: "0 14 * * *"
                        timezone: Europe/Madrid
                  - name: ingest-raw-customers
                    parameters:
                      table: raw_customers
                    schedules:
                      - <<: *default_schedule
                        cron: "0 3 * * *"
                  - name: ingest-raw-products
                    parameters:
                      table: raw_products
                    schedules:
                      - <<: *default_schedule
                """
            )
        )
        (tmp_path / "transform" / "dbt.yaml").write_text(
            textwrap.dedent(
                """\
                deployments:
                  - name: dbt-int-orders
                    parameters:
                      dbt_selects:
                        run: int_orders
                    schedules:
                      - cron: "0 7 * * *"
                        timezone: UTC
                """
            )
        )
        return tmp_path

    def test_ingestion_node_returns_deployment_and_crons(self, deployments_dir):
        table_name, crons = _uns.get_source_config_prefect_yaml(
            "raw_orders", deployments_dir
        )
        assert table_name == "ingest-raw-orders"
        assert crons == [
            {"cron": "0 6 * * *", "timezone": "Europe/Madrid"},
            {"cron": "0 14 * * *", "timezone": "Europe/Madrid"},
        ]

    def test_dbt_node_returns_empty_tuple(self, deployments_dir):
        assert _uns.get_source_config_prefect_yaml("int_orders", deployments_dir) == (
            "",
            [],
        )

    def test_unknown_node_returns_empty_tuple(self, deployments_dir):
        assert _uns.get_source_config_prefect_yaml("nonexistent", deployments_dir) == (
            "",
            [],
        )

    def test_yaml_anchor_in_schedule_is_resolved(self, deployments_dir):
        table_name, crons = _uns.get_source_config_prefect_yaml(
            "raw_products", deployments_dir
        )
        assert table_name == "ingest-raw-products"
        assert len(crons) == 1
        assert crons[0]["cron"] == "0 0 * * *"
        assert crons[0]["timezone"] == "Europe/Madrid"

    def test_yaml_merge_key_override_is_resolved(self, deployments_dir):
        table_name, crons = _uns.get_source_config_prefect_yaml(
            "raw_customers", deployments_dir
        )
        assert table_name == "ingest-raw-customers"
        assert len(crons) == 1
        assert crons[0]["cron"] == "0 3 * * *"
        assert crons[0]["timezone"] == "Europe/Madrid"


class TestParseSla:
    @pytest.mark.parametrize(
        ("sla_str", "expected"),
        [
            ("24 hours", timedelta(hours=24)),
            ("24h", timedelta(hours=24)),
            ("1 hour", timedelta(hours=1)),
            ("30 minutes", timedelta(minutes=30)),
            ("30m", timedelta(minutes=30)),
            ("1 min", timedelta(minutes=1)),
            ("1.5h", timedelta(hours=1.5)),
            ("7 days", timedelta(days=7)),
            ("7d", timedelta(days=7)),
            ("1 day", timedelta(days=1)),
            ("1 month", relativedelta(months=1)),
            ("2months", relativedelta(months=2)),
            ("1mo", relativedelta(months=1)),
            ("1 year", relativedelta(years=1)),
            ("2years", relativedelta(years=2)),
            ("1yr", relativedelta(years=1)),
            ("1y", relativedelta(years=1)),
            ("10:00", dt_time(10, 0)),
            ("14:30", dt_time(14, 30)),
        ],
    )
    def test_parses_supported_formats(self, sla_str, expected):
        assert _uns.parse_sla(sla_str) == expected

    def test_invalid_format_raises_value_error(self):
        with pytest.raises(ValueError, match="Cannot parse SLA"):
            _uns.parse_sla("tomorrow")


class TestCalcFreshUntilFromCron:
    def test_dict_cron_returns_next_utc_run(self):
        result = _uns._calc_fresh_until_from_cron(
            cron=[{"cron": "0 12 * * *", "timezone": "UTC"}],
            now=_FROZEN_NOW,
        )

        dt = datetime.fromisoformat(result).astimezone(timezone.utc)
        assert dt.hour == 12
        assert dt.minute == 0
        assert dt.date() == _FROZEN_NOW.date()

    def test_string_cron_returns_run_after_now(self):
        result = _uns._calc_fresh_until_from_cron(cron=["0 12 * * *"], now=_FROZEN_NOW)
        assert result is not None
        assert datetime.fromisoformat(result) > _FROZEN_NOW

    def test_empty_cron_list_returns_none(self):
        assert _uns._calc_fresh_until_from_cron(cron=[], now=_FROZEN_NOW) is None

    def test_returns_earliest_of_multiple_crons(self):
        result = _uns._calc_fresh_until_from_cron(
            cron=[
                {"cron": "0 15 * * *", "timezone": "UTC"},
                {"cron": "0 11 * * *", "timezone": "UTC"},
            ],
            now=_FROZEN_NOW,
        )

        dt = datetime.fromisoformat(result).astimezone(timezone.utc)
        assert dt.hour == 11


class TestCalcFreshUntilFromSla:
    def test_ignored_sla_returns_none(self):
        assert _uns._calc_fresh_until_from_sla("ignored", _FROZEN_NOW) is None
        assert _uns._calc_fresh_until_from_sla("IGNORED", _FROZEN_NOW) is None
        assert _uns._calc_fresh_until_from_sla("n/a", _FROZEN_NOW) is None
        assert _uns._calc_fresh_until_from_sla("N/A", _FROZEN_NOW) is None

    def test_timedelta_sla_returns_now_plus_delta(self):
        result = _uns._calc_fresh_until_from_sla("6h", _FROZEN_NOW)
        assert result == (_FROZEN_NOW + timedelta(hours=6)).isoformat()

    def test_wallclock_sla_in_future_returns_same_day(self):
        result = _uns._calc_fresh_until_from_sla("14:30", _FROZEN_NOW)
        dt = datetime.fromisoformat(result)
        assert dt.hour == 14
        assert dt.minute == 30
        assert dt.year == 2026
        assert dt.month == 3
        assert dt.day == 19

    def test_wallclock_sla_passed_rolls_to_next_day(self):
        result = _uns._calc_fresh_until_from_sla("08:00", _FROZEN_NOW)
        dt = datetime.fromisoformat(result)
        assert dt.day == 20
        assert dt.hour == 8
        assert dt.minute == 0


class TestCalcFreshUntil:
    def test_cron_takes_priority_over_sla(self):
        cron_result = _uns._calc_fresh_until(
            cron=[{"cron": "0 12 * * *", "timezone": "UTC"}],
            sla="6h",
            reference_time=_FROZEN_NOW,
        )
        sla_result = _uns._calc_fresh_until(
            cron=None, sla="6h", reference_time=_FROZEN_NOW
        )

        assert cron_result != sla_result
        dt = datetime.fromisoformat(cron_result).astimezone(timezone.utc)
        assert dt.hour == 12

    def test_returns_none_when_both_cron_and_sla_are_missing(self):
        assert (
            _uns._calc_fresh_until(cron=None, sla=None, reference_time=_FROZEN_NOW)
            is None
        )
        assert (
            _uns._calc_fresh_until(cron=[], sla=None, reference_time=_FROZEN_NOW)
            is None
        )

    def test_model_month_sla_returns_calendar_month_delta(self):
        result = _uns._calc_fresh_until(
            sla="1 month", cron=None, reference_time=_FROZEN_NOW
        )
        assert result == (_FROZEN_NOW + relativedelta(months=1)).isoformat()

    def test_model_month_sla_handles_february_boundary(self):
        jan_31 = datetime(2026, 1, 31, 10, 0, 0, tzinfo=timezone.utc)
        result = _uns._calc_fresh_until(sla="1 month", cron=None, reference_time=jan_31)
        dt = datetime.fromisoformat(result)
        assert dt.month == 2
        assert dt.day == 28

    def test_model_year_sla_returns_now_plus_one_year(self):
        result = _uns._calc_fresh_until(
            sla="1 year", cron=None, reference_time=_FROZEN_NOW
        )
        assert result == (_FROZEN_NOW + relativedelta(years=1)).isoformat()

    def test_model_day_sla_returns_now_plus_delta(self):
        result = _uns._calc_fresh_until(sla="7d", cron=None, reference_time=_FROZEN_NOW)
        assert result == (_FROZEN_NOW + timedelta(days=7)).isoformat()


class TestWriteNodeStateToS3:
    def test_calls_update_json_with_expected_payload(self, monkeypatch, credentials):
        state = {
            "table_name": "mart_sales",
            "status": "success",
            "fresh_until": "2026-03-19T12:00:00+00:00",
        }
        mock_update = MagicMock()
        mock_create = MagicMock()
        monkeypatch.setattr(_uns, "_update_json", mock_update)
        monkeypatch.setattr(_uns, "_create_new_state_file", mock_create)

        _uns._write_node_state_to_s3(
            state=state,
            credentials=credentials,
            state_file_path="s3://bucket/node-state.json",
        )

        mock_update.assert_called_once_with(
            state_file_path="s3://bucket/node-state.json",
            state=state,
            max_retries=3,
            aws_credentials=credentials,
        )
        mock_create.assert_not_called()

    def test_falls_back_to_create_new_file_when_state_file_missing(
        self, monkeypatch, credentials
    ):
        state = {"table_name": "mart_sales", "status": "failed", "fresh_until": None}
        mock_update = MagicMock(side_effect=FileNotFoundError)
        mock_create = MagicMock()
        monkeypatch.setattr(_uns, "_update_json", mock_update)
        monkeypatch.setattr(_uns, "_create_new_state_file", mock_create)

        _uns._write_node_state_to_s3(
            state=state,
            credentials=credentials,
            state_file_path="s3://bucket/node-state.json",
        )

        mock_create.assert_called_once_with(
            "s3://bucket/node-state.json",
            "mart_sales",
            state,
            credentials,
        )


class TestUpdateNodeStateTask:
    def test_success_status_calculates_fresh_until_and_writes_state(
        self, monkeypatch, credentials
    ):
        mock_calc = MagicMock(return_value="2026-03-20T10:00:00+00:00")
        mock_write = MagicMock()
        monkeypatch.setattr(_uns, "_calc_fresh_until", mock_calc)
        monkeypatch.setattr(_uns, "_write_node_state_to_s3", mock_write)

        _uns.update_node_state.fn(
            table_name="mart_sales",
            status="success",
            state_path="s3://bucket/node-state.json",
            node_type="model",
            credentials=credentials,
            sla="6h",
            owners=[{"email": "owner@example.com", "type": "Technical Owner"}],
            effective_source_data_slot="2026-03-19T09:00:00+00:00",
            batch_id=123,
            cron=["0 */6 * * *"],
            trigger_delay=10,
            sla_breach_grace_period=45,
        )

        calc_args = mock_calc.call_args.args
        assert calc_args[0] == ["0 */6 * * *"]
        assert calc_args[1] == "6h"
        assert isinstance(calc_args[2], datetime)

        write_kwargs = mock_write.call_args.kwargs
        state = write_kwargs["state"]
        assert state["table_name"] == "mart_sales"
        assert state["node_type"] == "model"
        assert state["status"] == "success"
        assert state["fresh_until"] == "2026-03-20T10:00:00+00:00"
        assert state["SLA"] == "6h"
        assert state["owners"] == [
            {"email": "owner@example.com", "type": "Technical Owner"}
        ]
        assert state["effective_source_data_slot"] == "2026-03-19T09:00:00+00:00"
        assert state["batch_id"] == 123
        assert state["cron"] == ["0 */6 * * *"]
        assert state["trigger_delay"] == 10
        assert state["sla_breach_grace_period"] == 45
        datetime.fromisoformat(state["last_refreshed_at"])

        assert write_kwargs["state_file_path"] == "s3://bucket/node-state.json"
        assert write_kwargs["credentials"] == credentials

    def test_non_success_status_does_not_calculate_fresh_until(
        self, monkeypatch, credentials
    ):
        mock_calc = MagicMock()
        mock_write = MagicMock()
        monkeypatch.setattr(_uns, "_calc_fresh_until", mock_calc)
        monkeypatch.setattr(_uns, "_write_node_state_to_s3", mock_write)

        _uns.update_node_state.fn(
            table_name="mart_sales",
            status="failed",
            state_path="s3://bucket/node-state.json",
            node_type="model",
            credentials=credentials,
            sla="6h",
            cron=["0 */6 * * *"],
        )

        mock_calc.assert_not_called()
        state = mock_write.call_args.kwargs["state"]
        assert state["fresh_until"] is None
        assert state["status"] == "failed"

    def test_raises_for_unsupported_store_type(self, credentials):
        with pytest.raises(
            NotImplementedError,
            match="State store type 'local' is not supported",
        ):
            _uns.update_node_state.fn(
                table_name="mart_sales",
                status="failed",
                state_path="s3://bucket/node-state.json",
                node_type="model",
                credentials=credentials,
                store_type="local",
            )
