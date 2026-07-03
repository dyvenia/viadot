from datetime import datetime, timedelta, timezone
from datetime import time as dt_time
import textwrap
from unittest.mock import MagicMock, call

from dateutil.relativedelta import relativedelta
from prefect.exceptions import ObjectNotFound
import pytest

from viadot.orchestration.dbt.manifest_handler import ManifestHandler
from viadot.orchestration.dbt.state_handler import StateHandler, is_fresh
from viadot.orchestration.prefect.tasks.dbt import (
    trigger_downstream_nodes,
    update_node_state,
)
from viadot.orchestration.prefect.tasks.dbt.utils import (
    get_node_schedules_prefect_yaml,
)


_MODULE = "viadot.orchestration.prefect.tasks.dbt"
_DC_MODULE = "viadot.orchestration.prefect.tasks.dbt.utils"

_FROZEN_NOW = datetime(2026, 3, 19, 10, 30, 0, tzinfo=timezone.utc)


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
                "depends_on": {"nodes": ["source.lakehouse.staging.raw_orders"]},
                "meta": {"SLA": "24h", "owners": [{"email": "eng@example.com"}]},
            },
            "model.lakehouse.int_order_items": {
                "resource_type": "model",
                "name": "int_order_items",
                "depends_on": {
                    "nodes": [
                        "source.lakehouse.staging.raw_orders",
                        "source.lakehouse.staging.raw_products",
                    ]
                },
            },
            "model.lakehouse.int_customers": {
                "resource_type": "model",
                "name": "int_customers",
                "depends_on": {"nodes": ["source.lakehouse.staging.raw_customers"]},
            },
            "model.lakehouse.mart_sales": {
                "resource_type": "model",
                "name": "mart_sales",
                "depends_on": {
                    "nodes": [
                        "model.lakehouse.int_orders",
                        "model.lakehouse.int_customers",
                    ]
                },
            },
        },
        "sources": {
            "source.lakehouse.staging.raw_orders": {
                "resource_type": "source",
                "name": "raw_orders",
                "meta": {"owners": [{"email": "source@example.com"}]},
            },
            "source.lakehouse.staging.raw_customers": {
                "resource_type": "source",
                "name": "raw_customers",
            },
            "source.lakehouse.staging.raw_products": {
                "resource_type": "source",
                "name": "raw_products",
            },
        },
    }


# ---------------------------------------------------------------------------
# is_fresh
# ---------------------------------------------------------------------------


class TestIsFresh:
    def test_fresh_when_fresh_until_is_in_the_future(self):
        state = {"raw_orders": {"fresh_until": "2026-03-19T11:00:00+00:00"}}
        assert is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is True

    def test_stale_when_fresh_until_is_in_the_past(self):
        state = {"raw_orders": {"fresh_until": "2026-03-19T10:00:00+00:00"}}
        assert is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is False

    def test_fresh_when_node_not_in_state(self):
        assert is_fresh("raw_orders", {}, reference_time=_FROZEN_NOW) is True

    def test_fresh_when_fresh_until_is_missing(self):
        state = {"raw_orders": {"status": "success"}}
        assert is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is True

    def test_at_exact_boundary_is_stale(self):
        state = {"raw_orders": {"fresh_until": _FROZEN_NOW.isoformat()}}
        assert is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is False

    def test_trigger_delay_is_applied_to_reference_time(self):
        state = {
            "raw_orders": {
                "fresh_until": "2026-03-19T10:35:00+00:00",
                "trigger_delay": 10,
            }
        }
        assert is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is False

    def test_naive_fresh_until_is_treated_as_utc(self):
        state = {"raw_orders": {"fresh_until": "2026-03-19T10:40:00"}}
        assert is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is True


# ---------------------------------------------------------------------------
# ManifestHandler
# ---------------------------------------------------------------------------


class TestGetNodeMeta:
    def test_returns_meta_for_model_node(self, manifest):
        assert ManifestHandler(manifest).get_node_meta("int_orders") == {
            "SLA": "24h",
            "owners": [{"email": "eng@example.com"}],
        }

    def test_returns_meta_for_source_node(self, manifest):
        assert ManifestHandler(manifest).get_node_meta("raw_orders") == {
            "owners": [{"email": "source@example.com"}],
        }

    def test_returns_empty_dict_when_node_missing(self, manifest):
        assert ManifestHandler(manifest).get_node_meta("nonexistent") == {}

    def test_returns_empty_dict_when_meta_is_none(self, manifest):
        manifest["nodes"]["model.lakehouse.int_orders"]["meta"] = None
        assert ManifestHandler(manifest).get_node_meta("int_orders") == {}


class TestGetUpstreams:
    def test_returns_upstream_sources_only(self, manifest):
        assert ManifestHandler(manifest).get_upstreams("int_orders") == ["raw_orders"]

    def test_returns_upstream_models_only(self, manifest):
        assert ManifestHandler(manifest).get_upstreams("mart_sales") == [
            "int_orders",
            "int_customers",
        ]

    def test_returns_multiple_upstream_sources(self, manifest):
        assert ManifestHandler(manifest).get_upstreams("int_order_items") == [
            "raw_orders",
            "raw_products",
        ]

    def test_unknown_model_returns_empty_list(self, manifest):
        assert ManifestHandler(manifest).get_upstreams("nonexistent_model") == []

    def test_model_with_no_upstreams_returns_empty_list(self, manifest):
        manifest["nodes"]["model.lakehouse.isolated_model"] = {
            "resource_type": "model",
            "name": "isolated_model",
            "depends_on": {"nodes": []},
        }
        assert ManifestHandler(manifest).get_upstreams("isolated_model") == []


class TestGetDirectDownstreamNodes:
    def test_source_node_returns_direct_downstream_models(self, manifest):
        assert set(
            ManifestHandler(manifest).get_direct_downstream_nodes("raw_orders")
        ) == {
            "int_orders",
            "int_order_items",
        }

    def test_model_node_returns_direct_downstream(self, manifest):
        assert ManifestHandler(manifest).get_direct_downstream_nodes("int_orders") == [
            "mart_sales"
        ]

    def test_model_with_no_downstreams_returns_empty_list(self, manifest):
        assert ManifestHandler(manifest).get_direct_downstream_nodes("mart_sales") == []

    def test_unknown_node_returns_empty_list(self, manifest):
        assert (
            ManifestHandler(manifest).get_direct_downstream_nodes("nonexistent") == []
        )


class TestGetRunnableNodes:
    def test_all_upstreams_fresh_returns_all_direct_downstreams(self, manifest):
        state = {"raw_products": {"fresh_until": "2099-01-01T00:00:00+00:00"}}
        runnable, stale = ManifestHandler(manifest).get_runnable_nodes(
            "raw_orders", state
        )
        assert set(runnable) == {"int_orders", "int_order_items"}
        assert stale == {}

    def test_stale_upstream_excludes_model(self, manifest):
        state = {"raw_products": {"fresh_until": "2000-01-01T00:00:00+00:00"}}
        runnable, stale = ManifestHandler(manifest).get_runnable_nodes(
            "raw_orders", state
        )
        assert runnable == ["int_orders"]
        assert stale == {"int_order_items": ["raw_products"]}

    def test_skips_freshness_check_for_current_node(self, manifest):
        state = {
            "raw_orders": {"fresh_until": "2000-01-01T00:00:00+00:00"},
            "raw_products": {"fresh_until": "2099-01-01T00:00:00+00:00"},
        }
        runnable, stale = ManifestHandler(manifest).get_runnable_nodes(
            "raw_orders", state
        )
        assert set(runnable) == {"int_orders", "int_order_items"}
        assert stale == {}

    def test_source_with_no_direct_downstreams_returns_empty(self, manifest):
        runnable, stale = ManifestHandler(manifest).get_runnable_nodes(
            "nonexistent_source", {}
        )
        assert runnable == []
        assert stale == {}

    def test_model_node_as_trigger_returns_direct_downstream(self, manifest):
        runnable, stale = ManifestHandler(manifest).get_runnable_nodes("int_orders", {})
        assert "mart_sales" in runnable
        assert stale == {}


# ---------------------------------------------------------------------------
# StateHandler
# ---------------------------------------------------------------------------


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
        assert StateHandler._parse_sla(sla_str) == expected

    def test_invalid_format_raises_value_error(self):
        with pytest.raises(ValueError, match="Cannot parse SLA"):
            StateHandler._parse_sla("tomorrow")
        with pytest.raises(ValueError, match="Cannot parse SLA"):
            StateHandler._parse_sla("")


class TestCalcFreshUntilFromCron:
    def test_dict_cron_returns_next_utc_run(self):
        result = StateHandler._calc_fresh_until_from_crons(
            crons=[{"cron": "0 12 * * *", "timezone": "UTC"}],
            now=_FROZEN_NOW,
        )
        dt = datetime.fromisoformat(result).astimezone(timezone.utc)
        assert dt.hour == 12
        assert dt.minute == 0
        assert dt.date() == _FROZEN_NOW.date()

    def test_string_cron_returns_run_after_now(self):
        result = StateHandler._calc_fresh_until_from_crons(
            crons=["0 12 * * *"], now=_FROZEN_NOW
        )
        assert result is not None
        assert datetime.fromisoformat(result) > _FROZEN_NOW

    def test_empty_cron_list_returns_none(self):
        assert (
            StateHandler._calc_fresh_until_from_crons(crons=[], now=_FROZEN_NOW) is None
        )

    def test_returns_earliest_of_multiple_schedules(self):
        result = StateHandler._calc_fresh_until_from_crons(
            crons=[
                {"cron": "0 15 * * *", "timezone": "UTC"},
                {"cron": "0 11 * * *", "timezone": "UTC"},
            ],
            now=_FROZEN_NOW,
        )
        dt = datetime.fromisoformat(result).astimezone(timezone.utc)
        assert dt.hour == 11


class TestCalcFreshUntilFromSla:
    def test_ignored_sla_returns_none(self):
        assert StateHandler._calc_fresh_until_from_sla("ignored", _FROZEN_NOW) is None
        assert StateHandler._calc_fresh_until_from_sla("IGNORED", _FROZEN_NOW) is None
        assert StateHandler._calc_fresh_until_from_sla("n/a", _FROZEN_NOW) is None
        assert StateHandler._calc_fresh_until_from_sla("N/A", _FROZEN_NOW) is None

    def test_timedelta_sla_returns_now_plus_delta(self):
        result = StateHandler._calc_fresh_until_from_sla("6h", _FROZEN_NOW)
        assert result == (_FROZEN_NOW + timedelta(hours=6)).isoformat()

    def test_wallclock_sla_in_future_returns_same_day(self):
        result = StateHandler._calc_fresh_until_from_sla("14:30", _FROZEN_NOW)
        dt = datetime.fromisoformat(result)
        assert dt.hour == 14
        assert dt.minute == 30
        assert dt.year == 2026
        assert dt.month == 3
        assert dt.day == 19

    def test_wallclock_sla_passed_rolls_to_next_day(self):
        result = StateHandler._calc_fresh_until_from_sla("08:00", _FROZEN_NOW)
        dt = datetime.fromisoformat(result)
        assert dt.day == 20
        assert dt.hour == 8
        assert dt.minute == 0


class TestCalcFreshUntil:
    def test_cron_takes_priority_over_sla(self):
        cron_result = StateHandler._calc_fresh_until(
            node_type="source",
            schedules=[{"cron": "0 12 * * *", "timezone": "UTC"}],
            sla="6h",
            reference_time=_FROZEN_NOW,
        )
        sla_result = StateHandler._calc_fresh_until(
            node_type="model", schedules=None, sla="6h", reference_time=_FROZEN_NOW
        )
        assert cron_result != sla_result
        dt = datetime.fromisoformat(cron_result).astimezone(timezone.utc)
        assert dt.hour == 12

    def test_returns_none_when_both_cron_and_sla_are_missing(self):
        assert (
            StateHandler._calc_fresh_until(
                node_type="source", schedules=None, sla=None, reference_time=_FROZEN_NOW
            )
            is None
        )
        assert (
            StateHandler._calc_fresh_until(
                node_type="model", schedules=[], sla=None, reference_time=_FROZEN_NOW
            )
            is None
        )

    def test_unknown_node_type_returns_none(self):
        assert (
            StateHandler._calc_fresh_until(
                node_type="unknown",
                schedules=[{"cron": "0 12 * * *", "timezone": "UTC"}],
                sla="6h",
                reference_time=_FROZEN_NOW,
            )
            is None
        )

    def test_model_month_sla_returns_calendar_month_delta(self):
        result = StateHandler._calc_fresh_until(
            node_type="model", sla="1 month", schedules=None, reference_time=_FROZEN_NOW
        )
        assert result == (_FROZEN_NOW + relativedelta(months=1)).isoformat()

    def test_model_month_sla_handles_february_boundary(self):
        jan_31 = datetime(2026, 1, 31, 10, 0, 0, tzinfo=timezone.utc)
        result = StateHandler._calc_fresh_until(
            node_type="model", sla="1 month", schedules=None, reference_time=jan_31
        )
        dt = datetime.fromisoformat(result)
        assert dt.month == 2
        assert dt.day == 28

    def test_model_year_sla_returns_now_plus_one_year(self):
        result = StateHandler._calc_fresh_until(
            node_type="model", sla="1 year", schedules=None, reference_time=_FROZEN_NOW
        )
        assert result == (_FROZEN_NOW + relativedelta(years=1)).isoformat()

    def test_model_day_sla_returns_now_plus_delta(self):
        result = StateHandler._calc_fresh_until(
            node_type="model", sla="7d", schedules=None, reference_time=_FROZEN_NOW
        )
        assert result == (_FROZEN_NOW + timedelta(days=7)).isoformat()


# ---------------------------------------------------------------------------
# deployment_config
# ---------------------------------------------------------------------------


class TestGetSourceConfigPrefectYaml:
    @pytest.fixture(autouse=True)
    def _mock_logger(self, monkeypatch):
        monkeypatch.setattr(f"{_DC_MODULE}.get_run_logger", MagicMock)

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
                deployments:
                """
            )
        )
        (tmp_path / "extract" / "ingestion.yaml").write_text(
            textwrap.dedent(
                """
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

    def test_ingestion_node_returns_schedules(self, deployments_dir):
        schedules = get_node_schedules_prefect_yaml("raw_orders", deployments_dir)
        assert schedules == [
            {"cron": "0 6 * * *", "timezone": "Europe/Madrid"},
            {"cron": "0 14 * * *", "timezone": "Europe/Madrid"},
        ]

    def test_dbt_node_returns_empty_list(self, deployments_dir):
        assert get_node_schedules_prefect_yaml("int_orders", deployments_dir) == []

    def test_unknown_node_returns_empty_list(self, deployments_dir):
        assert get_node_schedules_prefect_yaml("nonexistent", deployments_dir) == []

    def test_yaml_anchor_in_schedule_is_resolved(self, deployments_dir):
        schedules = get_node_schedules_prefect_yaml("raw_products", deployments_dir)
        assert len(schedules) == 1
        assert schedules[0]["cron"] == "0 0 * * *"
        assert schedules[0]["timezone"] == "Europe/Madrid"

    def test_yaml_merge_key_override_is_resolved(self, deployments_dir):
        schedules = get_node_schedules_prefect_yaml("raw_customers", deployments_dir)
        assert len(schedules) == 1
        assert schedules[0]["cron"] == "0 3 * * *"
        assert schedules[0]["timezone"] == "Europe/Madrid"


# ---------------------------------------------------------------------------
# update_node_state task
# ---------------------------------------------------------------------------


class TestUpdateNodeStateTask:
    @pytest.fixture(autouse=True)
    def _mock_logger(self, monkeypatch):
        monkeypatch.setattr(f"{_MODULE}.get_run_logger", MagicMock)

    def test_success_status_builds_state_and_calls_update(
        self, monkeypatch, credentials
    ):
        mock_handler = MagicMock()
        mock_handler.build_node_state.return_value = {
            "table_name": "mart_sales",
            "node_type": "model",
            "status": "success",
            "last_refreshed_at": "2026-03-19T10:30:00+00:00",
            "fresh_until": "2026-03-20T10:00:00+00:00",
            "SLA": "6h",
            "owners": [{"email": "owner@example.com", "type": "Technical Owner"}],
            "effective_source_data_slot": "2026-03-19T09:00:00+00:00",
            "batch_id": 123,
            "cron": ["0 */6 * * *"],
            "trigger_delay": 10,
            "sla_breach_grace_period": 45,
        }
        mock_artifact_store = MagicMock()
        mock_artifact_store.read_manifest.return_value = {}
        monkeypatch.setattr(f"{_MODULE}.StateHandler", lambda store: mock_handler)
        monkeypatch.setattr(
            f"{_MODULE}.StateStore", MagicMock(return_value=MagicMock())
        )
        monkeypatch.setattr(
            f"{_MODULE}.ArtifactStore", MagicMock(return_value=mock_artifact_store)
        )

        update_node_state.fn(
            node_name="mart_sales",
            status="success",
            node_type="model",
            state_path="s3://bucket/node-state.json",
            state_store_type="s3",
            artifact_store_path="s3://bucket/artifacts",
            artifact_store_type="s3",
            state_store_credentials=credentials,
            effective_source_data_slot="2026-03-19T09:00:00+00:00",
            batch_id=123,
            trigger_delay=10,
            sla_breach_grace_period_minutes=45,
        )

        mock_handler.build_node_state.assert_called_once_with(
            node_name="mart_sales",
            status="success",
            node_type="model",
            sla=None,
            owners=None,
            effective_source_data_slot="2026-03-19T09:00:00+00:00",
            batch_id=123,
            schedules=None,
            trigger_delay=10,
            sla_breach_grace_period_minutes=45,
        )
        mock_handler.update.assert_called_once_with(
            mock_handler.build_node_state.return_value
        )

    def test_non_success_status_does_not_calculate_fresh_until(
        self, monkeypatch, credentials
    ):
        mock_handler = MagicMock()
        mock_handler.build_node_state.return_value = {
            "node_name": "mart_sales",
            "status": "failed",
            "fresh_until": None,
        }
        mock_artifact_store = MagicMock()
        mock_artifact_store.read_manifest.return_value = {}
        monkeypatch.setattr(f"{_MODULE}.StateHandler", lambda store: mock_handler)
        monkeypatch.setattr(
            f"{_MODULE}.StateStore", MagicMock(return_value=MagicMock())
        )
        monkeypatch.setattr(
            f"{_MODULE}.ArtifactStore", MagicMock(return_value=mock_artifact_store)
        )

        update_node_state.fn(
            node_name="mart_sales",
            status="failed",
            state_path="s3://bucket/node-state.json",
            node_type="model",
            state_store_type="s3",
            artifact_store_path="s3://bucket/artifacts",
            artifact_store_type="s3",
            state_store_credentials=credentials,
        )

        mock_handler.build_node_state.assert_called_once_with(
            node_name="mart_sales",
            status="failed",
            node_type="model",
            sla=None,
            owners=None,
            effective_source_data_slot=None,
            batch_id=None,
            schedules=None,
            trigger_delay=0,
            sla_breach_grace_period_minutes=30,
        )
        mock_handler.update.assert_called_once()

    def test_raises_for_unsupported_store_type(self, monkeypatch, credentials):
        with pytest.raises(
            NotImplementedError,
            match="State store type 'local' is not supported",
        ):
            update_node_state.fn(
                node_name="mart_sales",
                status="failed",
                state_path="s3://bucket/node-state.json",
                node_type="model",
                state_store_type="local",
                artifact_store_path="s3://bucket/artifacts",
                artifact_store_type="s3",
                state_store_credentials=credentials,
            )


# ---------------------------------------------------------------------------
# trigger_downstream_nodes task
# ---------------------------------------------------------------------------


class TestTriggerDownstreamNodes:
    @pytest.fixture(autouse=True)
    def _mock_logger(self, monkeypatch):
        monkeypatch.setattr(f"{_MODULE}.get_run_logger", MagicMock)

    def test_triggers_run_deployment_for_each_runnable_node(
        self, monkeypatch, manifest
    ):
        mock_run = MagicMock()
        mock_store_instance = MagicMock()
        mock_store_instance._read.return_value = ({}, "etag1")
        monkeypatch.setattr(
            f"{_MODULE}.StateStore", MagicMock(return_value=mock_store_instance)
        )
        monkeypatch.setattr(f"{_MODULE}.run_deployment", mock_run)
        monkeypatch.setattr(
            ManifestHandler,
            "get_runnable_nodes",
            lambda self, n, s: (["int_orders", "int_items"], {}),
        )

        trigger_downstream_nodes.fn(
            node_name="raw_orders",
            manifest=manifest,
            state_path="s3://bucket/node-state.json",
            state_store_credentials={"token": "x"},
            flow_name="transform-flow",
        )

        mock_run.assert_has_calls(
            [
                call(name="transform-flow/dbt_int_orders", timeout=0, tags=None),
                call(name="transform-flow/dbt_int_items", timeout=0, tags=None),
            ]
        )
        assert mock_run.call_count == 2

    def test_does_not_trigger_when_no_runnable_nodes(self, monkeypatch, manifest):
        mock_run = MagicMock()
        mock_store_instance = MagicMock()
        mock_store_instance._read.return_value = ({}, "etag1")
        monkeypatch.setattr(
            f"{_MODULE}.StateStore", MagicMock(return_value=mock_store_instance)
        )
        monkeypatch.setattr(f"{_MODULE}.run_deployment", mock_run)
        monkeypatch.setattr(
            ManifestHandler,
            "get_runnable_nodes",
            lambda self, n, s: ([], {"int_orders": ["raw_items"]}),
        )

        trigger_downstream_nodes.fn(
            node_name="raw_orders",
            manifest=manifest,
            state_path="s3://bucket/node-state.json",
            state_store_credentials={"token": "x"},
        )

        mock_run.assert_not_called()

    def test_warns_and_continues_when_downstream_deployment_missing(
        self, monkeypatch, manifest
    ):
        mock_run = MagicMock(side_effect=[ObjectNotFound("missing"), None])
        mock_store_instance = MagicMock()
        mock_store_instance._read.return_value = ({}, "etag1")
        mock_logger = MagicMock()
        monkeypatch.setattr(
            f"{_MODULE}.StateStore", MagicMock(return_value=mock_store_instance)
        )
        monkeypatch.setattr(f"{_MODULE}.run_deployment", mock_run)
        monkeypatch.setattr(
            f"{_MODULE}.get_run_logger", MagicMock(return_value=mock_logger)
        )
        monkeypatch.setattr(
            ManifestHandler,
            "get_runnable_nodes",
            lambda self, n, s: (["int_orders", "int_items"], {}),
        )

        trigger_downstream_nodes.fn(
            node_name="raw_orders",
            manifest=manifest,
            state_path="s3://bucket/node-state.json",
            state_store_credentials={"token": "x"},
            flow_name="transform-flow",
            on_missing_downstream_deployment="warn",
        )

        mock_run.assert_has_calls(
            [
                call(name="transform-flow/dbt_int_orders", timeout=0, tags=None),
                call(name="transform-flow/dbt_int_items", timeout=0, tags=None),
            ]
        )
        assert mock_run.call_count == 2
        mock_logger.warning.assert_any_call(
            "Skipping missing downstream deployment '%s'.",
            "transform-flow/dbt_int_orders",
        )

    def test_raises_when_downstream_deployment_missing_in_raise_mode(
        self, monkeypatch, manifest
    ):
        mock_run = MagicMock(side_effect=ObjectNotFound("missing"))
        mock_store_instance = MagicMock()
        mock_store_instance._read.return_value = ({}, "etag1")
        monkeypatch.setattr(
            f"{_MODULE}.StateStore", MagicMock(return_value=mock_store_instance)
        )
        monkeypatch.setattr(f"{_MODULE}.run_deployment", mock_run)
        monkeypatch.setattr(
            ManifestHandler,
            "get_runnable_nodes",
            lambda self, n, s: (["int_orders"], {}),
        )

        with pytest.raises(ObjectNotFound):
            trigger_downstream_nodes.fn(
                node_name="raw_orders",
                manifest=manifest,
                state_path="s3://bucket/node-state.json",
                state_store_credentials={"token": "x"},
                flow_name="transform-flow",
            )
