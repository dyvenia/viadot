from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest
import trigger_downstream_node as _tdn


@pytest.fixture(autouse=True)
def mock_prefect_logger(monkeypatch):
    mock_log = MagicMock()
    monkeypatch.setattr(_tdn, "get_run_logger", lambda: mock_log)
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
                "depends_on": {"nodes": ["source.lakehouse.staging.raw_orders"]},
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


_FROZEN_NOW = datetime(2026, 3, 19, 10, 30, 0, tzinfo=timezone.utc)


class TestIsFresh:
    def test_fresh_when_fresh_until_is_in_the_future(self):
        state = {"raw_orders": {"fresh_until": "2026-03-19T11:00:00+00:00"}}
        assert _tdn.is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is True

    def test_stale_when_fresh_until_is_in_the_past(self):
        state = {"raw_orders": {"fresh_until": "2026-03-19T10:00:00+00:00"}}
        assert _tdn.is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is False

    def test_fresh_when_node_not_in_state(self):
        assert _tdn.is_fresh("raw_orders", {}, reference_time=_FROZEN_NOW) is True

    def test_fresh_when_fresh_until_is_missing(self):
        state = {"raw_orders": {"status": "success"}}
        assert _tdn.is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is True

    def test_at_exact_boundary_is_stale(self):
        state = {"raw_orders": {"fresh_until": _FROZEN_NOW.isoformat()}}
        assert _tdn.is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is False

    def test_trigger_delay_is_applied_to_reference_time(self):
        state = {
            "raw_orders": {
                "fresh_until": "2026-03-19T10:35:00+00:00",
                "trigger_delay": 10,
            }
        }
        assert _tdn.is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is False

    def test_naive_fresh_until_is_treated_as_utc(self):
        state = {"raw_orders": {"fresh_until": "2026-03-19T10:40:00"}}
        assert _tdn.is_fresh("raw_orders", state, reference_time=_FROZEN_NOW) is True


class TestGetUpstreams:
    def test_returns_upstream_sources_only(self, manifest):
        assert _tdn.get_upstreams("int_orders", manifest) == ["raw_orders"]

    def test_returns_upstream_models_only(self, manifest):
        assert _tdn.get_upstreams("mart_sales", manifest) == [
            "int_orders",
            "int_customers",
        ]

    def test_returns_multiple_upstream_sources(self, manifest):
        assert _tdn.get_upstreams("int_order_items", manifest) == [
            "raw_orders",
            "raw_products",
        ]

    def test_unknown_model_returns_empty_list(self, manifest):
        assert _tdn.get_upstreams("nonexistent_model", manifest) == []

    def test_model_with_no_upstreams_returns_empty_list(self, manifest):
        manifest["nodes"]["model.lakehouse.isolated_model"] = {
            "resource_type": "model",
            "name": "isolated_model",
            "depends_on": {"nodes": []},
        }
        assert _tdn.get_upstreams("isolated_model", manifest) == []


class TestGetDirectDownstreamNodes:
    def test_source_node_returns_direct_downstream_models(self, manifest):
        assert set(_tdn.get_direct_downstream_nodes("raw_orders", manifest)) == {
            "int_orders",
            "int_order_items",
        }

    def test_model_node_returns_direct_downstream(self, manifest):
        assert _tdn.get_direct_downstream_nodes("int_orders", manifest) == [
            "mart_sales"
        ]

    def test_model_with_no_downstreams_returns_empty_list(self, manifest):
        assert _tdn.get_direct_downstream_nodes("mart_sales", manifest) == []

    def test_unknown_node_returns_empty_list(self, manifest):
        assert _tdn.get_direct_downstream_nodes("nonexistent", manifest) == []


class TestReadNodeState:
    def test_read_node_state_delegates_to_s3_reader(self, monkeypatch, credentials):
        expected = {"raw_orders": {"status": "success"}}
        mock_read = MagicMock(return_value=expected)
        monkeypatch.setattr(_tdn, "_read_node_state_from_s3", mock_read)

        result = _tdn.read_node_state(
            state_path="s3://bucket/node-state.json",
            credentials=credentials,
            table_name="raw_orders",
        )

        assert result == expected
        mock_read.assert_called_once_with(
            credentials=credentials,
            state_file_path="s3://bucket/node-state.json",
            table_name="raw_orders",
        )

    def test_read_node_state_raises_for_unsupported_store_type(self, credentials):
        with pytest.raises(
            NotImplementedError,
            match="State store type 'local' is not supported",
        ):
            _tdn.read_node_state(
                state_path="s3://bucket/node-state.json",
                credentials=credentials,
                store_type="local",
            )


class TestReadNodeStateFromS3:
    def test_returns_full_state_when_table_name_is_none(self, monkeypatch, credentials):
        payload = {
            "raw_orders": {"status": "success"},
            "raw_customers": {"status": "failed"},
        }
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = payload
        mock_s3_cls = MagicMock(return_value=mock_s3)
        monkeypatch.setattr(_tdn, "S3", mock_s3_cls)

        result = _tdn._read_node_state_from_s3(
            credentials=credentials,
            state_file_path="s3://bucket/node-state.json",
            table_name=None,
        )

        assert result == payload
        mock_s3_cls.assert_called_once_with(credentials=credentials)
        mock_s3.to_dict.assert_called_once_with(path="s3://bucket/node-state.json")

    def test_returns_selected_table_state(self, monkeypatch, credentials):
        payload = {"raw_orders": {"status": "success"}}
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = payload
        monkeypatch.setattr(_tdn, "S3", MagicMock(return_value=mock_s3))

        result = _tdn._read_node_state_from_s3(
            credentials=credentials,
            state_file_path="s3://bucket/node-state.json",
            table_name="raw_orders",
        )

        assert result == payload["raw_orders"]
        mock_s3.to_dict.assert_called_once_with(path="s3://bucket/node-state.json")

    def test_returns_none_for_missing_table_key(self, monkeypatch, credentials):
        mock_s3 = MagicMock()
        mock_s3.to_dict.return_value = {"raw_orders": {"status": "success"}}
        monkeypatch.setattr(_tdn, "S3", MagicMock(return_value=mock_s3))

        result = _tdn._read_node_state_from_s3(
            credentials=credentials,
            state_file_path="s3://bucket/node-state.json",
            table_name="raw_products",
        )

        assert result is None

    def test_propagates_s3_errors(self, monkeypatch, credentials):
        mock_s3 = MagicMock()
        mock_s3.to_dict.side_effect = FileNotFoundError("missing state")
        monkeypatch.setattr(_tdn, "S3", MagicMock(return_value=mock_s3))

        with pytest.raises(FileNotFoundError, match="missing state"):
            _tdn._read_node_state_from_s3(
                credentials=credentials,
                state_file_path="s3://bucket/node-state.json",
                table_name=None,
            )


class TestGetRunnableNodes:
    def test_all_upstreams_fresh_returns_all_direct_downstreams(
        self, monkeypatch, manifest
    ):
        state = {"raw_products": {"fresh_until": "2099-01-01T00:00:00+00:00"}}
        monkeypatch.setattr(_tdn, "read_node_state", MagicMock(return_value=state))

        runnable, stale = _tdn.get_runnable_nodes.fn(
            node_name="raw_orders",
            manifest=manifest,
            state_path="s3://bucket/state.json",
            state_store_credentials={},
        )

        assert set(runnable) == {"int_orders", "int_order_items"}
        assert stale == {}

    def test_stale_upstream_excludes_model(self, monkeypatch, manifest):
        state = {"raw_products": {"fresh_until": "2000-01-01T00:00:00+00:00"}}
        monkeypatch.setattr(_tdn, "read_node_state", MagicMock(return_value=state))

        runnable, stale = _tdn.get_runnable_nodes.fn(
            node_name="raw_orders",
            manifest=manifest,
            state_path="s3://bucket/state.json",
            state_store_credentials={},
        )

        assert runnable == ["int_orders"]
        assert stale == {"int_order_items": ["raw_products"]}

    def test_skips_freshness_check_for_current_node(self, monkeypatch, manifest):
        state = {
            "raw_orders": {"fresh_until": "2000-01-01T00:00:00+00:00"},
            "raw_products": {"fresh_until": "2099-01-01T00:00:00+00:00"},
        }
        monkeypatch.setattr(_tdn, "read_node_state", MagicMock(return_value=state))

        runnable, stale = _tdn.get_runnable_nodes.fn(
            node_name="raw_orders",
            manifest=manifest,
            state_path="s3://bucket/state.json",
            state_store_credentials={},
        )

        assert set(runnable) == {"int_orders", "int_order_items"}
        assert stale == {}

    def test_source_with_no_direct_downstreams_returns_empty(
        self, monkeypatch, manifest
    ):
        monkeypatch.setattr(_tdn, "read_node_state", MagicMock(return_value={}))

        runnable, stale = _tdn.get_runnable_nodes.fn(
            node_name="nonexistent_source",
            manifest=manifest,
            state_path="s3://bucket/state.json",
            state_store_credentials={},
        )

        assert runnable == []
        assert stale == {}

    def test_model_node_as_trigger_returns_direct_downstream(
        self, monkeypatch, manifest
    ):
        monkeypatch.setattr(_tdn, "read_node_state", MagicMock(return_value={}))

        runnable, stale = _tdn.get_runnable_nodes.fn(
            node_name="int_orders",
            manifest=manifest,
            state_path="s3://bucket/state.json",
            state_store_credentials={},
        )

        assert "mart_sales" in runnable
        assert stale == {}

    def test_reads_state_from_s3_with_expected_arguments(self, monkeypatch, manifest):
        mock_read = MagicMock(return_value={})
        monkeypatch.setattr(_tdn, "read_node_state", mock_read)

        _tdn.get_runnable_nodes.fn(
            node_name="raw_orders",
            manifest=manifest,
            state_path="s3://bucket/state.json",
            state_store_credentials={"token": "x"},
        )

        mock_read.assert_called_once_with(
            state_path="s3://bucket/state.json",
            credentials={"token": "x"},
            table_name=None,
        )


class TestTriggerDownstreamNode:
    def test_triggers_safe_run_for_each_runnable_node(self, monkeypatch):
        mock_get_runnable = MagicMock(return_value=(["int_orders", "int_items"], {}))
        mock_safe_run = MagicMock()
        monkeypatch.setattr(_tdn, "get_runnable_nodes", mock_get_runnable)
        monkeypatch.setattr(_tdn, "safe_run_deployment", mock_safe_run)

        _tdn.trigger_downstream_node.fn(
            node_name="raw_orders",
            manifest={"nodes": {}, "sources": {}},
            state_path="s3://bucket/node-state.json",
            state_store_credentials={"token": "x"},
            flow_name="transform-flow",
        )

        mock_get_runnable.assert_called_once_with(
            node_name="raw_orders",
            manifest={"nodes": {}, "sources": {}},
            state_path="s3://bucket/node-state.json",
            state_store_credentials={"token": "x"},
        )
        mock_safe_run.assert_has_calls(
            [
                call(deployment_name="dbt_int_orders", flow_name="transform-flow"),
                call(deployment_name="dbt_int_items", flow_name="transform-flow"),
            ]
        )
        assert mock_safe_run.call_count == 2

    def test_does_not_trigger_when_no_runnable_nodes(self, monkeypatch):
        mock_get_runnable = MagicMock(return_value=([], {"int_orders": ["raw_items"]}))
        mock_safe_run = MagicMock()
        monkeypatch.setattr(_tdn, "get_runnable_nodes", mock_get_runnable)
        monkeypatch.setattr(_tdn, "safe_run_deployment", mock_safe_run)

        _tdn.trigger_downstream_node.fn(
            node_name="raw_orders",
            manifest={"nodes": {}, "sources": {}},
            state_path="s3://bucket/node-state.json",
            state_store_credentials={"token": "x"},
        )

        mock_safe_run.assert_not_called()


class TestSafeRunDeployment:
    def test_builds_fqn_and_calls_prefect_run_deployment(self, monkeypatch):
        fake_flow_run = object()
        mock_run_deployment = MagicMock(return_value=fake_flow_run)
        monkeypatch.setattr(_tdn, "run_deployment", mock_run_deployment)

        result = _tdn.safe_run_deployment.fn(
            deployment_name="dbt_mart_sales",
            flow_name="transform-and-catalog-v2",
        )

        assert result is fake_flow_run
        mock_run_deployment.assert_called_once_with(
            name="transform-and-catalog-v2/dbt_mart_sales",
            timeout=0,
        )
