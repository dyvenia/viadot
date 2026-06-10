from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from viadot.orchestration.prefect.flows.dbt_sla_monitor import (
    _notify_and_mark_breaches,
    sla_monitor,
)


def _iso_in_past(minutes: int = 120) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _iso_in_future(minutes: int = 120) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


class TestNotifyAndMarkBreaches:
    def test_non_dry_run_sends_once_per_owner_and_marks_unique_nodes(self, monkeypatch):
        # owner -> [(node_name, fresh_until), ...]
        owner_breaches = {
            "a@example.com": [
                ("model_a", "2026-01-01T00:00:00+00:00"),
                ("model_b", "2026-01-01T00:00:00+00:00"),
            ],
            "b@example.com": [
                ("model_b", "2026-01-01T00:00:00+00:00"),
                ("model_c", "2026-01-01T00:00:00+00:00"),
            ],
        }

        notify_mock = MagicMock()
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.notify_sla_breaches",
            notify_mock,
        )

        store = MagicMock()
        logger = MagicMock()

        _notify_and_mark_breaches(
            owner_breaches=owner_breaches,
            dry_run=False,
            smtp_credentials_secret="smtp-secret",
            store=store,
            prefect_logger=logger,
        )

        assert notify_mock.call_count == 2
        notify_mock.assert_any_call(
            recipient="a@example.com",
            breaches=owner_breaches["a@example.com"],
            smtp_credentials_secret="smtp-secret",
        )
        notify_mock.assert_any_call(
            recipient="b@example.com",
            breaches=owner_breaches["b@example.com"],
            smtp_credentials_secret="smtp-secret",
        )

        written_node_names = {
            call.kwargs["node_state"]["table_name"]
            for call in store.write.call_args_list
        }
        assert written_node_names == {"model_a", "model_b", "model_c"}
        assert store.write.call_count == 3

    def test_dry_run_does_not_send_or_mark(self, monkeypatch):
        notify_mock = MagicMock()
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.notify_sla_breaches",
            notify_mock,
        )

        store = MagicMock()
        logger = MagicMock()
        owner_breaches = {"a@example.com": [("model_a", "2026-01-01T00:00:00+00:00")]}

        _notify_and_mark_breaches(
            owner_breaches=owner_breaches,
            dry_run=True,
            smtp_credentials_secret=None,
            store=store,
            prefect_logger=logger,
        )

        notify_mock.assert_not_called()
        store.write.assert_not_called()


class TestSlaMonitor:
    def test_groups_breaches_per_owner_before_notifying(self, monkeypatch):
        state = {
            "model_a": {
                "table_name": "model_a",
                "node_type": "model",
                "status": "failed",
                "fresh_until": _iso_in_past(),
                "sla_breach_grace_period": 30,
                "owners": [{"email": "a@example.com", "type": "technical owner"}],
            },
            "model_b": {
                "table_name": "model_b",
                "node_type": "model",
                "status": "failed",
                "fresh_until": _iso_in_past(),
                "sla_breach_grace_period": 30,
                "owners": [{"email": "a@example.com", "type": "technical owner"}],
            },
            "model_running": {
                "table_name": "model_running",
                "node_type": "model",
                "status": "running",
                "fresh_until": _iso_in_past(),
                "owners": [{"email": "a@example.com", "type": "technical owner"}],
            },
            "source_x": {
                "table_name": "source_x",
                "node_type": "source",
                "status": "failed",
                "fresh_until": _iso_in_past(),
                "owners": [{"email": "a@example.com", "type": "technical owner"}],
            },
        }

        fake_store = MagicMock()
        fake_store._read.return_value = (state, None)
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.StateStore",
            MagicMock(return_value=fake_store),
        )
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.get_credentials",
            MagicMock(return_value={}),
        )

        captured = {}

        def _capture_notify(
            owner_breaches, dry_run, smtp_credentials_secret, store, prefect_logger
        ):
            captured["owner_breaches"] = owner_breaches
            captured["dry_run"] = dry_run
            captured["smtp_credentials_secret"] = smtp_credentials_secret
            captured["store"] = store

        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor._notify_and_mark_breaches",
            _capture_notify,
        )
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.get_run_logger",
            MagicMock(return_value=MagicMock()),
        )

        sla_monitor.fn(
            state_path="s3://bucket/state.json",
            state_store_credentials_secret="state-secret",
            smtp_credentials_secret="smtp-secret",
            owner_type="technical owner",
            dry_run=False,
        )

        assert captured["dry_run"] is False
        assert captured["smtp_credentials_secret"] == "smtp-secret"
        assert captured["store"] is fake_store
        assert list(captured["owner_breaches"].keys()) == ["a@example.com"]
        assert [node for node, _ in captured["owner_breaches"]["a@example.com"]] == [
            "model_a",
            "model_b",
        ]

    def test_resets_notification_flag_when_node_is_fresh_again(self, monkeypatch):
        state = {
            "model_ok": {
                "table_name": "model_ok",
                "node_type": "model",
                "status": "success",
                "fresh_until": _iso_in_future(),
                "_sla_breach_notification_sent": True,
                "owners": [{"email": "a@example.com", "type": "technical owner"}],
            }
        }

        fake_store = MagicMock()
        fake_store._read.return_value = (state, None)
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.StateStore",
            MagicMock(return_value=fake_store),
        )
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.get_credentials",
            MagicMock(return_value={}),
        )
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor.get_run_logger",
            MagicMock(return_value=MagicMock()),
        )

        notify_and_mark = MagicMock()
        monkeypatch.setattr(
            "viadot.orchestration.prefect.flows.dbt_sla_monitor._notify_and_mark_breaches",
            notify_and_mark,
        )

        sla_monitor.fn(
            state_path="s3://bucket/state.json",
            state_store_credentials_secret="state-secret",
            smtp_credentials_secret="smtp-secret",
            owner_type="technical owner",
            dry_run=False,
        )

        fake_store.write.assert_called_once_with(
            node_state={
                "table_name": "model_ok",
                "_sla_breach_notification_sent": False,
            }
        )
        notify_and_mark.assert_not_called()
