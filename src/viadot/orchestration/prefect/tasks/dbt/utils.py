"""Utilities for dbt orchestration tasks."""

from collections.abc import Iterator
from pathlib import Path
import textwrap

from prefect import get_run_logger
from ruamel.yaml import YAML


_yaml = YAML(typ="safe", pure=True)

NODE_NAME_PARAM_NAMES = ["table", "redshift_table"]


def _iter_prefect_deployments(deployments_dir: str | Path | None) -> Iterator[dict]:
    """Yield deployments parsed from YAML files under ``deployments_dir``."""
    directory = Path(deployments_dir or "prefect/deployments").resolve()
    base_yaml = directory / "prefect_base.yaml"
    base_content = base_yaml.read_text() if base_yaml.exists() else ""

    deployment_files = [
        *directory.rglob("*.yaml"),
        *directory.rglob("*.yml"),
    ]
    for yaml_file in deployment_files:
        if yaml_file == base_yaml:
            continue

        file_content = textwrap.indent(yaml_file.read_text().strip(), "  ")
        parsed_yaml = _yaml.load(base_content + "\n" + file_content)
        deployments = (
            parsed_yaml.get("deployments", [])
            if isinstance(parsed_yaml, dict)
            else parsed_yaml
        )
        yield from deployments or []


def get_node_schedules_prefect_yaml(
    node_name: str, deployments_dir: str | Path | None = "prefect/deployments"
) -> list[dict[str, str]]:
    """Retrieve the schedules of a source node from its ingestion deployment.

    Scans Prefect deployment YAML files under ``deployments_dir`` to find an
    ingestion deployment whose ``table`` or ``redshift_table`` parameter matches
    ``node_name``. dbt model deployments are intentionally ignored — their
    freshness is determined entirely by whether their upstream sources are fresh.

    Args:
        node_name: Short name of the source node to look up.
        deployments_dir: Directory containing Prefect deployment YAML files.
            Defaults to ``prefect/deployments``.

    Returns:
        A list of cron schedule dicts. Returns an empty list if no matching deployment
        is found.
    """
    logger = get_run_logger()

    logger.info(f"Retrieving node '{node_name}' schedule from Prefect deployments...")
    for deployment in _iter_prefect_deployments(deployments_dir):
        params = deployment.get("parameters", {})
        if not params:
            logger.warning(
                f"Deployment '{deployment.get('name')}' has no parameters defined."
            )

        if any(params.get(key) == node_name for key in NODE_NAME_PARAM_NAMES):
            schedules = deployment.get("schedules")
            logger.info("Retrieved the following schedules: %s", schedules)
            return schedules or []

    logger.info(
        f"No schedules found for node '{node_name}' in Prefect deployment files."
    )
    return []


def get_state_tracked_nodes_prefect_yaml(
    deployments_dir: str | Path | None = "prefect/deployments",
) -> set[str]:
    """Return node names whose Prefect deployments enable ``track_state``."""
    logger = get_run_logger()
    tracked_nodes: set[str] = set()

    for deployment in _iter_prefect_deployments(deployments_dir):
        parameters = deployment.get("parameters", {})
        if parameters.get("track_state") is True:
            tracked_nodes.update(
                node_name
                for key in (
                    "model_name",
                    "node_name",
                    *NODE_NAME_PARAM_NAMES,
                )
                if isinstance((node_name := parameters.get(key)), str)
            )

    logger.info(f"Found state-tracked nodes in Prefect deployments: {tracked_nodes}")
    return tracked_nodes
