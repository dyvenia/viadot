"""Utilities for dbt orchestration tasks."""

from pathlib import Path
import textwrap

from prefect import get_run_logger
from ruamel.yaml import YAML


_yaml = YAML(typ="safe", pure=True)

NODE_NAME_PARAM_NAMES = ["table", "redshift_table"]


def get_node_schedules_prefect_yaml(
    node_name: str, deployments_dir: str | Path | None = None
) -> list[dict[str, str]]:
    """Retrieve the schedules of a source node from its ingestion deployment.

    Scans Prefect deployment YAML files under ``deployments_dir`` to find an
    ingestion deployment whose ``table`` or ``redshift_table`` parameter matches
    ``node_name``. dbt model deployments are intentionally ignored — their
    freshness is determined entirely by whether their upstream sources are fresh.

    Args:
        node_name: Short name of the source node to look up.
        deployments_dir: Directory containing Prefect deployment YAML files.
            Defaults to ``<this_file's_parent>/../../deployments``.

    Returns:
        A list of cron schedule dicts. Returns an empty list if no matching deployment
        is found.
    """
    logger = get_run_logger()

    if not deployments_dir:
        deployments_dir = Path(__file__).parent.parent.parent / "deployments"
    else:
        deployments_dir = Path(deployments_dir)

    logger.info(f"Retrieving node '{node_name}' schedule from '{deployments_dir}'...")

    base_yaml = deployments_dir / "prefect_base.yaml"
    base_content = base_yaml.read_text() if base_yaml.exists() else ""

    # Parse each deployment file individually, prepending the base file so that
    # YAML anchors defined there (e.g. &default_schedule) are available.
    file_count = 0
    for yaml_file in deployments_dir.rglob("*.yaml"):
        if yaml_file == base_yaml:
            continue

        logger.debug(f"Inspecting deployment file '{yaml_file}'...")

        file_content = textwrap.indent(yaml_file.read_text().strip(), "  ")
        deployments = _yaml.load(base_content + "\n" + file_content).get("deployments")
        for deployment in deployments:
            params = deployment.get("parameters", {})
            if not params:
                logger.warning(
                    f"Deployment '{deployment.get('name')}' has no parameters defined."
                )

            for key in NODE_NAME_PARAM_NAMES:
                if params.get(key) == node_name:
                    logger.info(
                        f"Retrieving schedules from deployment '{deployment.get('name')}'..."
                    )
                    return deployment.get("schedules") or []
        file_count += 1

    logger.info(
        f"No schedules found for node '{node_name}' (checked {file_count} deployment files)."
    )
    return []
