"""Utilities for dbt orchestration tasks."""

from pathlib import Path

from prefect import get_run_logger
from ruamel.yaml import YAML


_yaml = YAML(typ="safe", pure=True)


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
    logger.info("Looking up source schedule configuration in deployment YAML files.")

    if not deployments_dir:
        deployments_dir = Path(__file__).parent.parent.parent / "deployments"
    else:
        deployments_dir = Path(deployments_dir)

    base_yaml = deployments_dir / "prefect_base.yaml"
    base_content = base_yaml.read_text() if base_yaml.exists() else ""

    # Parse each deployment file individually, prepending the base file so that
    # YAML anchors defined there (e.g. &default_schedule) are available.
    for yaml_file in deployments_dir.rglob("*.yaml"):
        if yaml_file == base_yaml:
            continue
        parsed = _yaml.load(base_content + "\n" + yaml_file.read_text()) or {}
        for deployment in parsed.get("deployments", []):
            params = deployment.get("parameters") or {}
            for key in ["table", "redshift_table"]:
                if params.get(key) == node_name:
                    logger.info("Matching deployment configuration found for source.")
                    return deployment.get("schedules") or []

    logger.info("No deployment configuration found for source.")
    return []
