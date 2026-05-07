"""Manifest traversal logic for dbt dynamic orchestration."""

import logging

from viadot.orchestration.dbt_dynamic.state_handler import is_fresh


logger = logging.getLogger(__name__)


class ManifestHandler:
    """Encapsulates traversal and freshness-gating logic over a dbt manifest.

    This class is a pure-Python layer with no Prefect dependency. It operates
    on a manifest dict that has already been loaded (e.g. by ``ManifestStore``).

    Args:
        manifest: The parsed dbt manifest dictionary.
    """

    def __init__(self, manifest: dict) -> None:
        """Initialize ManifestHandler with the given manifest dict."""
        self.manifest = manifest

    def get_node_meta(self, node_name: str) -> dict:
        """Return the ``meta`` block for a dbt node (model or source).

        Args:
            node_name: The short node name (e.g. ``"int_orders"``).

        Returns:
            The ``meta`` dict, or ``{}`` if the node is not found.
        """
        logger.info(f"Reading node '{node_name}' metadata from manifest.")
        all_nodes = {
            **self.manifest.get("nodes", {}),
            **self.manifest.get("sources", {}),
        }
        node = next((n for n in all_nodes.values() if n.get("name") == node_name), None)
        if not node:
            return {}
        return node.get("meta") or {}

    def get_upstreams(self, node_name: str) -> list[str]:
        """Return the short names of all direct upstream dependencies of a model node.

        Only model nodes have an explicit ``depends_on`` block; sources have no
        upstreams in this sense. If ``node_name`` is not a model the method
        returns an empty list.

        Args:
            node_name: Short name of the node to inspect.

        Returns:
            List of upstream node short names.
        """
        logger.info(f"Resolving upstream dependencies for node '{node_name}'.")
        nodes = self.manifest.get("nodes", {})
        sources = self.manifest.get("sources", {})

        node = next(
            (
                n
                for n in nodes.values()
                if n["name"] == node_name and n.get("resource_type") == "model"
            ),
            None,
        )
        if not node:
            return []

        upstreams = [
            (nodes[uid] if uid in nodes else sources[uid])["name"]
            for uid in node.get("depends_on", {}).get("nodes", [])
            if uid in nodes or uid in sources
        ]
        logger.info(f"Node '{node_name}' has upstream dependencies: {upstreams}")
        return upstreams

    def get_direct_downstream_nodes(self, node_name: str) -> list[str]:
        """Return the short names of all model nodes that directly depend on a node.

        Args:
            node_name: Short name of the node whose direct downstream nodes to resolve.

        Returns:
            List of downstream model node short names.
        """
        logger.info(f"Resolving direct downstream nodes for node '{node_name}'.")
        nodes = self.manifest.get("nodes", {})
        sources = self.manifest.get("sources", {})
        node_uids = {
            uid for uid, n in {**nodes, **sources}.items() if n.get("name") == node_name
        }
        downstreams = [
            n["name"]
            for n in nodes.values()
            if n.get("resource_type") == "model"
            and node_uids & set(n.get("depends_on", {}).get("nodes", []))
        ]
        logger.info(f"Node '{node_name}' has direct downstream nodes: {downstreams}")
        return downstreams

    def get_runnable_nodes(
        self, node_name: str, state: dict
    ) -> tuple[list[str], dict[str, list[str]]]:
        """Return the runnable direct downstream nodes of ``node_name``.

        A downstream node is runnable when all of its other upstream
        dependencies (i.e. every upstream except ``node_name`` itself) are
        fresh according to ``state``.

        Args:
            node_name: The source or model node that just completed.
            state: Full state dict loaded from the state store
                (mapping of node short name → node state).

        Returns:
            A ``(runnable, stale)`` tuple where ``runnable`` is a list of node
            names ready to run and ``stale`` maps each excluded node name to
            the list of its stale upstream names.
        """
        logger.info("Selecting runnable downstream nodes.")
        downstream_nodes = self.get_direct_downstream_nodes(node_name)
        runnable: list[str] = []
        stale: dict[str, list[str]] = {}
        for node in downstream_nodes:
            stale_upstreams = [
                u
                for u in self.get_upstreams(node)
                if u != node_name and not is_fresh(u, state)
            ]
            if stale_upstreams:
                stale[node] = stale_upstreams
            else:
                runnable.append(node)
        logger.info("Runnable downstream nodes selection finished.")
        return runnable, stale
