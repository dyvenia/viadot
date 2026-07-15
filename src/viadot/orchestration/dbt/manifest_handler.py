"""Manifest traversal logic for dbt dynamic orchestration."""

import logging

from viadot.orchestration.dbt.state_handler import is_fresh


logger = logging.getLogger(__name__)

_REFERENCE_UPSTREAMS_META_KEY = "trigger_downstream_nodes_reference_upstreams"


class ManifestHandler:
    """Encapsulates traversal and freshness-gating logic over a dbt manifest.

    This class is a pure-Python layer with no Prefect dependency. It operates
    on a manifest dict that has already been loaded (e.g. by ``ArtifactStore``).

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

    def get_runnable_nodes_for_event(
        self,
        node_name: str,
        state: dict,
        event_id: str,
    ) -> tuple[list[str], dict[str, list[str]]]:
        """Return direct downstream nodes ready for one logical event.

        Event-bound upstreams must have immutable success evidence for the exact
        ``event_id``. Unlike freshness mode, this checks every direct upstream,
        including ``node_name``. A child can explicitly declare reference upstreams
        in ``meta.trigger_downstream_nodes_reference_upstreams``; those must have a
        current successful state and satisfy the existing freshness policy.

        Args:
            node_name: Source or model node whose completion caused the evaluation.
            state: Full state dict loaded from the state store.
            event_id: Opaque logical attempt identifier shared by cycle-bound nodes.

        Returns:
            A ``(runnable, blocked)`` tuple. ``blocked`` maps each excluded child to
            direct upstreams that have not satisfied its event barrier.

        Raises:
            ValueError: If ``event_id`` is blank or reference metadata is invalid.
        """
        if not isinstance(event_id, str) or not event_id.strip():
            msg = "event_id must be a non-empty string in event mode."
            raise ValueError(msg)

        logger.info("Selecting runnable downstream nodes for event '%s'.", event_id)
        downstream_nodes = self.get_direct_downstream_nodes(node_name)
        runnable: list[str] = []
        blocked: dict[str, list[str]] = {}

        for downstream_node in downstream_nodes:
            upstreams = self.get_upstreams(downstream_node)
            references = self.get_node_meta(downstream_node).get(
                _REFERENCE_UPSTREAMS_META_KEY, []
            )
            if references is None:
                references = []
            if not isinstance(references, list) or any(
                not isinstance(reference, str) for reference in references
            ):
                msg = (
                    f"Node '{downstream_node}' metadata "
                    f"'{_REFERENCE_UPSTREAMS_META_KEY}' must be a list of strings."
                )
                raise ValueError(msg)

            unknown_references = set(references) - set(upstreams)
            if unknown_references:
                msg = (
                    f"Node '{downstream_node}' declares non-upstream references: "
                    f"{sorted(unknown_references)}."
                )
                raise ValueError(msg)

            blocked_upstreams = []
            for upstream in upstreams:
                upstream_state = state.get(upstream, {})
                if upstream in references:
                    is_ready = upstream_state.get("status") == "success" and is_fresh(
                        upstream, state
                    )
                else:
                    event_successes = upstream_state.get("_event_successes", {})
                    is_ready = event_id in event_successes

                if not is_ready:
                    blocked_upstreams.append(upstream)

            if blocked_upstreams:
                blocked[downstream_node] = blocked_upstreams
            else:
                runnable.append(downstream_node)

        logger.info("Event-based runnable downstream node selection finished.")
        return runnable, blocked
