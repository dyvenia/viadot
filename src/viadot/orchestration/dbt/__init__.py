"""Functions for dynamic dbt model orchestration.

The functionality depends on four primary classes:
- ArtifactStore: A class for managing dbt artifact storage
- StateStore: A class for managing dbt node state storage
- ManifestHandler: A class for traversing a dbt manifest and applying freshness-gating
    logic
- StateHandler: A class for applying dbt freshness logic to node state and determining
    execution status
"""
