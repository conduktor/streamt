"""Extracted reference validators to keep validator.py under 1000 lines."""

from __future__ import annotations

from collections.abc import Callable

from streamt.core import errors
from streamt.core.models import StreamtProject


def validate_cluster_refs(
    project: StreamtProject, add_error: Callable[..., None]
) -> None:
    """Validate flink_cluster and connect_cluster references exist."""
    flink_clusters: set[str] = set()
    if project.runtime.flink and project.runtime.flink.clusters:
        flink_clusters = set(project.runtime.flink.clusters.keys())

    connect_clusters: set[str] = set()
    if project.runtime.connect and project.runtime.connect.clusters:
        connect_clusters = set(project.runtime.connect.clusters.keys())

    for model in project.models:
        fc = model.flink_cluster
        if fc and fc not in flink_clusters:
            add_error(
                "INVALID_CLUSTER_REF",
                errors.invalid_cluster_ref("Model", model.name, fc, "flink", sorted(flink_clusters)),
                f"model '{model.name}'",
            )

        cc = model.connect_cluster
        if cc and cc not in connect_clusters:
            add_error(
                "INVALID_CLUSTER_REF",
                errors.invalid_cluster_ref("Model", model.name, cc, "connect", sorted(connect_clusters)),
                f"model '{model.name}'",
            )

    for test in project.tests:
        fc = test.flink_cluster
        if fc and fc not in flink_clusters:
            add_error(
                "INVALID_CLUSTER_REF",
                errors.invalid_cluster_ref("Test", test.name, fc, "flink", sorted(flink_clusters)),
                f"test '{test.name}'",
            )

    # Validate defaults (cluster refs)
    if project.defaults:
        if project.defaults.models and project.defaults.models.cluster:
            dc = project.defaults.models.cluster
            if dc not in flink_clusters:
                add_error(
                    "INVALID_CLUSTER_REF",
                    errors.invalid_cluster_ref(
                        "Default", "defaults.models.cluster", dc, "flink", sorted(flink_clusters)
                    ),
                    "defaults.models.cluster",
                )
        if project.defaults.tests and project.defaults.tests.flink_cluster:
            dc = project.defaults.tests.flink_cluster
            if dc not in flink_clusters:
                add_error(
                    "INVALID_CLUSTER_REF",
                    errors.invalid_cluster_ref(
                        "Default", "defaults.tests.flink_cluster", dc, "flink", sorted(flink_clusters)
                    ),
                    "defaults.tests.flink_cluster",
                )


def validate_connection_refs(
    project: StreamtProject, add_error: Callable[..., None]
) -> None:
    """Validate sink connection references exist in project.connections."""
    available = set(project.connections.keys())

    for model in project.models:
        sink = model.sink
        if sink and sink.connection:
            if sink.connection not in available:
                add_error(
                    "INVALID_CONNECTION_REF",
                    errors.invalid_connection_ref(model.name, sink.connection, sorted(available)),
                    f"model '{model.name}'",
                )


def validate_key_columns(
    project: StreamtProject, add_error: Callable[..., None]
) -> None:
    """Validate key and primary_key reference declared columns."""
    for model in project.models:
        # Collect known column names from contract and columns
        known_columns: set[str] = set()
        if model.contract and model.contract.columns:
            known_columns.update(c.name for c in model.contract.columns)
        if model.columns:
            known_columns.update(c.name for c in model.columns)

        if not known_columns:
            continue  # No schema to validate against

        if model.key and model.key not in known_columns:
            add_error(
                "INVALID_KEY_COLUMN",
                errors.invalid_key_column(model.name, model.key, "key", sorted(known_columns)),
                f"model '{model.name}'",
            )

        if model.primary_key:
            for pk in model.primary_key:
                if pk not in known_columns:
                    add_error(
                        "INVALID_KEY_COLUMN",
                        errors.invalid_key_column(
                            model.name, pk, "primary_key", sorted(known_columns)
                        ),
                        f"model '{model.name}'",
                    )
