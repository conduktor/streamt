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
