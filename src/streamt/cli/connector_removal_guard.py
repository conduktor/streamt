"""Temporary fail-closed CLI boundary for unimplemented Connector removals."""

from __future__ import annotations

import sys

from streamt.core.errors import ErrorCode
from streamt.output import OutputFormatter, StructuredError

CONNECTOR_REMOVAL_UNAVAILABLE_MESSAGE = (
    "Connector removals are not supported by plan or apply in this release"
)
CONNECTOR_REMOVAL_UNAVAILABLE_DATA: dict[str, object] = {
    "policy": "connector_removal_unavailable",
    "status": "unsupported",
}


def enforce_connector_removals_unavailable(
    raw_connector_removals: object,
    formatter: OutputFormatter,
) -> None:
    """Reject every non-empty or malformed collection until preflight lands.

    This Slice 0/1 guard is intentionally replaceable by the authoritative
    provider-free preflight and reviewed-plan authorization in later slices.
    It must not inspect or echo any tombstone content.
    """
    if type(raw_connector_removals) is list and not raw_connector_removals:
        return

    formatter.set_data(dict(CONNECTOR_REMOVAL_UNAVAILABLE_DATA))
    formatter.add_error(
        StructuredError(
            code=ErrorCode.REVIEWED_PLAN_REQUIRED,
            message=CONNECTOR_REMOVAL_UNAVAILABLE_MESSAGE,
            suggestion=(
                "Remove lifecycle.connector_removals before running plan or apply."
            ),
        )
    )
    formatter.print_error(CONNECTOR_REMOVAL_UNAVAILABLE_MESSAGE)
    formatter.flush()
    sys.exit(1)
