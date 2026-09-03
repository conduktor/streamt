"""Early workflow authorization for explicit Connector removals."""

from __future__ import annotations

import sys
from pathlib import Path

from streamt.core.errors import ErrorCode
from streamt.output import OutputFormatter, StructuredError

CONNECTOR_REMOVAL_REVIEW_MESSAGE = "Connector removals require a complete online reviewed plan"
CONNECTOR_REMOVAL_REVIEW_SUGGESTION = (
    "Run streamt plan with --out, review the saved plan, then apply it with --plan."
)


def _connector_removals_requested(raw_connector_removals: object) -> bool:
    return type(raw_connector_removals) is not list or bool(raw_connector_removals)


def _fail_reviewed_workflow(
    raw_connector_removals: object,
    formatter: OutputFormatter,
) -> None:
    formatter.set_data(
        {
            "policy": "connector_removal",
            "required_workflow": "reviewed_plan",
            "connector_removals": (
                len(raw_connector_removals) if isinstance(raw_connector_removals, list) else None
            ),
        }
    )
    formatter.add_error(
        StructuredError(
            code=ErrorCode.REVIEWED_PLAN_REQUIRED,
            message=CONNECTOR_REMOVAL_REVIEW_MESSAGE,
            suggestion=CONNECTOR_REMOVAL_REVIEW_SUGGESTION,
        )
    )
    formatter.print_error(CONNECTOR_REMOVAL_REVIEW_MESSAGE)
    formatter.flush()
    sys.exit(1)


def enforce_connector_removal_plan_authorization(
    raw_connector_removals: object,
    formatter: OutputFormatter,
    *,
    offline: bool,
    plan_output: Path | None,
) -> None:
    """Require an online plan with an atomic output target for every removal."""
    if not _connector_removals_requested(raw_connector_removals):
        return
    if offline or plan_output is None:
        _fail_reviewed_workflow(raw_connector_removals, formatter)


def enforce_connector_removal_apply_authorization(
    raw_connector_removals: object,
    formatter: OutputFormatter,
    *,
    reviewed_plan_path: Path | None,
    dry_run: bool,
    target: str | None,
    select: str | None,
) -> None:
    """Reject direct, dry-run, and partial Connector removal apply workflows."""
    if not _connector_removals_requested(raw_connector_removals):
        return
    if reviewed_plan_path is None or dry_run or target is not None or select is not None:
        _fail_reviewed_workflow(raw_connector_removals, formatter)
