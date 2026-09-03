"""Early workflow authorization for explicit Connector removals."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path

from streamt.core.errors import ErrorCode
from streamt.deployer.state_backend import OperationAction
from streamt.output import OutputFormatter, StructuredError

CONNECTOR_REMOVAL_REVIEW_MESSAGE = "Connector removals require a complete online reviewed plan"
CONNECTOR_REMOVAL_REVIEW_SUGGESTION = (
    "Run streamt plan with --out, review the saved plan, then apply it with --plan."
)
CONNECTOR_REMOVAL_DRIFT_MESSAGE = (
    "Kafka Connect managed deletion could not prove exact absence"
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


def connector_removal_delete_count(actions: Sequence[OperationAction]) -> int:
    """Count exact managed Connector delete actions in one frozen action sequence."""
    return sum(
        1
        for action in actions
        if type(action) is OperationAction
        and action.action == "delete"
        and action.connector_evidence is not None
    )


def emit_connector_removal_destructive_warning(
    formatter: OutputFormatter,
    actions: Sequence[OperationAction],
) -> int:
    """Emit one aggregate, secret-neutral warning for actionable removals."""
    count = connector_removal_delete_count(actions)
    if count:
        formatter.print_warning(
            f"Planned Connector removal is destructive ({count} delete(s))",
            code=ErrorCode.CONNECTOR_REMOVAL_DESTRUCTIVE,
        )
    return count
