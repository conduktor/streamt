"""Pure conversion of planned runtime actions to durable operation actions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from streamt.deployer.kafka_streams_evidence import KafkaStreamsActionEvidence
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    GatewayActionEvidence,
    OperationAction,
)


class PlannedOperationAction(Protocol):
    """Minimal planner action surface needed by durable action consumers."""

    @property
    def resource_id(self) -> str: ...

    @property
    def action(self) -> str: ...

    @property
    def gateway_evidence(self) -> GatewayActionEvidence | None: ...

    @property
    def connector_evidence(self) -> ConnectorActionEvidence | None: ...

    @property
    def kafka_streams_evidence(self) -> KafkaStreamsActionEvidence | None: ...


def operation_actions_from_planned(
    planned_actions: Sequence[PlannedOperationAction],
) -> tuple[OperationAction, ...]:
    """Freeze one ordered planner result into its durable immutable form."""
    return tuple(
        OperationAction(
            index=index,
            resource_id=planned_action.resource_id,
            action=planned_action.action,
            gateway_evidence=planned_action.gateway_evidence,
            connector_evidence=planned_action.connector_evidence,
            kafka_streams_evidence=planned_action.kafka_streams_evidence,
        )
        for index, planned_action in enumerate(planned_actions)
    )
