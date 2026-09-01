"""Focused behavior tests for extracted governance rule validators."""

from __future__ import annotations

from streamt.core.models import (
    Classification,
    ColumnDefinition,
    Model,
    ProjectInfo,
    Rules,
    SecurityPolicies,
    SecurityRules,
    Source,
    StreamtProject,
)
from streamt.core.runtime import KafkaConfig, RuntimeConfig
from streamt.core.validator import ProjectValidator


def _security_project(*, model: Model | None = None) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="security-rules"),
        runtime=RuntimeConfig(kafka=KafkaConfig(bootstrap_servers="localhost:9092")),
        rules=Rules(
            security=SecurityRules(sensitive_columns_require_masking=True),
        ),
        sources=[
            Source(
                name="customers",
                topic="customers.v1",
                columns=[
                    ColumnDefinition(
                        name="email",
                        type="STRING",
                        classification=Classification.SENSITIVE,
                    )
                ],
            )
        ],
        models=[model] if model else [],
    )


def test_sensitive_column_without_masking_policy_is_rejected() -> None:
    result = ProjectValidator(_security_project()).validate()

    errors = [
        error for error in result.errors if error.code == "RULE_SENSITIVE_REQUIRES_MASKING"
    ]
    assert len(errors) == 1
    assert "email" in errors[0].message
    assert "customers" in errors[0].message


def test_sensitive_column_with_matching_masking_policy_is_allowed() -> None:
    model = Model(
        name="customers_masked",
        sql="SELECT 1",
        security=SecurityPolicies(
            policies=[{"mask": {"column": "email", "method": "hash"}}]
        ),
    )

    result = ProjectValidator(_security_project(model=model)).validate()

    assert not any(
        error.code == "RULE_SENSITIVE_REQUIRES_MASKING" for error in result.errors
    )


def test_non_mapping_mask_payload_does_not_crash_governance_validation() -> None:
    model = Model(name="unsafe_constructed_model", sql="SELECT 1")
    model.security = SecurityPolicies.model_construct(
        classification={},
        policies=[{"mask": True}],
    )

    result = ProjectValidator(_security_project(model=model)).validate()

    assert any(
        error.code == "RULE_SENSITIVE_REQUIRES_MASKING" for error in result.errors
    )
