"""Extracted governance rule validators to keep validator.py under 1000 lines."""

from __future__ import annotations

from collections.abc import Callable

from streamt.core.models import Classification, SecurityRules, StreamtProject

AddError = Callable[[str, str], None]


def _masks_column(policy: dict[str, object], column_name: str) -> bool:
    """Return whether a safely narrowed masking policy targets a column."""
    mask = policy.get("mask")
    return isinstance(mask, dict) and mask.get("column") == column_name


def validate_security_rules(
    project: StreamtProject, rules: SecurityRules, add_error: AddError
) -> None:
    """Validate security rules: classification and masking requirements."""
    sensitive_columns: dict[str, list[str]] = {}

    for source in project.sources:
        for column in source.columns:
            if column.classification in (
                Classification.SENSITIVE,
                Classification.HIGHLY_SENSITIVE,
            ):
                sensitive_columns.setdefault(source.name, []).append(column.name)

    for model in project.models:
        if model.security and model.security.classification:
            for column_name, classification in model.security.classification.items():
                if classification in (
                    Classification.SENSITIVE,
                    Classification.HIGHLY_SENSITIVE,
                ):
                    sensitive_columns.setdefault(model.name, []).append(column_name)

    if rules.require_classification:
        for source in project.sources:
            for column in source.columns:
                if column.classification is None:
                    add_error(
                        "RULE_REQUIRE_CLASSIFICATION",
                        f"Column '{column.name}' in source '{source.name}' "
                        f"missing required classification",
                    )

    if rules.sensitive_columns_require_masking:
        for entity_name, columns in sensitive_columns.items():
            for column_name in columns:
                has_masking = any(
                    _masks_column(policy, column_name)
                    for model in project.models
                    if model.security and model.security.policies
                    for policy in model.security.policies
                )
                if not has_masking:
                    add_error(
                        "RULE_SENSITIVE_REQUIRES_MASKING",
                        f"Column '{column_name}' in '{entity_name}' classified as sensitive "
                        f"has no masking policy",
                    )


def validate_data_residency(
    project: StreamtProject, allowed_regions: list[str], add_error: AddError
) -> None:
    """Validate data residency constraints on models."""
    for model in project.models:
        if model.region and allowed_regions and model.region not in allowed_regions:
            add_error(
                "RULE_DATA_RESIDENCY",
                f"Model '{model.name}' region '{model.region}' not in allowed regions: "
                f"{', '.join(allowed_regions)}",
            )
    for source in project.sources:
        if source.region and allowed_regions and source.region not in allowed_regions:
            add_error(
                "RULE_DATA_RESIDENCY",
                f"Source '{source.name}' region '{source.region}' not in allowed regions: "
                f"{', '.join(allowed_regions)}",
            )
