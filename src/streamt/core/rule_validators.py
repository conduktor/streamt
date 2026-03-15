"""Extracted governance rule validators to keep validator.py under 1000 lines."""

from __future__ import annotations

from streamt.core.models import Classification, SecurityRules, StreamtProject


def validate_security_rules(
    project: StreamtProject, rules: SecurityRules, add_error: callable
) -> None:
    """Validate security rules: classification and masking requirements."""
    sensitive_columns: dict[str, list[str]] = {}

    for source in project.sources:
        for col in source.columns:
            if col.classification in (Classification.SENSITIVE, Classification.HIGHLY_SENSITIVE):
                sensitive_columns.setdefault(source.name, []).append(col.name)

    for model in project.models:
        if model.security and model.security.classification:
            for col, cls in model.security.classification.items():
                if cls in (Classification.SENSITIVE, Classification.HIGHLY_SENSITIVE):
                    sensitive_columns.setdefault(model.name, []).append(col)

    if rules.require_classification:
        for source in project.sources:
            for col in source.columns:
                if col.classification is None:
                    add_error(
                        "RULE_REQUIRE_CLASSIFICATION",
                        f"Column '{col.name}' in source '{source.name}' "
                        f"missing required classification",
                    )

    if rules.sensitive_columns_require_masking:
        for entity_name, columns in sensitive_columns.items():
            for col in columns:
                has_masking = any(
                    "mask" in p and p["mask"].get("column") == col
                    for m in project.models
                    if m.security and m.security.policies
                    for p in m.security.policies
                )
                if not has_masking:
                    add_error(
                        "RULE_SENSITIVE_REQUIRES_MASKING",
                        f"Column '{col}' in '{entity_name}' classified as sensitive "
                        f"has no masking policy",
                    )


def validate_data_residency(
    project: StreamtProject, allowed_regions: list[str], add_error: callable
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
