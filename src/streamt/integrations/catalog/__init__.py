"""Offline catalog integration primitives."""

from streamt.integrations.catalog.backstage_validation import (
    BACKSTAGE_CATALOG_MODEL_VERSION,
    BACKSTAGE_RELEASE,
    BackstageResourceError,
    BackstageValidationError,
    validate_backstage_entity,
)

__all__ = [
    "BACKSTAGE_CATALOG_MODEL_VERSION",
    "BACKSTAGE_RELEASE",
    "BackstageResourceError",
    "BackstageValidationError",
    "validate_backstage_entity",
]
