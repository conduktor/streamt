"""Offline catalog integration primitives."""

from streamt.integrations.catalog.backstage_validation import (
    BACKSTAGE_CATALOG_MODEL_VERSION,
    BACKSTAGE_RELEASE,
    BackstageResourceError,
    BackstageValidationError,
    validate_backstage_entity,
)
from streamt.integrations.catalog.model import (
    CatalogContractSummary,
    CatalogDataset,
    CatalogDependency,
    CatalogOwnerLabel,
    CatalogProcess,
    CatalogProjectionError,
    CatalogSnapshot,
    build_catalog_snapshot,
)

__all__ = [
    "BACKSTAGE_CATALOG_MODEL_VERSION",
    "BACKSTAGE_RELEASE",
    "BackstageResourceError",
    "BackstageValidationError",
    "CatalogContractSummary",
    "CatalogDataset",
    "CatalogDependency",
    "CatalogOwnerLabel",
    "CatalogProcess",
    "CatalogProjectionError",
    "CatalogSnapshot",
    "build_catalog_snapshot",
    "validate_backstage_entity",
]
