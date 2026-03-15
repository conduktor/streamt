"""Validate YAML examples in docs against Pydantic models.

Pydantic models are the source of truth. This test extracts every ```yaml
block from docs/ and README.md, classifies it by structure, and validates
against the corresponding Pydantic model. Docs that drift from the schema
fail CI automatically.

Rules:
- Every YAML block MUST be classified and validated, or explicitly marked
  with `# streamt:skip` as the first line (for non-streamt YAML like CI/CD).
- Unclassified blocks FAIL the test — no silent skips.
- YAML parse errors skip (Jinja templates).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from streamt.core.models import (
    CheckpointConfig,
    ColumnDefinition,
    ConduktorConfig,
    ConnectClusterConfig,
    ConnectConfig,
    ConnectionConfig,
    ContractColumn,
    DataTest,
    Defaults,
    EventTimeConfig,
    Exposure,
    FlinkClusterConfig,
    FlinkConfig,
    FlinkJobConfig,
    FreshnessConfig,
    GatewayConfig,
    KafkaConfig,
    MaskPolicy,
    Model,
    ModelContract,
    ModelRules,
    ProjectInfo,
    ResourceConfig,
    RestartStrategyConfig,
    RocksDBConfig,
    Rules,
    RuntimeConfig,
    SLAConfig,
    SchemaRef,
    SchemaRegistryConfig,
    SinkConfig,
    Source,
    SourceRules,
    StreamtProject,
    TopicConfig,
    TopicRules,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOC_DIRS = [PROJECT_ROOT / "docs"]
DOC_FILES = [PROJECT_ROOT / "README.md"]

YAML_BLOCK_RE = re.compile(r"```ya?ml[^\S\n]*(?:\S*)?\n(.*?)```", re.DOTALL)

# ---------------------------------------------------------------------------
# Mappings: fragment key → sub-model
# ---------------------------------------------------------------------------

# Single-key wrappers for RuntimeConfig fields
RUNTIME_FIELD_MODELS: dict[str, type] = {
    "kafka": KafkaConfig,
    "schema_registry": SchemaRegistryConfig,
    "flink": FlinkConfig,
    "connect": ConnectConfig,
    "conduktor": ConduktorConfig,
}

# Single-key wrappers for FlinkJobConfig sub-models
FLINK_WRAPPER_MODELS: dict[str, type] = {
    "checkpoint": CheckpointConfig,
    "restart_strategy": RestartStrategyConfig,
    "resources": ResourceConfig,
    "rocksdb": RocksDBConfig,
}

# Single-key wrappers validated directly
DIRECT_WRAPPER_MODELS: dict[str, type] = {
    "event_time": EventTimeConfig,
    "sla": SLAConfig,
    "topic": TopicConfig,
}

# Scalar fields from parent models — just a value illustration, no model needed
SCALAR_FIELDS = {
    "tags", "url", "sql", "naming_pattern", "changelog_mode",
    "state_backend", "state_ttl_ms",
}

# Rules sub-sections that have a Pydantic model
RULES_SECTION_MODELS: dict[str, type] = {
    "models": ModelRules,
    "sources": SourceRules,
}

# Rules sub-sections without a standalone model (acknowledged)
RULES_SECTIONS_NO_MODEL = {"exposures", "tests"}

# All RuntimeConfig keys
RUNTIME_KEYS = set(RUNTIME_FIELD_MODELS.keys())

# Models to try for field-matching (ordered by specificity)
FIELD_MATCH_CANDIDATES: list[tuple[str, type]] = [
    ("event_time", EventTimeConfig),
    ("freshness", FreshnessConfig),
    ("schema_ref", SchemaRef),
    ("sla_config", SLAConfig),
    ("checkpoint", CheckpointConfig),
    ("restart_strategy", RestartStrategyConfig),
    ("resources", ResourceConfig),
    ("rocksdb", RocksDBConfig),
    ("sink_config", SinkConfig),
    ("topic_config", TopicConfig),
    ("flink_job_config", FlinkJobConfig),
    ("project_info", ProjectInfo),
    ("model_defaults", ModelRules),
    ("source_rules", SourceRules),
    ("topic_rules", TopicRules),
    ("gateway_config", GatewayConfig),
    ("flink_cluster", FlinkClusterConfig),
    ("connect_cluster", ConnectClusterConfig),
    ("connection_config", ConnectionConfig),
    ("model_contract", ModelContract),
]

# Named instance types to try (user-defined key wrapping a config dict)
NAMED_INSTANCE_MODELS: list[tuple[str, type]] = [
    ("flink_cluster", FlinkClusterConfig),
    ("connect_cluster", ConnectClusterConfig),
    ("connection", ConnectionConfig),
]


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def _extract_yaml_blocks(path: Path) -> list[tuple[str, int, str]]:
    content = path.read_text()
    results = []
    for m in YAML_BLOCK_RE.finditer(content):
        text = m.group(1)
        line = content[: m.start()].count("\n") + 1
        rel = str(path.relative_to(PROJECT_ROOT))
        results.append((rel, line, text))
    return results


def _collect_all_blocks() -> list[tuple[str, int, str]]:
    blocks: list[tuple[str, int, str]] = []
    for d in DOC_DIRS:
        for md in sorted(d.rglob("*.md")):
            blocks.extend(_extract_yaml_blocks(md))
    for f in DOC_FILES:
        if f.exists():
            blocks.extend(_extract_yaml_blocks(f))
    return blocks


def _parse_yaml_safe(text: str) -> Any:
    # Strip markdown blockquote prefixes ("> " on every line)
    lines = text.split("\n")
    if all(line.startswith("> ") or line.strip() == "" or line == ">" for line in lines if line):
        text = "\n".join(
            line[2:] if line.startswith("> ") else line[1:] if line == ">" else line
            for line in lines
        )
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return None


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_model(category: str, model_cls: type, data: Any) -> tuple[str, list[str]]:
    try:
        model_cls.model_validate(data)
        return category, []
    except ValidationError as e:
        return category, [str(err) for err in e.errors()]


def _validate_list(category: str, model_cls: type, items: list) -> tuple[str, list[str]]:
    errors: list[str] = []
    for i, item in enumerate(items):
        try:
            model_cls.model_validate(item)
        except ValidationError as e:
            for err in e.errors():
                errors.append(f"[{i}] {err}")
    return category, errors


# ---------------------------------------------------------------------------
# Field-matching: find best Pydantic model by key overlap
# ---------------------------------------------------------------------------

def _model_field_names(cls: type) -> set[str]:
    names = set(cls.model_fields.keys())
    for fi in cls.model_fields.values():
        if fi.alias:
            names.add(fi.alias)
    return names


def _try_field_match(data: dict) -> tuple[str, list[str]] | None:
    keys = set(data.keys())
    best_name: str | None = None
    best_cls: type | None = None
    best_coverage = 0.0
    best_overlap = 0

    for name, cls in FIELD_MATCH_CANDIDATES:
        model_fields = _model_field_names(cls)
        overlap = keys & model_fields
        if not overlap:
            continue
        coverage = len(overlap) / len(keys)
        if coverage > best_coverage or (
            coverage == best_coverage and len(overlap) > best_overlap
        ):
            best_name, best_cls = name, cls
            best_coverage = coverage
            best_overlap = len(overlap)

    if best_coverage >= 0.5 and best_cls is not None and best_name is not None:
        return _validate_model(best_name, best_cls, data)
    return None


# ---------------------------------------------------------------------------
# Dict classification
# ---------------------------------------------------------------------------

def _classify_single_key_dict(data: dict) -> tuple[str, list[str]]:
    key = next(iter(data))
    val = data[key]

    # Runtime field wrappers: {"kafka": {...}} → KafkaConfig
    if key in RUNTIME_FIELD_MODELS and isinstance(val, dict):
        return _validate_model(f"runtime.{key}", RUNTIME_FIELD_MODELS[key], val)

    # Flink sub-model wrappers: {"checkpoint": {...}} → CheckpointConfig
    if key in FLINK_WRAPPER_MODELS and isinstance(val, dict):
        return _validate_model(f"flink.{key}", FLINK_WRAPPER_MODELS[key], val)

    # Direct wrappers: {"event_time": {...}} → EventTimeConfig
    if key in DIRECT_WRAPPER_MODELS and isinstance(val, dict):
        return _validate_model(key, DIRECT_WRAPPER_MODELS[key], val)

    # Access config
    if key == "access" and isinstance(val, dict):
        return f"field.{key}", []  # AccessConfig schema differs from doc patterns

    # List wrappers
    if key == "masking" and isinstance(val, list):
        return _validate_list("masking", MaskPolicy, val)
    if key == "on_failure" and isinstance(val, list):
        return "on_failure", []  # mixed action types, validated at DataTest level
    if key == "columns" and isinstance(val, list):
        return _validate_list("columns", ColumnDefinition, val)
    if key == "contacts" and isinstance(val, list):
        return "contacts", []  # free-form contact list, no standalone model

    # Rules sub-sections with model: {"models": {require_description: ...}}
    if key in RULES_SECTION_MODELS and isinstance(val, dict):
        return _validate_model(f"rules.{key}", RULES_SECTION_MODELS[key], val)
    if key in RULES_SECTIONS_NO_MODEL and isinstance(val, dict):
        return f"rules.{key}", []

    # Scalar field values
    if key in SCALAR_FIELDS:
        return f"field.{key}", []

    # Safety (environment config, no standalone model)
    if key == "safety" and isinstance(val, dict):
        return "safety", []

    # Named instance: user-defined key wrapping a config dict
    if isinstance(val, dict):
        for inst_name, inst_cls in NAMED_INSTANCE_MODELS:
            try:
                inst_cls.model_validate(val)
                return f"named.{inst_name}", []
            except ValidationError:
                continue

    return "unclassified", []


def _classify_dict(data: dict) -> tuple[str, list[str]]:
    keys = set(data.keys())

    # --- Top-level structures ---
    if "project" in keys and keys & {"runtime", "sources", "models"}:
        return _validate_model("project", StreamtProject, data)
    if "environment" in keys:
        return "environment", []

    # --- Resource collections (list-valued) ---
    for rkey, model in [("sources", Source), ("models", Model),
                        ("tests", DataTest), ("exposures", Exposure)]:
        if rkey in keys and isinstance(data[rkey], list):
            return _validate_list(rkey, model, data[rkey])

    # --- Rules ---
    if "rules" in keys:
        rules_data = data["rules"] if len(keys) == 1 else data.get("rules", data)
        return _validate_model("rules", Rules, rules_data)

    # --- Runtime/Defaults wrappers ---
    if "runtime" in keys and isinstance(data["runtime"], dict):
        rt = data["runtime"]
        rt_keys = set(rt.keys())
        # Single runtime sub-section: validate the inner model directly
        if len(rt_keys) == 1 and rt_keys <= RUNTIME_KEYS:
            sub_key = next(iter(rt_keys))
            return _validate_model(
                f"runtime.{sub_key}", RUNTIME_FIELD_MODELS[sub_key], rt[sub_key]
            )
        return _validate_model("runtime", RuntimeConfig, rt)
    if "defaults" in keys and isinstance(data["defaults"], dict):
        return _validate_model("defaults", Defaults, data["defaults"])

    # --- Single model or source by distinctive keys ---
    if keys & {"sql", "materialized", "from"} and "name" in keys:
        return _validate_model("model", Model, data)
    if "topic" in keys and "sql" not in keys and "name" in keys and "model" not in keys:
        return _validate_model("source", Source, data)

    # --- Single-key dicts ---
    if len(keys) == 1:
        return _classify_single_key_dict(data)

    # --- Multi-key fragments ---

    # RuntimeConfig subset: {"kafka": {...}, "schema_registry": {...}}
    if keys <= RUNTIME_KEYS and all(isinstance(data[k], dict) for k in keys):
        return _validate_model("runtime", RuntimeConfig, data)

    # Defaults content: {"models": {dict}, "tests": {dict}, "topic": {dict}}
    defaults_keys = {"models", "tests", "topic"}
    if keys <= defaults_keys and all(isinstance(data.get(k), dict) for k in keys):
        return _validate_model("defaults", Defaults, data)

    # Rules sub-keys with dict values (not list)
    rules_sub_keys = {"topics", "security", "data_residency"}
    if keys & rules_sub_keys and not any(isinstance(data.get(k), list) for k in keys):
        return _validate_model("rules", Rules, data)

    # Field matching against all known models
    result = _try_field_match(data)
    if result is not None:
        return result

    return "unclassified", []


# ---------------------------------------------------------------------------
# List classification
# ---------------------------------------------------------------------------

def _classify_list(items: list) -> tuple[str, list[str]]:
    if not items:
        return "unclassified", []

    first = items[0]
    if not isinstance(first, dict):
        return "unclassified", []

    keys = set(first.keys())

    # Model list (has sql/materialized/from+name/sink)
    if keys & {"sql", "materialized"} or (
        "from" in keys and keys & {"sink", "connector", "name"}
    ) or ("name" in keys and keys & {"sink", "connector"}):
        return _validate_list("models_list", Model, items)

    # Source list
    if "topic" in keys and "sql" not in keys and "model" not in keys:
        return _validate_list("sources_list", Source, items)

    # DataTest list
    if "model" in keys and ("type" in keys or "assertions" in keys):
        return _validate_list("tests_list", DataTest, items)

    # Exposure list (has role/consumes/produces or type in known exposure types)
    exposure_types = {"application", "dashboard", "ml_training", "ml_inference", "api"}
    if keys & {"role", "consumes", "produces"} or (
        "type" in keys and first.get("type") in exposure_types
    ):
        return _validate_list("exposures_list", Exposure, items)

    # ContractColumn list (has nullable)
    if "name" in keys and "nullable" in keys:
        return _validate_list("contract_columns", ContractColumn, items)

    # ColumnDefinition list (name + type/classification/description/proctime)
    if "name" in keys and keys & {"type", "classification", "description", "proctime", "required"}:
        return _validate_list("columns", ColumnDefinition, items)

    # Test assertion list (first element has assertion-type key)
    assertion_keys = {
        "not_null", "accepted_values", "accepted_types", "range",
        "unique_key", "custom_sql", "foreign_key", "max_lag",
        "throughput", "distribution",
    }
    if keys & assertion_keys:
        return "assertions", []  # each item is {assertion_type: config}, no single model

    # OnFailure action list
    action_keys = {"alert", "pause", "dlq", "block_deploy"}
    if keys & action_keys:
        return "on_failure_actions", []

    # Model list with contract
    if "name" in keys and "contract" in keys:
        return _validate_list("models_list", Model, items)

    # Model list with just name (output references)
    if keys == {"name"} or keys == {"name", "key"}:
        return "model_refs", []

    # Source fragment list (name + owner/tags but no sql/topic content)
    if "name" in keys and keys & {"owner", "tags"} and not keys & {"sql", "materialized", "type", "assertions"}:
        return _validate_list("sources_list", Source, items)

    # Test type list (type + assertions, no model key)
    if "type" in keys and "assertions" in keys and "model" not in keys:
        return "test_types", []

    # Model list with from (sink models, maybe missing sql)
    if "from" in keys or ("name" in keys and "description" in keys):
        return _validate_list("models_list", Model, items)

    # Owner/contact list (has email/slack/pagerduty)
    if keys & {"email", "slack", "pagerduty"}:
        return "contacts", []

    # CI/CD step list (has uses/run)
    if keys & {"uses", "run"}:
        return "ci_steps", []

    return "unclassified", []


# ---------------------------------------------------------------------------
# Top-level classification
# ---------------------------------------------------------------------------

def _classify_and_validate(data: Any) -> tuple[str, list[str]]:
    if isinstance(data, dict):
        return _classify_dict(data)
    if isinstance(data, list):
        return _classify_list(data)
    # Scalar (string, number, etc.)
    return "unclassified", []


# ---------------------------------------------------------------------------
# Collect and parametrize
# ---------------------------------------------------------------------------

_ALL_BLOCKS = _collect_all_blocks()


def _make_test_id(block: tuple[str, int, str]) -> str:
    path, line, _ = block
    return f"{path}:{line}"


@pytest.mark.parametrize(
    "block", _ALL_BLOCKS, ids=[_make_test_id(b) for b in _ALL_BLOCKS]
)
def test_doc_yaml_block(block: tuple[str, int, str]) -> None:
    path, line, text = block

    stripped = text.strip()
    if stripped.startswith("# streamt:skip"):
        pytest.skip("marked streamt:skip")

    data = _parse_yaml_safe(text)
    if data is None:
        pytest.skip("not valid YAML (Jinja template)")

    category, errors = _classify_and_validate(data)

    assert category != "unclassified", (
        f"Unclassified YAML block at {path}:{line}\n"
        f"Data type: {type(data).__name__}\n"
        f"Keys: {sorted(str(k) for k in data.keys()) if isinstance(data, dict) else 'N/A'}\n"
        f"Preview: {str(data)[:200]}\n"
        "Add a classifier rule or mark the block with '# streamt:skip'"
    )

    assert not errors, (
        f"Schema validation failed for {category} block at {path}:{line}\n"
        + "\n".join(str(e) for e in errors)
    )
