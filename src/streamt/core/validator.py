"""Validator for streamt projects."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from streamt.core import errors
from streamt.core.models import (
    Classification,
    MaterializedType,
    Model,
    ModelRules,
    SecurityRules,
    Source,
    SourceRules,
    StreamtProject,
    TopicRules,
)
from streamt.core.parser import ProjectParser


class ValidationLevel(str, Enum):
    """Validation message level."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationMessage:
    """A validation message."""

    level: ValidationLevel
    code: str
    message: str
    location: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of validation."""

    messages: list[ValidationMessage] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return not any(m.level == ValidationLevel.ERROR for m in self.messages)

    @property
    def errors(self) -> list[ValidationMessage]:
        """Get error messages."""
        return [m for m in self.messages if m.level == ValidationLevel.ERROR]

    @property
    def warnings(self) -> list[ValidationMessage]:
        """Get warning messages."""
        return [m for m in self.messages if m.level == ValidationLevel.WARNING]

    @property
    def infos(self) -> list[ValidationMessage]:
        """Get info messages."""
        return [m for m in self.messages if m.level == ValidationLevel.INFO]

    def add_error(self, code: str, message: str, location: Optional[str] = None) -> None:
        """Add an error message."""
        self.messages.append(ValidationMessage(ValidationLevel.ERROR, code, message, location))

    def add_warning(self, code: str, message: str, location: Optional[str] = None) -> None:
        """Add a warning message."""
        self.messages.append(ValidationMessage(ValidationLevel.WARNING, code, message, location))

    def add_info(self, code: str, message: str, location: Optional[str] = None) -> None:
        """Add an info message."""
        self.messages.append(ValidationMessage(ValidationLevel.INFO, code, message, location))


class ProjectValidator:
    """Validator for streamt projects."""

    def __init__(self, project: StreamtProject) -> None:
        """Initialize validator with project."""
        self.project = project
        self.parser = ProjectParser(project.project_path) if project.project_path else None
        self.result = ValidationResult()

        # Build lookup maps
        self.source_names = {s.name for s in project.sources}
        self.model_names = {m.name for m in project.models}
        self.test_names = {t.name for t in project.tests}
        self.exposure_names = {e.name for e in project.exposures}

    def validate(self) -> ValidationResult:
        """Run all validations."""
        self._validate_duplicates()
        self._validate_sources()
        self._validate_models()
        self._validate_tests()
        self._validate_exposures()
        self._validate_dag()
        self._validate_rules()
        return self.result

    def _validate_duplicates(self) -> None:
        """Check for duplicate names."""
        # Check source duplicates
        seen_sources: set[str] = set()
        for source in self.project.sources:
            if source.name in seen_sources:
                self.result.add_error(
                    "DUPLICATE_SOURCE",
                    errors.duplicate_name("source", source.name),
                )
            seen_sources.add(source.name)

        # Check model duplicates
        seen_models: set[str] = set()
        for model in self.project.models:
            if model.name in seen_models:
                self.result.add_error(
                    "DUPLICATE_MODEL",
                    errors.duplicate_name("model", model.name),
                )
            seen_models.add(model.name)

        # Check test duplicates
        seen_tests: set[str] = set()
        for test in self.project.tests:
            if test.name in seen_tests:
                self.result.add_error(
                    "DUPLICATE_TEST",
                    errors.duplicate_name("test", test.name),
                )
            seen_tests.add(test.name)

        # Check exposure duplicates
        seen_exposures: set[str] = set()
        for exposure in self.project.exposures:
            if exposure.name in seen_exposures:
                self.result.add_error(
                    "DUPLICATE_EXPOSURE",
                    errors.duplicate_name("exposure", exposure.name),
                )
            seen_exposures.add(exposure.name)

    def _validate_sources(self) -> None:
        """Validate source declarations."""
        for source in self.project.sources:
            # Topic is required (already handled by Pydantic)
            if not source.topic:
                self.result.add_error(
                    "MISSING_TOPIC",
                    f"Source '{source.name}' missing required field 'topic'",
                )

    def _validate_models(self) -> None:
        """Validate model declarations."""
        for model in self.project.models:
            self._validate_model(model)

    def _validate_model(self, model: Model) -> None:
        """Validate a single model."""
        # Check SQL has FROM clause (required for streaming)
        if model.sql and not re.search(r'\bFROM\b', model.sql, re.IGNORECASE):
            self.result.add_error(
                "SQL_NO_FROM",
                f"Model '{model.name}' SQL has no FROM clause. "
                "Streaming models must read from at least one table or source.",
                f"model '{model.name}'",
            )

        # Check virtual_topic gateway availability
        if model.get_materialized() == MaterializedType.VIRTUAL_TOPIC:
            has_gateway = self.project.runtime.conduktor and self.project.runtime.conduktor.gateway
            is_explicit_virtual_topic = model.gateway and model.gateway.virtual_topic

            if not has_gateway:
                if is_explicit_virtual_topic:
                    # User explicitly configured virtual_topic but Gateway not available → error
                    self.result.add_error(
                        "GATEWAY_REQUIRED",
                        errors.gateway_required(model.name),
                    )
                else:
                    # Auto-detected stateless SQL but Gateway not available
                    has_flink = self.project.runtime.flink is not None

                    if has_flink:
                        # Gateway not available but Flink is → warn about ambiguity, will use Flink
                        self.result.add_warning(
                            "AMBIGUOUS_MATERIALIZATION",
                            f"Model '{model.name}' has stateless SQL that could use either Gateway or Flink. "
                            f"No Gateway is configured, so Flink will be used. "
                            f"Consider specifying explicitly:\n"
                            f"  - materialized: virtual_topic  # Uses Gateway (faster, no Flink job)\n"
                            f"  - materialized: flink          # Uses Flink SQL job",
                            f"model '{model.name}'",
                        )
                    else:
                        # Neither Gateway nor Flink available → error
                        self.result.add_error(
                            "NO_PROCESSING_RUNTIME",
                            f"Model '{model.name}' has SQL transformations but no processing runtime is configured. "
                            f"Configure either:\n"
                            f"  - conduktor.gateway: For stateless transformations (WHERE, projections)\n"
                            f"  - flink: For all SQL transformations including stateful operations",
                            f"model '{model.name}'",
                        )

        # Check sink requires sink config
        if model.get_materialized() == MaterializedType.SINK:
            if not model.get_sink_config():
                self.result.add_error(
                    "MISSING_SINK_CONFIG",
                    errors.missing_sink_config(model.name),
                )

        # Check flink requires flink runtime
        if model.get_materialized() == MaterializedType.FLINK:
            if not self.project.runtime.flink:
                self.result.add_error(
                    "FLINK_REQUIRED",
                    errors.flink_required(model.name),
                )

        # Validate state_ttl_ms
        if model.get_flink_config() and model.get_flink_config().state_ttl_ms is not None:
            ttl = model.get_flink_config().state_ttl_ms
            if ttl <= 0:
                self.result.add_error(
                    "INVALID_STATE_TTL",
                    errors.invalid_state_ttl(model.name, ttl, "must be a positive number"),
                )
            elif ttl < 1000:
                self.result.add_warning(
                    "STATE_TTL_TOO_SHORT",
                    f"State TTL of {ttl}ms for model '{model.name}' is very short. "
                    "This may cause premature state expiration and incorrect results.",
                    f"model '{model.name}'",
                )

        # Validate SQL and extract dependencies
        if model.sql and self.parser:
            # Validate Jinja syntax
            is_valid, error = self.parser.validate_jinja_sql(model.sql)
            if not is_valid:
                self.result.add_error(
                    "JINJA_SYNTAX_ERROR",
                    errors.jinja_syntax_error(model.name, error, model.sql),
                )
                return

            # Extract and validate references
            sources, refs = self.parser.extract_refs_from_sql(model.sql)

            for source_name in sources:
                if source_name not in self.source_names:
                    self.result.add_error(
                        "SOURCE_NOT_FOUND",
                        errors.source_not_found(source_name, model.name, list(self.source_names)),
                    )

            for ref_name in refs:
                if ref_name not in self.model_names:
                    self.result.add_error(
                        "MODEL_NOT_FOUND",
                        errors.model_not_found(ref_name, model.name, list(self.model_names)),
                    )

            # Check from vs SQL consistency
            if model.from_:
                declared_sources = {f.source for f in model.from_ if f.source}
                declared_refs = {f.ref for f in model.from_ if f.ref}

                # Warn about declared but unused
                for declared in declared_sources - set(sources):
                    self.result.add_warning(
                        "UNUSED_FROM_SOURCE",
                        f"Source '{declared}' declared in 'from' but not used in SQL",
                        f"model '{model.name}'",
                    )

                for declared in declared_refs - set(refs):
                    self.result.add_warning(
                        "UNUSED_FROM_REF",
                        f"Model '{declared}' declared in 'from' but not used in SQL",
                        f"model '{model.name}'",
                    )

            # Check ML_PREDICT usage without ml_outputs declaration
            self._validate_ml_predict_declarations(model)

        # Warn about security policies that require Gateway
        if model.security and model.security.policies:
            has_mask = any("mask" in p for p in model.security.policies)
            has_allow_deny = any("allow" in p or "deny" in p for p in model.security.policies)

            if has_mask or has_allow_deny:
                if not (self.project.runtime.conduktor and self.project.runtime.conduktor.gateway):
                    self.result.add_warning(
                        "SECURITY_POLICIES_NOT_ENFORCED",
                        f"Model '{model.name}' has security policies (masking/RBAC) that require "
                        "Conduktor Gateway to enforce. Policies will be metadata-only without Gateway.",
                        f"model '{model.name}'",
                    )

        # Validate access control
        if model.access.value == "private" and model.group:
            # Check references from other models
            for other_model in self.project.models:
                if other_model.name == model.name:
                    continue

                if other_model.sql and self.parser:
                    _, refs = self.parser.extract_refs_from_sql(other_model.sql)
                    if model.name in refs:
                        # Check if other model is in same group
                        if other_model.group != model.group:
                            self.result.add_error(
                                "ACCESS_DENIED",
                                errors.access_denied(other_model.name, model.name, model.group),
                            )

    def _validate_tests(self) -> None:
        """Validate test declarations."""
        for test in self.project.tests:
            # Check model exists
            if test.model not in self.model_names and test.model not in self.source_names:
                available = list(self.model_names | self.source_names)
                self.result.add_error(
                    "TEST_MODEL_NOT_FOUND",
                    errors.test_model_not_found(test.name, test.model, available),
                )

            # Check continuous tests require Flink
            if test.type.value == "continuous":
                if not self.project.runtime.flink:
                    self.result.add_error(
                        "CONTINUOUS_TEST_REQUIRES_FLINK",
                        errors.continuous_test_without_flink(test.name),
                    )

            # Warn if sample_size is set on non-sample tests
            if test.sample_size is not None and test.type.value != "sample":
                self.result.add_warning(
                    "SAMPLE_SIZE_IGNORED",
                    f"Test '{test.name}' has sample_size but is type '{test.type.value}'. "
                    f"sample_size only applies to 'sample' tests.",
                    f"test '{test.name}'",
                )

            # Validate DLQ references
            if test.on_failure:
                for action in test.on_failure.actions:
                    if "dlq" in action:
                        dlq_model = action["dlq"].get("model")
                        if dlq_model and dlq_model not in self.model_names:
                            self.result.add_error(
                                "DLQ_MODEL_NOT_FOUND",
                                f"DLQ model '{dlq_model}' not found in test '{test.name}'",
                            )

    def _validate_exposures(self) -> None:
        """Validate exposure declarations."""
        for exposure in self.project.exposures:
            # Warn about SLA configuration (metadata-only, not enforced)
            if exposure.sla:
                self.result.add_warning(
                    "SLA_NOT_ENFORCED",
                    f"Exposure '{exposure.name}' has SLA configuration. "
                    "SLAs are metadata-only and not actively monitored/enforced.",
                    f"exposure '{exposure.name}'",
                )

            # Validate produces references
            for ref in exposure.produces:
                if ref.source and ref.source not in self.source_names:
                    self.result.add_error(
                        "EXPOSURE_SOURCE_NOT_FOUND",
                        errors.exposure_source_not_found(
                            exposure.name, ref.source, list(self.source_names)
                        ),
                    )

            # Validate consumes references
            for ref in exposure.consumes:
                if ref.ref and ref.ref not in self.model_names:
                    self.result.add_error(
                        "EXPOSURE_MODEL_NOT_FOUND",
                        errors.exposure_model_not_found(exposure.name, ref.ref, list(self.model_names)),
                    )

            # Validate depends_on references
            for ref in exposure.depends_on:
                if ref.ref and ref.ref not in self.model_names:
                    self.result.add_error(
                        "EXPOSURE_DEPENDENCY_NOT_FOUND",
                        errors.exposure_dependency_not_found(
                            exposure.name, ref.ref, "model", list(self.model_names)
                        ),
                    )
                if ref.source and ref.source not in self.source_names:
                    self.result.add_error(
                        "EXPOSURE_DEPENDENCY_NOT_FOUND",
                        errors.exposure_dependency_not_found(
                            exposure.name, ref.source, "source", list(self.source_names)
                        ),
                    )

    def _validate_dag(self) -> None:
        """Validate DAG has no cycles."""
        # Build adjacency list
        graph: dict[str, set[str]] = {m.name: set() for m in self.project.models}

        for model in self.project.models:
            if model.sql and self.parser:
                _, refs = self.parser.extract_refs_from_sql(model.sql)
                for ref in refs:
                    if ref in graph:
                        graph[model.name].add(ref)

        # Detect cycles using DFS - colors: 0=white (unvisited), 1=gray (visiting), 2=black (done)
        white, gray, black = 0, 1, 2
        color = {node: white for node in graph}
        path: list[str] = []

        def dfs(node: str) -> Optional[list[str]]:
            color[node] = gray
            path.append(node)

            for neighbor in graph[node]:
                if color[neighbor] == gray:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]
                elif color[neighbor] == white:
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle

            path.pop()
            color[node] = black
            return None

        for node in graph:
            if color[node] == white:
                cycle = dfs(node)
                if cycle:
                    self.result.add_error(
                        "CYCLE_DETECTED",
                        errors.cycle_detected(cycle),
                    )
                    return

    def _validate_rules(self) -> None:
        """Validate governance rules."""
        rules = self.project.rules
        if not rules:
            return

        # Topic rules
        if rules.topics:
            for model in self.project.models:
                if model.get_materialized() in [
                    MaterializedType.TOPIC,
                    MaterializedType.FLINK,
                ]:
                    self._validate_topic_rules(model, rules.topics)

        # Model rules
        if rules.models:
            for model in self.project.models:
                self._validate_model_rules(model, rules.models)

        # Source rules
        if rules.sources:
            for source in self.project.sources:
                self._validate_source_rules(source, rules.sources)

        # Security rules
        if rules.security:
            self._validate_security_rules(rules.security)

    def _validate_topic_rules(self, model: Model, rules: TopicRules) -> None:
        """Validate topic rules for a model."""

        topic_config = model.get_topic_config()

        # Check min partitions
        if rules.min_partitions is not None:
            partitions = topic_config.partitions if topic_config else None
            if partitions is not None and partitions < rules.min_partitions:
                self.result.add_error(
                    "RULE_MIN_PARTITIONS",
                    f"Model '{model.name}' violates rule 'topics.min_partitions': "
                    f"expected >= {rules.min_partitions}, got {partitions}",
                )

        # Check max partitions
        if rules.max_partitions is not None:
            partitions = topic_config.partitions if topic_config else None
            if partitions is not None and partitions > rules.max_partitions:
                self.result.add_error(
                    "RULE_MAX_PARTITIONS",
                    f"Model '{model.name}' violates rule 'topics.max_partitions': "
                    f"expected <= {rules.max_partitions}, got {partitions}",
                )

        # Check min replication factor
        if rules.min_replication_factor is not None:
            rf = topic_config.replication_factor if topic_config else None
            if rf is not None and rf < rules.min_replication_factor:
                self.result.add_error(
                    "RULE_MIN_REPLICATION",
                    f"Model '{model.name}' violates rule 'topics.min_replication_factor': "
                    f"expected >= {rules.min_replication_factor}, got {rf}",
                )

        # Check naming pattern
        if rules.naming_pattern:
            topic_name = topic_config.name if topic_config and topic_config.name else model.name
            if not re.match(rules.naming_pattern, topic_name):
                self.result.add_error(
                    "RULE_NAMING_PATTERN",
                    f"Topic name '{topic_name}' does not match pattern '{rules.naming_pattern}'",
                )

        # Check forbidden prefixes
        if rules.forbidden_prefixes:
            topic_name = topic_config.name if topic_config and topic_config.name else model.name
            for prefix in rules.forbidden_prefixes:
                if topic_name.startswith(prefix):
                    self.result.add_error(
                        "RULE_FORBIDDEN_PREFIX",
                        f"Topic name '{topic_name}' has forbidden prefix '{prefix}'",
                    )

    def _validate_model_rules(self, model: Model, rules: ModelRules) -> None:
        """Validate model rules."""

        # Check require_description
        if rules.require_description and not model.description:
            self.result.add_error(
                "RULE_REQUIRE_DESCRIPTION",
                f"Model '{model.name}' missing required field 'description'",
            )

        # Check require_owner
        if rules.require_owner and not model.owner:
            self.result.add_error(
                "RULE_REQUIRE_OWNER",
                f"Model '{model.name}' missing required field 'owner'",
            )

        # Check require_tests
        if rules.require_tests:
            has_test = any(t.model == model.name for t in self.project.tests)
            if not has_test:
                self.result.add_error(
                    "RULE_REQUIRE_TESTS",
                    f"Model '{model.name}' has no tests defined",
                )

        # Check max_dependencies
        if rules.max_dependencies is not None and self.parser:
            if model.sql:
                sources, refs = self.parser.extract_refs_from_sql(model.sql)
                dep_count = len(sources) + len(refs)
                if dep_count > rules.max_dependencies:
                    self.result.add_error(
                        "RULE_MAX_DEPENDENCIES",
                        f"Model '{model.name}' has {dep_count} dependencies, "
                        f"max allowed is {rules.max_dependencies}",
                    )

    def _validate_source_rules(self, source: Source, rules: SourceRules) -> None:
        """Validate source rules."""

        # Check require_schema
        if rules.require_schema and not source.schema_:
            self.result.add_error(
                "RULE_REQUIRE_SCHEMA",
                f"Source '{source.name}' missing required field 'schema'",
            )

        # Check require_freshness
        if rules.require_freshness and not source.freshness:
            self.result.add_error(
                "RULE_REQUIRE_FRESHNESS",
                f"Source '{source.name}' missing required field 'freshness'",
            )

    def _validate_security_rules(self, rules: SecurityRules) -> None:
        """Validate security rules."""

        sensitive_columns: dict[str, list[str]] = {}  # source/model -> columns

        # Collect sensitive columns from sources
        for source in self.project.sources:
            for col in source.columns:
                if col.classification in [
                    Classification.SENSITIVE,
                    Classification.HIGHLY_SENSITIVE,
                ]:
                    if source.name not in sensitive_columns:
                        sensitive_columns[source.name] = []
                    sensitive_columns[source.name].append(col.name)

        # Collect sensitive columns from models
        for model in self.project.models:
            if model.security and model.security.classification:
                for col, classification in model.security.classification.items():
                    if classification in [
                        Classification.SENSITIVE,
                        Classification.HIGHLY_SENSITIVE,
                    ]:
                        if model.name not in sensitive_columns:
                            sensitive_columns[model.name] = []
                        sensitive_columns[model.name].append(col)

        # Check require_classification (sources must have all columns classified)
        if rules.require_classification:
            for source in self.project.sources:
                if source.columns:
                    for col in source.columns:
                        if col.classification is None:
                            self.result.add_error(
                                "RULE_REQUIRE_CLASSIFICATION",
                                f"Column '{col.name}' in source '{source.name}' "
                                f"missing required classification",
                            )

        # Check sensitive_columns_require_masking
        if rules.sensitive_columns_require_masking:
            for entity_name, columns in sensitive_columns.items():
                for col in columns:
                    # Check if any model has masking for this column
                    has_masking = False
                    for model in self.project.models:
                        if model.security and model.security.policies:
                            for policy in model.security.policies:
                                if "mask" in policy:
                                    if policy["mask"].get("column") == col:
                                        has_masking = True
                                        break

                    if not has_masking:
                        self.result.add_error(
                            "RULE_SENSITIVE_REQUIRES_MASKING",
                            f"Column '{col}' in '{entity_name}' classified as sensitive "
                            f"has no masking policy",
                        )

    def _has_confluent_flink(self) -> bool:
        """Check if any configured Flink cluster is Confluent Cloud."""
        if not self.project.runtime.flink:
            return False
        for cluster in self.project.runtime.flink.clusters.values():
            if cluster.type == "confluent":
                return True
        return False

    def _validate_ml_predict_declarations(self, model: Model) -> None:
        """Validate ML_PREDICT/ML_EVALUATE usage has corresponding ml_outputs declarations.

        When a model uses ML_PREDICT or ML_EVALUATE functions:
        1. Error if no Confluent Flink cluster is configured (these are Confluent-only)
        2. Warn if there's no corresponding ml_outputs declaration
        """
        if not model.sql:
            return

        # Find ML_PREDICT/ML_EVALUATE patterns in SQL
        ml_pattern = re.compile(
            r'\b(ML_PREDICT|ML_EVALUATE)\s*\(\s*(\w+|\([^)]+\))',
            re.IGNORECASE
        )
        matches = ml_pattern.findall(model.sql)

        if not matches:
            return

        # Check if Confluent Flink is configured - ML_PREDICT/ML_EVALUATE are Confluent-only
        if not self._has_confluent_flink():
            # Get the first function name used for the error message
            func_name = matches[0][0].upper()
            self.result.add_error(
                "CONFLUENT_FLINK_REQUIRED",
                errors.confluent_flink_required(model.name, func_name),
                f"model '{model.name}'",
            )
            return  # Don't add further warnings if there's a fundamental error

        # Extract ML model names from the matches
        ml_models_used: set[str] = set()
        for func_name, first_arg in matches:
            # First argument is typically the model reference
            # Could be: model_name, `model_name`, or TABLE(model_name)
            clean_name = first_arg.strip('`"\'() ')
            if clean_name.upper().startswith('TABLE'):
                # Extract from TABLE(...) syntax
                inner = re.match(r'TABLE\s*\(\s*(\w+)', first_arg, re.IGNORECASE)
                if inner:
                    clean_name = inner.group(1)
            ml_models_used.add(clean_name)

        # Check which ML models lack declarations
        declared_ml_outputs = set(model.ml_outputs.keys()) if model.ml_outputs else set()
        undeclared = ml_models_used - declared_ml_outputs

        for ml_model in undeclared:
            self.result.add_warning(
                "ML_PREDICT_OPAQUE_OUTPUT",
                f"Model '{model.name}' uses ML_PREDICT/ML_EVALUATE with '{ml_model}' "
                "but no ml_outputs declaration found. The ML output schema is opaque - "
                "streamt cannot infer types, track lineage, or detect breaking changes "
                "if the ML model schema changes. Add ml_outputs declaration:\n"
                f"  ml_outputs:\n"
                f"    {ml_model}:\n"
                f"      columns:\n"
                f"        - name: <output_column>\n"
                f"          type: <FLINK_SQL_TYPE>",
                f"model '{model.name}'",
            )
