"""Validator for streamt projects."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Optional, SupportsIndex, SupportsInt

from streamt.core import errors
from streamt.core.models import (
    DataResidencyRules,
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

if TYPE_CHECKING:
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

logger = logging.getLogger(__name__)

_CONTRACT_TYPE_GROUPS: dict[str, str] = {
    "STRING": "string",
    "VARCHAR": "string",
    "TEXT": "string",
    "CHAR": "string",
    "INT": "int",
    "INTEGER": "int",
    "SMALLINT": "int",
    "TINYINT": "int",
    "BIGINT": "bigint",
    "LONG": "bigint",
    "DOUBLE": "float",
    "FLOAT": "float",
    "DECIMAL": "float",
    "NUMERIC": "float",
    "REAL": "float",
    "BOOLEAN": "boolean",
    "BOOL": "boolean",
    "TIMESTAMP": "timestamp",
    "DATETIME": "timestamp",
    "DATE": "date",
    "TIME": "time",
    "BYTES": "bytes",
}


def _normalize_contract_type(t: str) -> str:
    """Normalize a SQL type string for contract compatibility comparison."""
    base = t.split("(")[0].upper().strip()
    return _CONTRACT_TYPE_GROUPS.get(base, base.lower())


def _known_declared_contract_type(value: str | None) -> str | None:
    """Compare recognized local type families without guessing unknown schemas.

    This reuses the contract aliases, not a precision, nullability, or runtime
    compatibility proof. Complex and undeclared types remain unknown.
    """
    if (
        not value
        or value.split("(")[0].upper().strip() not in _CONTRACT_TYPE_GROUPS
        or re.fullmatch(r"\s*[A-Za-z]+\s*(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?\s*", value) is None
    ):
        return None
    return _normalize_contract_type(value)


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

    def __init__(
        self,
        project: StreamtProject,
        schema_registry_deployer: Optional[SchemaRegistryDeployer] = None,
    ) -> None:
        """Initialize validator with project."""
        self.project = project
        self.schema_registry_deployer = schema_registry_deployer
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
        self._validate_live_schemas()
        self._validate_models()
        self._validate_tests()
        self._validate_exposures()
        self._validate_dag()
        self._validate_column_types()
        self._validate_model_contracts()
        self._validate_references()
        self._validate_rules()
        self._validate_schema_versions()
        self._validate_unused_sources()
        self._validate_source_columns()
        if self.result.is_valid and any(
            model.executor and model.executor.value == "kafka_streams" for model in self.project.models
        ):
            # The bounded executor has a closed grammar and exact inferred
            # output schema. Validate must enforce that same compiler contract,
            # without writing artifacts or constructing providers.
            from streamt.compiler import Compiler
            from streamt.compiler.model_resolution import CompileError

            try:
                Compiler(self.project).compile(dry_run=True)
            except (ValueError, CompileError) as error:
                self.result.add_error("KAFKA_STREAMS_INVALID", str(error), "project")
        return self.result

    def _validate_duplicates(self) -> None:
        """Check for duplicate names and name collisions."""
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

        # Check source/model name collisions
        collisions = self.source_names & self.model_names
        for name in collisions:
            self.result.add_error(
                "NAME_COLLISION",
                f"Name '{name}' is used as both a source and a model. "
                f"This creates ambiguity between source() and ref() references "
                f"and may cause DDL conflicts at deploy time.",
            )

    def _validate_sources(self) -> None:
        """Validate source declarations."""
        for source in self.project.sources:
            # Topic is required (already handled by Pydantic)
            if not source.topic:
                self.result.add_error(
                    "MISSING_TOPIC",
                    f"Source '{source.name}' missing required field 'topic'",
                )

    def _validate_live_schemas(self) -> None:
        """Resolve external source schemas when an opt-in Registry client is provided.

        Inline definitions and field-generated schemas are deployable desired state, so
        their subjects are not required to exist yet. External references are read-only:
        the resolver fetches the selected version and its pinned reference graph.
        """
        deployer = self.schema_registry_deployer
        if deployer is None:
            return

        from streamt.deployer.schema_registry import SchemaRegistryResolutionError

        supported_schema_types = {"AVRO", "JSON", "PROTOBUF"}
        for source in self.project.sources:
            schema_ref = source.schema_
            if schema_ref is None:
                continue
            if schema_ref.definition is not None or schema_ref.fields is not None:
                continue
            if schema_ref.subject is None and schema_ref.registry is None:
                continue

            location = f"source '{source.name}'.schema"
            if schema_ref.registry not in (None, "confluent"):
                self.result.add_error(
                    "SCHEMA_REGISTRY_SELECTOR_UNSUPPORTED",
                    f"Source '{source.name}' selects schema registry "
                    f"'{schema_ref.registry}', but this runtime has one Confluent-compatible "
                    "Schema Registry client. Use 'confluent' or omit schema.registry.",
                    location,
                )
                continue

            subject = schema_ref.subject or f"{source.topic}-value"
            try:
                state = deployer.resolve_schema_state(subject, schema_ref.version)
            except SchemaRegistryResolutionError as e:
                self.result.add_error(
                    "SCHEMA_REGISTRY_RESOLUTION_FAILED",
                    f"Could not resolve Schema Registry subject '{subject}' for source "
                    f"'{source.name}': {e}",
                    location,
                )
                continue
            except Exception as e:
                self.result.add_error(
                    "SCHEMA_REGISTRY_UNAVAILABLE",
                    f"Could not read Schema Registry subject '{subject}' for source "
                    f"'{source.name}': {e}. Check runtime.schema_registry URL, TLS, and "
                    "credentials, then retry with --check-schemas.",
                    location,
                )
                continue

            if not state.exists:
                self.result.add_error(
                    "SCHEMA_SUBJECT_NOT_FOUND",
                    f"Schema Registry subject '{subject}' version '{schema_ref.version}' "
                    f"does not exist for source '{source.name}'. Check schema.subject and "
                    "schema.version, or provide an inline schema for a new subject.",
                    location,
                )
                continue

            schema_type = (state.schema_type or "AVRO").upper()
            if schema_type not in supported_schema_types:
                self.result.add_error(
                    "SCHEMA_TYPE_UNSUPPORTED",
                    f"Schema Registry subject '{subject}' resolved to unsupported type "
                    f"'{schema_type}'. streamt validates Avro, JSON Schema, and Protobuf "
                    "subject/version metadata and references.",
                    location,
                )
                continue

            expected_type = schema_ref.format.upper() if schema_ref.format else None
            if expected_type is not None and expected_type != schema_type:
                self.result.add_error(
                    "SCHEMA_FORMAT_MISMATCH",
                    f"Source '{source.name}' declares schema format "
                    f"'{schema_ref.format}', but subject '{subject}' version "
                    f"{state.version} is '{schema_type.lower()}'.",
                    location,
                )
                continue

            compatibility = (
                f", compatibility {state.compatibility}" if state.compatibility else ""
            )
            self.result.add_info(
                "SCHEMA_SUBJECT_RESOLVED",
                f"Resolved Schema Registry subject '{subject}' version {state.version} "
                f"(id {state.schema_id}, type {schema_type}, "
                f"{len(state.references)} direct reference(s){compatibility}).",
                location,
            )

    def _validate_models(self) -> None:
        """Validate model declarations."""
        for model in self.project.models:
            self._validate_model(model)

    def _validate_model(self, model: Model) -> None:
        """Validate a single model."""
        # Check SQL has FROM clause (expected for streaming, but warn rather than hard-fail)
        if model.sql and not re.search(r"\bFROM\b", model.sql, re.IGNORECASE):
            self.result.add_warning(
                "SQL_NO_FROM",
                f"Model '{model.name}' SQL has no FROM clause. "
                "Streaming models typically read from at least one table or source.",
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
                        # Neither Gateway nor Flink available → warn (auto-detected, not explicit)
                        self.result.add_warning(
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
        flink_config = model.get_flink_config()
        if flink_config is not None and flink_config.state_ttl_ms is not None:
            ttl = flink_config.state_ttl_ms
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
                error_message = error if error is not None else "Unknown Jinja syntax error"
                self.result.add_error(
                    "JINJA_SYNTAX_ERROR",
                    errors.jinja_syntax_error(model.name, error_message, model.sql),
                )
                return

            # Validate SQL syntax via sqlglot
            self._validate_sql_syntax(model)

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
                        dlq_config = action["dlq"]
                        dlq_model = (
                            dlq_config.get("model") if isinstance(dlq_config, dict) else None
                        )
                        if isinstance(dlq_model, str) and dlq_model not in self.model_names:
                            self.result.add_error(
                                "DLQ_MODEL_NOT_FOUND",
                                f"DLQ model '{dlq_model}' not found in test '{test.name}'",
                            )

    def _validate_exposures(self) -> None:
        """Validate exposure declarations."""
        for exposure in self.project.exposures:
            if exposure.name in self.source_names | self.model_names:
                self.result.add_error(
                    "NAME_COLLISION",
                    f"Exposure '{exposure.name}' shares a name with a source or model. "
                    "Graph node names must be distinct across these declarations.",
                    f"exposure '{exposure.name}'",
                )

            # Warn about SLA configuration (metadata-only, not enforced)
            if exposure.sla:
                self.result.add_warning(
                    "SLA_NOT_ENFORCED",
                    f"Exposure '{exposure.name}' has SLA configuration. "
                    "SLAs are metadata-only and not actively monitored/enforced.",
                    f"exposure '{exposure.name}'",
                )

            for relationship, references in (
                ("produces", exposure.produces),
                ("consumes", exposure.consumes),
                ("depends_on", exposure.depends_on),
            ):
                for index, ref in enumerate(references):
                    location = f"exposure '{exposure.name}'.{relationship}[{index}]"
                    specified = [value for value in (ref.source, ref.ref) if value is not None]
                    if len(specified) != 1 or not specified[0].strip():
                        self.result.add_error(
                            "EXPOSURE_REFERENCE_INVALID",
                            "An exposure reference must set exactly one non-empty "
                            "'source' or 'ref'.",
                            location,
                        )
                        continue
                    for kind, name, known in (
                        ("source", ref.source, self.source_names),
                        ("model", ref.ref, self.model_names),
                    ):
                        if name is None or name in known:
                            continue
                        if relationship == "depends_on":
                            code = "EXPOSURE_DEPENDENCY_NOT_FOUND"
                            message = errors.exposure_dependency_not_found(
                                exposure.name, name, kind, sorted(known)
                            )
                        elif kind == "source":
                            code = "EXPOSURE_SOURCE_NOT_FOUND"
                            message = errors.exposure_source_not_found(
                                exposure.name, name, sorted(known)
                            )
                        else:
                            code = "EXPOSURE_MODEL_NOT_FOUND"
                            message = errors.exposure_model_not_found(
                                exposure.name, name, sorted(known)
                            )
                        self.result.add_error(code, message, location)

        if self.project.exposures and self.result.is_valid:
            from streamt.core.dag import DAGBuilder, DAGCycleError

            try:
                DAGBuilder(self.project).build().topological_sort()
            except DAGCycleError as error:
                self.result.add_error("CYCLE_DETECTED", str(error))

    def _validate_dag(self) -> None:
        """Validate DAG has no cycles."""
        if any(error.code == "CYCLE_DETECTED" for error in self.result.errors):
            return
        graph: dict[str, set[str]] = {m.name: set() for m in self.project.models}

        for model in self.project.models:
            if model.sql and self.parser:
                _, refs = self.parser.extract_refs_from_sql(model.sql)
                for ref in refs:
                    if ref in graph:
                        graph[model.name].add(ref)

        # Detect cycles using DFS - colors: 0=white (unvisited), 1=gray (visiting), 2=black (done)
        white, gray, black = 0, 1, 2
        color = dict.fromkeys(graph, white)
        path: list[str] = []

        def dfs(node: str) -> Optional[list[str]]:
            color[node] = gray
            path.append(node)

            for neighbor in graph[node]:
                if color[neighbor] == gray:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    return [*path[cycle_start:], neighbor]
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

    def _validate_column_types(self) -> None:
        """Validate column references across sources and models."""
        try:
            from streamt.core.type_checker import ColumnTypeChecker
        except ImportError:
            return

        checker = ColumnTypeChecker(self.project)
        for model in self.project.models:
            try:
                issues = checker.check_model(model)
            except Exception:
                logger.debug("Type check failed for model '%s'", model.name, exc_info=True)
                continue
            for issue in issues:
                if issue.issue == "type_incompatible":
                    self.result.add_warning(
                        "COLUMN_TYPE_MISMATCH",
                        f"Column '{issue.column}' in model '{issue.model}': "
                        f"declared '{issue.expected_type}' but SQL infers "
                        f"'{issue.actual_type}'",
                        f"model '{issue.model}'",
                    )
                else:
                    hint = f" Did you mean '{issue.suggestion}'?" if issue.suggestion else ""
                    self.result.add_warning(
                        "COLUMN_TYPE_CHECK",
                        f"Column '{issue.column}' not found in {issue.source_or_model} "
                        f"(referenced in model '{issue.model}').{hint}",
                        f"model '{issue.model}'",
                    )

    def _validate_model_contracts(self) -> None:
        """Validate model data contracts: column presence and type compatibility."""
        try:
            from streamt.compiler.type_inference import TypeInferenceMixin

            class _Helper(TypeInferenceMixin):
                def __init__(self, project: StreamtProject) -> None:
                    self.project = project
                    self._udf_types = {
                        udf.name.upper(): udf.return_type for udf in project.udfs
                    }

            helper: Optional[_Helper] = _Helper(self.project)
        except Exception:
            helper = None

        for model in self.project.models:
            if not model.contract:
                continue
            if not model.sql:
                continue

            inferred: list[tuple[str, str]] = []
            if helper:
                try:
                    inferred = helper._extract_select_columns_with_types(
                        model.sql, schema_context={}, model=None
                    )
                except Exception:
                    pass

            if not inferred:
                continue  # can't infer → skip to avoid false positives

            inferred_map = dict(inferred)
            enforced = model.contract.enforced

            for col_spec in model.contract.columns:
                col_name = col_spec.name
                if col_name not in inferred_map:
                    msg = (
                        f"Column '{col_name}' declared in contract is not produced "
                        f"by model '{model.name}' SQL"
                    )
                    if enforced:
                        self.result.add_error(
                            "CONTRACT_MISSING_COLUMN", msg, f"model '{model.name}'"
                        )
                    else:
                        self.result.add_warning(
                            "CONTRACT_MISSING_COLUMN", msg, f"model '{model.name}'"
                        )
                    continue

                if col_spec.type:
                    declared_norm = _normalize_contract_type(col_spec.type)
                    inferred_norm = _normalize_contract_type(inferred_map[col_name])
                    if declared_norm != inferred_norm:
                        msg = (
                            f"Column '{col_name}' in model '{model.name}': "
                            f"contract declares '{col_spec.type}' "
                            f"but SQL produces '{inferred_map[col_name]}'"
                        )
                        if enforced:
                            self.result.add_error(
                                "CONTRACT_TYPE_MISMATCH", msg, f"model '{model.name}'"
                            )
                        else:
                            self.result.add_warning(
                                "CONTRACT_TYPE_MISMATCH", msg, f"model '{model.name}'"
                            )

        self._validate_exposure_contracts()

    def _validate_exposure_contracts(self) -> None:
        """Check consumed columns against explicit declarations, never live schemas.

        Exposure.columns is a flat requirement on every consumed input, as in
        the existing contract check; it cannot express per-input requirements.
        """
        for exposure in self.project.exposures:
            if not exposure.columns:
                continue

            for ref in exposure.consumes:
                # Invalid references have their own diagnostics. Do not select
                # one interpretation of an ambiguous source/ref declaration.
                if bool(ref.ref) == bool(ref.source):
                    continue
                declared: dict[str, str | None]
                if ref.ref:
                    model = self.project.get_model(ref.ref)
                    if model is None:
                        continue
                    if model.contract is not None:
                        declared = {column.name: column.type for column in model.contract.columns}
                        origin = f"model '{model.name}' declared contract"
                    elif model.columns is not None:
                        declared = {column.name: column.type for column in model.columns}
                        origin = f"model '{model.name}' declared columns"
                    else:
                        continue
                else:
                    source = self.project.get_source(ref.source or "")
                    if source is None or not source.columns:
                        # Import without enrichment has no column evidence.
                        continue
                    declared = {column.name: column.type for column in source.columns}
                    origin = f"source '{source.name}' declared columns"

                for column in exposure.columns:
                    if column.name not in declared:
                        self.result.add_error(
                            "CONTRACT_BREAKING_CHANGE",
                            f"Exposure '{exposure.name}' consumes column '{column.name}' "
                            f"which is absent from {origin}",
                            f"exposure '{exposure.name}'",
                        )
                        continue
                    expected = _known_declared_contract_type(column.type)
                    actual = _known_declared_contract_type(declared[column.name])
                    if expected is not None and actual is not None and expected != actual:
                        self.result.add_error(
                            "CONTRACT_CONSUMER_TYPE_MISMATCH",
                            f"Exposure '{exposure.name}' consumes column '{column.name}' "
                            f"as '{column.type}', but {origin} specifies "
                            f"'{declared[column.name]}'",
                            f"exposure '{exposure.name}'",
                        )

    def _validate_references(self) -> None:
        from streamt.core import ref_validators as rv
        rv.validate_cluster_refs(self.project, self.result.add_error)
        rv.validate_connection_refs(self.project, self.result.add_error)
        rv.validate_key_columns(self.project, self.result.add_error)

    def _validate_rules(self) -> None:
        if not (rules := self.project.rules):
            return
        if rules.topics:
            for model in self.project.models:
                if model.get_materialized() in [MaterializedType.TOPIC, MaterializedType.FLINK]:
                    self._validate_topic_rules(model, rules.topics)
            if rules.topics.naming_pattern:
                for source in self.project.sources:
                    if not re.match(rules.topics.naming_pattern, source.topic):
                        self.result.add_error(
                            "NAMING_VIOLATION",
                            f"Source topic '{source.topic}' does not match naming pattern "
                            f"'{rules.topics.naming_pattern}'",
                            f"source '{source.name}'",
                        )
        if rules.models:
            for model in self.project.models:
                self._validate_model_rules(model, rules.models)
        if rules.sources:
            for source in self.project.sources:
                self._validate_source_rules(source, rules.sources)
        if rules.security:
            self._validate_security_rules(rules.security)
        if rules.data_residency:
            self._validate_data_residency(rules.data_residency)

    def _check_range(
        self, name: str, val: Optional[int], limit: int, op: str, rule: str, code: str
    ) -> None:
        if val is not None and ((op == ">=" and val < limit) or (op == "<=" and val > limit)):
            self.result.add_error(
                code,
                f"Model '{name}' violates rule 'topics.{rule}': expected {op} {limit}, got {val}",
            )

    def _validate_topic_rules(self, model: Model, rules: TopicRules) -> None:
        """Validate topic rules for a model."""
        tc = model.get_topic_config()
        partitions = tc.partitions if tc else None
        rf = tc.replication_factor if tc else None
        if rules.min_partitions is not None:
            self._check_range(
                model.name,
                partitions,
                rules.min_partitions,
                ">=",
                "min_partitions",
                "RULE_MIN_PARTITIONS",
            )
        if rules.max_partitions is not None:
            self._check_range(
                model.name,
                partitions,
                rules.max_partitions,
                "<=",
                "max_partitions",
                "RULE_MAX_PARTITIONS",
            )
        if rules.min_replication_factor is not None:
            self._check_range(
                model.name,
                rf,
                rules.min_replication_factor,
                ">=",
                "min_replication_factor",
                "RULE_MIN_REPLICATION",
            )
        if rules.max_replication_factor is not None:
            self._check_range(
                model.name,
                rf,
                rules.max_replication_factor,
                "<=",
                "max_replication_factor",
                "RULE_MAX_REPLICATION",
            )
        if rules.max_retention_ms is not None and tc:
            retention = tc.config.get("retention.ms") if tc.config else None
            if isinstance(
                retention, (str, bytes, bytearray, SupportsInt, SupportsIndex)
            ):
                try:
                    retention_val = int(retention)
                    if retention_val > rules.max_retention_ms and retention_val != -1:
                        self.result.add_error(
                            "RULE_MAX_RETENTION",
                            f"Model '{model.name}' topic retention {retention_val}ms exceeds "
                            f"max allowed {rules.max_retention_ms}ms",
                        )
                except (ValueError, TypeError, OverflowError):
                    pass
        topic_name = tc.name if tc and tc.name else model.name
        if rules.naming_pattern and not re.match(rules.naming_pattern, topic_name):
            self.result.add_error(
                "RULE_NAMING_PATTERN",
                f"Topic name '{topic_name}' does not match pattern '{rules.naming_pattern}'",
            )
        for prefix in rules.forbidden_prefixes:
            if topic_name.startswith(prefix):
                self.result.add_error(
                    "RULE_FORBIDDEN_PREFIX",
                    f"Topic name '{topic_name}' has forbidden prefix '{prefix}'",
                )
        for suffix in rules.forbidden_suffixes:
            if topic_name.endswith(suffix):
                self.result.add_error(
                    "RULE_FORBIDDEN_SUFFIX",
                    f"Topic name '{topic_name}' has forbidden suffix '{suffix}'",
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

        # Check valid_owners
        if rules.valid_owners and model.owner:
            if model.owner not in rules.valid_owners:
                self.result.add_error(
                    "RULE_INVALID_OWNER",
                    f"Model '{model.name}' owner '{model.owner}' is not in the allowed list: "
                    f"{', '.join(rules.valid_owners)}",
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
        from streamt.core.rule_validators import validate_security_rules
        validate_security_rules(self.project, rules, self.result.add_error)

    def _validate_data_residency(self, rules: DataResidencyRules) -> None:
        from streamt.core.rule_validators import validate_data_residency
        validate_data_residency(self.project, rules.allowed_regions, self.result.add_error)

    def _validate_schema_versions(self) -> None:
        """Check sequential versioning and no duplicate name+version."""
        from collections import defaultdict

        versions: dict[str, list[int]] = defaultdict(list)
        for model in self.project.models:
            if model.version is not None:
                versions[model.name].append(model.version)
        for name, vers in versions.items():
            if len(vers) != len(set(vers)):
                self.result.add_error(
                    "DUPLICATE_VERSION",
                    f"Model '{name}' has duplicate version numbers",
                )
            for v in vers:
                if v > 1 and (v - 1) not in vers:
                    self.result.add_warning(
                        "VERSION_GAP",
                        f"Model '{name}' has version {v} but no version {v - 1}",
                        f"model '{name}'",
                    )

    def _validate_sql_syntax(self, model: Model) -> None:
        """Validate SQL syntax using sqlglot; also warn on SELECT *."""
        if not model.sql:
            return
        if re.search(r"\bSELECT\s+\*\s", model.sql, re.IGNORECASE):
            self.result.add_warning(
                "SELECT_STAR",
                f"Model '{model.name}' uses SELECT *. Explicit column lists are safer "
                f"for streaming — schema changes upstream won't silently alter output.",
                f"model '{model.name}'",
            )
        try:
            import sqlglot

            from streamt.compiler.flink_dialect import FlinkDialect
            from streamt.core.sql_checks import check_having_aliases, check_window_group_by
        except ImportError:
            return
        rendered = re.sub(
            r'\{\{\s*(?:source|ref)\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}',
            r"\1",
            model.sql,
        )

        try:
            sqlglot.parse(rendered, dialect=FlinkDialect)
        except sqlglot.errors.ParseError as e:
            self.result.add_warning(
                "SQL_PARSE_WARNING",
                f"Model '{model.name}' SQL may have syntax issues: {e}",
                f"model '{model.name}'",
            )
        except Exception:
            pass
        ctx = f"model '{model.name}'"
        for code, msg in check_window_group_by(model.sql):
            self.result.add_warning(code, msg, ctx)
        for code, msg in check_having_aliases(model.sql):
            self.result.add_warning(code, msg, ctx)

    def _validate_unused_sources(self) -> None:
        """Warn when sources are not referenced by models or external applications."""
        if not self.parser:
            return

        consumed_sources: set[str] = set()
        for model in self.project.models:
            if model.sql:
                sources, _ = self.parser.extract_refs_from_sql(model.sql)
                consumed_sources.update(sources)

        for exposure in self.project.exposures:
            for ref in (*exposure.produces, *exposure.consumes, *exposure.depends_on):
                if ref.source:
                    consumed_sources.add(ref.source)

        for source in self.project.sources:
            if source.name not in consumed_sources:
                self.result.add_warning(
                    "UNUSED_SOURCE",
                    f"Source '{source.name}' is declared but not referenced by any model "
                    "or exposure. Consider declaring the application that uses it.",
                    f"source '{source.name}'",
                )

    def _validate_source_columns(self) -> None:
        """Warn when sources referenced by models have no column definitions."""
        if not self.parser:
            return

        warned: set[str] = set()
        for model in self.project.models:
            if not model.sql:
                continue
            sources, _ = self.parser.extract_refs_from_sql(model.sql)
            for source_name in sources:
                if source_name in warned:
                    continue
                source = next((s for s in self.project.sources if s.name == source_name), None)
                if source and not source.columns:
                    warned.add(source_name)
                    self.result.add_warning(
                        "SOURCE_NO_COLUMNS",
                        f"Source '{source_name}' referenced by model '{model.name}' "
                        f"has no column definitions. Column type checking and contract "
                        f"validation will be limited.",
                        f"source '{source_name}'",
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
        ml_pattern = re.compile(r"\b(ML_PREDICT|ML_EVALUATE)\s*\(\s*(\w+|\([^)]+\))", re.IGNORECASE)
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
        for _func_name, first_arg in matches:
            # First argument is typically the model reference
            # Could be: model_name, `model_name`, or TABLE(model_name)
            clean_name = first_arg.strip("`\"'() ")
            if clean_name.upper().startswith("TABLE"):
                # Extract from TABLE(...) syntax
                inner = re.match(r"TABLE\s*\(\s*(\w+)", first_arg, re.IGNORECASE)
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
