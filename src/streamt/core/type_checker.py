"""Column type checker for validating column references across sources and models."""

from __future__ import annotations

import difflib
import logging
import re
from dataclasses import dataclass
from typing import Optional

from streamt.compiler.type_inference import TypeInferenceMixin
from streamt.core.models import Model, StreamtProject

logger = logging.getLogger(__name__)


@dataclass
class TypeCheckResult:
    """Result of a type check on a single column."""

    column: str
    issue: str  # "missing_column", "type_incompatible"
    model: str
    source_or_model: str
    expected_type: Optional[str] = None
    actual_type: Optional[str] = None
    suggestion: Optional[str] = None


class _MinimalTypeInference(TypeInferenceMixin):
    """Minimal host for TypeInferenceMixin used by the checker."""

    def __init__(self, project: StreamtProject) -> None:
        self._project = project
        self._current_model: Optional[Model] = None
        self._udf_types: dict[str, str] = {
            udf.name.upper(): udf.return_type for udf in project.udfs
        }

    def _build_source_schema(self, model: Model) -> dict[str, str]:
        """Build schema from model dependencies."""
        schema: dict[str, str] = {}
        deps = self._get_deps(model)

        for dep_name, dep_type in deps:
            if dep_type == "source":
                source = self._project.get_source(dep_name)
                if source and source.columns:
                    for col in source.columns:
                        if col.proctime:
                            col_type = "TIMESTAMP_LTZ(3)"
                        elif source.event_time and col.name == source.event_time.column:
                            col_type = "TIMESTAMP(3)"
                        elif col.type:
                            col_type = col.type
                        else:
                            col_type = "STRING"
                        schema[col.name] = col_type
                        schema[f"{dep_name}.{col.name}"] = col_type
            else:
                dep_model = self._project.get_model(dep_name)
                if dep_model and dep_model.sql:
                    upstream_schema = self._build_source_schema(dep_model)
                    dep_columns = self._extract_select_columns_with_types(
                        dep_model.sql, schema_context=upstream_schema
                    )
                    for col_name, col_type in dep_columns:
                        schema[col_name] = col_type
        return schema

    def _get_deps(self, model: Model) -> list[tuple[str, str]]:
        deps: list[tuple[str, str]] = []
        if model.sql:
            sources, refs = self._extract_refs(model.sql)
            for s in sources:
                deps.append((s, "source"))
            for r in refs:
                deps.append((r, "model"))
        if model.from_:
            for f in model.from_:
                if f.source:
                    deps.append((f.source, "source"))
                if f.ref:
                    deps.append((f.ref, "model"))
        return deps

    @staticmethod
    def _extract_refs(sql: str) -> tuple[list[str], list[str]]:
        sources = re.findall(r"\{\{\s*source\s*\(\s*[\"'](\w+)[\"']\s*\)\s*\}\}", sql)
        refs = re.findall(r"\{\{\s*ref\s*\(\s*[\"'](\w+)[\"']\s*\)\s*\}\}", sql)
        return sources, refs


_TYPE_GROUPS: dict[str, str] = {
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
    "BOOLEAN": "boolean",
    "BOOL": "boolean",
    "TIMESTAMP": "timestamp",
    "DATETIME": "timestamp",
    "DATE": "date",
    "TIME": "time",
    "BYTES": "bytes",
}


def _normalize_type(t: str) -> str:
    """Normalize a Flink SQL type to its base group for comparison."""
    base = re.split(r"[\s(]", t.upper().strip())[0]
    return _TYPE_GROUPS.get(base, base.lower())


def _types_compatible(declared: str, inferred: str) -> bool:
    """Check if declared and inferred types are compatible."""
    return _normalize_type(declared) == _normalize_type(inferred)


def _suggest_similar(name: str, available: list[str]) -> Optional[str]:
    """Suggest a similar column name if a close match exists."""
    matches = difflib.get_close_matches(name, available, n=1, cutoff=0.6)
    return matches[0] if matches else None


class ColumnTypeChecker:
    """Checks column type compatibility between sources and models."""

    def __init__(self, project: StreamtProject) -> None:
        self._project = project
        self._inference = _MinimalTypeInference(project)
        self._source_map = {s.name: s for s in project.sources}
        self._model_map = {m.name: m for m in project.models}
        self._inferred_columns: dict[str, list[tuple[str, str]]] = {}

    def check_model(self, model: Model) -> list[TypeCheckResult]:
        """Run all column type checks for a model."""
        if not model.sql:
            return []
        results: list[TypeCheckResult] = []
        results.extend(self._check_missing_source_columns(model))
        results.extend(self._check_missing_ref_columns(model))
        results.extend(self._check_contract_type_inference(model))
        return results

    def _check_contract_type_inference(self, model: Model) -> list[TypeCheckResult]:
        """Check contract column types against SQL-inferred types."""
        results: list[TypeCheckResult] = []
        if not model.contract or not model.contract.columns or not model.sql:
            return results
        try:
            schema = self._inference._build_source_schema(model)
            inferred = self._inference._extract_select_columns_with_types(
                model.sql,
                schema_context=schema,
                model=model,
            )
        except Exception:
            return results

        inferred_map = dict(inferred)
        for col_spec in model.contract.columns:
            if not col_spec.type:
                continue
            inferred_type = inferred_map.get(col_spec.name)
            if not inferred_type:
                continue
            if not _types_compatible(col_spec.type, inferred_type):
                results.append(
                    TypeCheckResult(
                        column=col_spec.name,
                        issue="type_incompatible",
                        model=model.name,
                        source_or_model=f"contract for '{model.name}'",
                        expected_type=col_spec.type,
                        actual_type=inferred_type,
                    )
                )
        return results

    def _check_missing_source_columns(self, model: Model) -> list[TypeCheckResult]:
        """Check if SQL references columns that don't exist in declared sources."""
        results: list[TypeCheckResult] = []
        if not model.sql:
            return results

        sources_used, _ = _MinimalTypeInference._extract_refs(model.sql)
        schema = self._inference._build_source_schema(model)

        try:
            self._inference._extract_select_columns_with_types(
                model.sql,
                schema_context=schema,
                model=model,
            )
        except Exception:
            logger.debug("Type inference failed for model '%s', skipping column checks", model.name)
            return results

        # For each source referenced, check if its declared columns cover what's used
        for source_name in sources_used:
            source = self._source_map.get(source_name)
            if not source or not source.columns:
                continue  # No declared columns → skip

            declared = {col.name for col in source.columns}
            # Extract column references from the SQL that come from this source
            referenced = self._extract_source_column_refs(model.sql, source_name, schema)
            for col_name in referenced:
                if col_name not in declared:
                    suggestion = _suggest_similar(col_name, list(declared))
                    results.append(
                        TypeCheckResult(
                            column=col_name,
                            issue="missing_column",
                            model=model.name,
                            source_or_model=f"source '{source_name}'",
                            suggestion=suggestion,
                        )
                    )
        return results

    def _check_missing_ref_columns(self, model: Model) -> list[TypeCheckResult]:
        """Check if SQL references columns not in upstream model output."""
        results: list[TypeCheckResult] = []
        if not model.sql:
            return results

        _, refs_used = _MinimalTypeInference._extract_refs(model.sql)
        schema = self._inference._build_source_schema(model)

        for ref_name in refs_used:
            upstream = self._model_map.get(ref_name)
            if not upstream:
                continue

            # Get upstream model's output columns
            upstream_cols = self._get_model_output_columns(upstream)
            if not upstream_cols:
                continue

            upstream_names = {name for name, _ in upstream_cols}
            referenced = self._extract_source_column_refs(model.sql, ref_name, schema)
            for col_name in referenced:
                if col_name not in upstream_names:
                    suggestion = _suggest_similar(col_name, list(upstream_names))
                    results.append(
                        TypeCheckResult(
                            column=col_name,
                            issue="missing_column",
                            model=model.name,
                            source_or_model=f"model '{ref_name}'",
                            suggestion=suggestion,
                        )
                    )
        return results

    def _get_model_output_columns(self, model: Model) -> list[tuple[str, str]]:
        """Get a model's output columns (declared or inferred)."""
        # Prefer declared columns as the contract
        if model.columns:
            return [(c.name, c.type or "STRING") for c in model.columns]

        # Cache inferred columns
        if model.name in self._inferred_columns:
            return self._inferred_columns[model.name]

        if not model.sql:
            return []

        try:
            schema = self._inference._build_source_schema(model)
            cols = self._inference._extract_select_columns_with_types(
                model.sql,
                schema_context=schema,
                model=model,
            )
            self._inferred_columns[model.name] = cols
            return cols
        except Exception:
            logger.debug("Cannot infer columns for model '%s'", model.name)
            return []

    def _extract_source_column_refs(
        self,
        sql: str,
        table_name: str,
        _schema: dict[str, str],
    ) -> set[str]:
        """Extract column names referenced in SQL from a specific source/model.

        Uses sqlglot to parse the SQL and find Column expressions. Returns
        unqualified column names that are referenced in the query (excluding
        SELECT * which passes through all columns).
        """
        import sqlglot
        from sqlglot import exp

        from streamt.compiler.flink_dialect import FlinkDialect

        clean_sql = re.sub(r"\{\{\s*source\s*\(\s*[\"'](\w+)[\"']\s*\)\s*\}\}", r"\1", sql)
        clean_sql = re.sub(r"\{\{\s*ref\s*\(\s*[\"'](\w+)[\"']\s*\)\s*\}\}", r"\1", clean_sql)

        referenced: set[str] = set()
        try:
            parsed = sqlglot.parse_one(clean_sql, dialect=FlinkDialect)
        except Exception:
            return referenced

        # If SELECT * is used, all columns pass through — no missing column check
        select = parsed.find(exp.Select) if not isinstance(parsed, exp.Select) else parsed
        if select:
            for expr in select.expressions:
                if isinstance(expr, exp.Star):
                    return set()
                if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                    return set()

        for col in parsed.find_all(exp.Column):
            col_name = col.name
            # Skip if qualified to a different table
            if col.table and col.table.lower() != table_name.lower():
                continue
            # Skip virtual columns
            if col_name.upper() in (
                "$ROWTIME",
                "ROWTIME",
                "$PROCTIME",
                "PROCTIME",
                "WINDOW_START",
                "WINDOW_END",
                "WINDOW_TIME",
            ):
                continue
            referenced.add(col_name)

        return referenced
