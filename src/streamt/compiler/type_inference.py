"""Type inference mixin for the streamt compiler.

Extracts all type-inference logic (expression type resolution, numeric promotion,
cast normalization, collection type helpers, regex fallback) into a mixin class
that Compiler inherits from.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Optional

import sqlglot
from sqlglot import exp

from streamt.compiler.flink_dialect import FlinkDialect, get_flink_function_type
from streamt.compiler.scope import _enrich_schema_from_scope

if TYPE_CHECKING:
    from streamt.core.models import Model

logger = logging.getLogger(__name__)


class TypeInferenceMixin:
    """Mixin providing Flink SQL type inference methods.

    Expects the host class to provide:
    - ``self._build_source_schema(model)`` for schema resolution
    - ``self._current_model`` for ML_PREDICT type inference

    Optional attributes:
    - ``self._udf_types`` — dict mapping upper-case function names to return types
    """

    # ------------------------------------------------------------------
    # Top-level column extraction (sqlglot + regex fallback)
    # ------------------------------------------------------------------

    def _extract_select_columns_with_types(
        self, sql: str, schema_context: Optional[dict[str, str]] = None, model: Optional[Model] = None
    ) -> list[tuple[str, str]]:
        """Extract column names and infer types from SELECT clause using sqlglot.

        Args:
            sql: The SQL query to parse
            schema_context: Optional pre-built schema context
            model: Optional model to build schema context from

        Returns list of (column_name, flink_type) tuples.
        """
        # Build schema context if not provided
        if schema_context is None and model is not None:
            schema_context = self._build_source_schema(model)
        elif schema_context is None:
            schema_context = {}

        # Store model for ML_PREDICT type inference (accessed in _infer_expression_type)
        self._current_model = model

        # Clean Jinja templates for parsing (replace with valid identifiers)
        clean_sql = re.sub(r'\{\{\s*source\s*\(\s*["\'](\w+)["\']\s*\)\s*\}\}', r'\1', sql)
        clean_sql = re.sub(r'\{\{\s*ref\s*\(\s*["\'](\w+)["\']\s*\)\s*\}\}', r'\1', clean_sql)

        try:
            # Parse SQL with FlinkDialect for proper Flink SQL support
            parsed = sqlglot.parse_one(clean_sql, dialect=FlinkDialect)
            if not isinstance(parsed, exp.Select):
                # Might be wrapped in other statements
                select = parsed.find(exp.Select)
                if not select:
                    self._current_model = None
                    return []
                parsed = select

            # Enrich schema with CTE and subquery column types
            schema_context = _enrich_schema_from_scope(self, parsed, dict(schema_context))

            columns = []
            for expr in parsed.expressions:
                # Handle SELECT * — expand all schema columns
                if isinstance(expr, exp.Star):
                    for col_name, col_type in schema_context.items():
                        if "." not in col_name:
                            columns.append((col_name, col_type))
                    continue
                # Handle table.* (qualified star)
                if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                    for col_name, col_type in schema_context.items():
                        if "." not in col_name:
                            columns.append((col_name, col_type))
                    continue
                col_name = self._get_expression_alias(expr)
                col_type = self._infer_expression_type(expr, schema_context)
                if col_name:
                    columns.append((col_name, col_type))

            self._current_model = None
            return columns

        except Exception as e:
            logger.debug(f"sqlglot parse failed, falling back to regex: {e}")
            self._current_model = None
            # Fallback to regex-based extraction
            return self._extract_select_columns_with_types_regex(sql, schema_context)

    # ------------------------------------------------------------------
    # Expression alias
    # ------------------------------------------------------------------

    def _get_expression_alias(self, expr: exp.Expression) -> Optional[str]:
        """Get the output column name for an expression."""
        # If it has an alias, use that
        if isinstance(expr, exp.Alias):
            return expr.alias
        # If it's a column reference, use the column name
        if isinstance(expr, exp.Column):
            return expr.name
        # For other expressions without aliases, this is invalid SQL
        return None

    # ------------------------------------------------------------------
    # Core expression type inference
    # ------------------------------------------------------------------

    def _infer_expression_type(self, expr: exp.Expression, schema: dict[str, str]) -> str:
        """Infer Flink SQL type from a sqlglot expression.

        Uses the schema context to resolve column reference types.
        Uses _current_model (set during extraction) for ML_PREDICT type inference.
        """
        # Unwrap alias to get the actual expression
        if isinstance(expr, exp.Alias):
            expr = expr.this

        # Column reference - look up in schema
        if isinstance(expr, exp.Column):
            col_name = expr.name
            # Try qualified name first (table.column) for disambiguation
            table = expr.table
            if table:
                qualified = f"{table}.{col_name}"
                if qualified in schema:
                    return schema[qualified]
            if col_name in schema:
                return schema[col_name]
            upper_name = col_name.upper()
            if upper_name == "$ROWTIME":
                return "TIMESTAMP_LTZ(3)"
            if upper_name == "ROWTIME":
                return "TIMESTAMP(3)"
            if upper_name in ("$PROCTIME", "PROCTIME"):
                return "TIMESTAMP_LTZ(3)"
            if upper_name in ("WINDOW_START", "WINDOW_END", "WINDOW_TIME"):
                return "TIMESTAMP(3)"
            return "STRING"

        # Aggregate functions
        if isinstance(expr, exp.Count):
            return "BIGINT"
        if isinstance(expr, exp.Sum):
            if expr.this is None:
                return "DOUBLE"
            input_type = self._infer_expression_type(expr.this, schema)
            base_type = input_type.split("(")[0].upper()
            if base_type in ("DECIMAL", "NUMERIC"):
                return self._widen_decimal_for_aggregate(input_type)
            if base_type in ("FLOAT", "DOUBLE"):
                return "DOUBLE"
            if base_type in ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"):
                return "BIGINT"
            return "DOUBLE"
        if isinstance(expr, exp.Avg):
            if expr.this is None:
                return "DOUBLE"
            input_type = self._infer_expression_type(expr.this, schema)
            base_type = input_type.split("(")[0].upper()
            if base_type in ("DECIMAL", "NUMERIC"):
                return self._widen_decimal_for_aggregate(input_type)
            return "DOUBLE"
        if isinstance(expr, (exp.Min, exp.Max)):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"
        if isinstance(
            expr,
            (
                exp.Stddev,
                exp.StddevPop,
                exp.StddevSamp,
                exp.Variance,
                exp.VariancePop,
                exp.CumeDist,
                exp.PercentRank,
            ),
        ):
            return "DOUBLE"

        # Case expression - infer from THEN/ELSE branches
        if isinstance(expr, exp.Case):
            branch_types: list[str] = []
            for when in expr.args.get("ifs", []):
                then_expr = when.args.get("true")
                if then_expr is not None:
                    branch_types.append(self._infer_expression_type(then_expr, schema))
            else_expr = expr.args.get("default")
            if else_expr is not None:
                branch_types.append(self._infer_expression_type(else_expr, schema))
            return self._merge_types(branch_types)

        # IF function - infer type from THEN branch
        if isinstance(expr, exp.If):
            return self._merge_types(
                [
                    self._infer_expression_type(expr.args.get("true"), schema)
                    if expr.args.get("true") is not None
                    else "",
                    self._infer_expression_type(expr.args.get("false"), schema)
                    if expr.args.get("false") is not None
                    else "",
                ]
            )

        # Coalesce - infer type from all arguments
        if isinstance(expr, exp.Coalesce):
            return self._merge_types(
                [self._infer_expression_type(arg, schema) for arg in expr.iter_expressions()]
            )
        if isinstance(expr, exp.Nullif):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"

        # String functions
        if isinstance(expr, (exp.Upper, exp.Lower, exp.Concat, exp.ConcatWs, exp.Substring, exp.Trim)):
            return "STRING"

        # NULL literal
        if isinstance(expr, exp.Null):
            return "NULL"

        # Numeric literals
        if isinstance(expr, exp.Literal):
            if expr.is_int:
                return "INT"
            if expr.is_number:
                return "DOUBLE"
            if expr.is_string:
                return "STRING"

        if isinstance(expr, exp.Extract):
            return "BIGINT"

        if isinstance(expr, exp.Round):
            value_type = (
                self._infer_expression_type(expr.this, schema)
                if expr.this is not None
                else "DOUBLE"
            )
            return value_type if self._is_numeric_type(value_type) else "DOUBLE"

        if isinstance(expr, exp.Rand):
            return "DOUBLE"

        if isinstance(expr, exp.Uuid):
            return "STRING"

        if isinstance(expr, exp.Array):
            return self._infer_array_literal_type(expr, schema)

        if isinstance(expr, exp.Bracket):
            if expr.this is None:
                return "STRING"
            container_type = self._infer_expression_type(expr.this, schema)
            element_type = self._extract_array_element_type(container_type)
            if element_type:
                return element_type
            key_value = self._extract_map_key_value_types(container_type)
            if key_value:
                return key_value[1]
            return "STRING"

        # Boolean literals and comparisons
        if isinstance(expr, exp.Boolean):
            return "BOOLEAN"
        if isinstance(
            expr,
            (
                exp.EQ,
                exp.NEQ,
                exp.GT,
                exp.GTE,
                exp.LT,
                exp.LTE,
                exp.And,
                exp.Or,
                exp.Not,
                exp.In,
                exp.Between,
                exp.Like,
                exp.ILike,
                exp.Is,
                exp.IsNullValue,
                exp.Exists,
                exp.RegexpLike,
                exp.RegexpILike,
                exp.RegexpFullMatch,
            ),
        ):
            return "BOOLEAN"

        # Arithmetic operations - use type promotion
        if isinstance(expr, exp.Div):
            left_type = self._infer_expression_type(expr.left, schema) if expr.left else "STRING"
            right_type = self._infer_expression_type(expr.right, schema) if expr.right else "STRING"
            return self._infer_division_type(left_type, right_type)
        if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Mod)):
            left_type = self._infer_expression_type(expr.left, schema) if expr.left else "STRING"
            right_type = self._infer_expression_type(expr.right, schema) if expr.right else "STRING"
            return self._promote_numeric_types(left_type, right_type)

        # Window functions
        if isinstance(expr, exp.Window):
            inner = expr.this
            if inner:
                return self._infer_expression_type(inner, schema)
            return "STRING"

        # Ranking window functions
        if isinstance(expr, (exp.RowNumber, exp.Rank, exp.DenseRank, exp.Ntile)):
            return "BIGINT"

        # LAG/LEAD - preserve argument type
        if isinstance(expr, (exp.Lag, exp.Lead)):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"

        # FirstValue/LastValue - preserve argument type
        if isinstance(expr, (exp.FirstValue, exp.LastValue, exp.NthValue)):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"

        # Timestamp conversion functions
        if isinstance(expr, (exp.TsOrDsToTimestamp, exp.StrToTime, exp.UnixToTime)):
            return "TIMESTAMP(3)"
        if isinstance(expr, exp.TimeToUnix):
            return "BIGINT"
        if isinstance(expr, (exp.TimeToStr, exp.UnixToStr)):
            return "STRING"

        # Anonymous functions (like TUMBLE_START, PROCTIME, etc.)
        if isinstance(expr, exp.Anonymous):
            func_name = expr.name.upper()

            # Handle ML_PREDICT specially - use ml_outputs if declared
            if func_name in ("ML_PREDICT", "ML_EVALUATE"):
                return self._infer_ml_predict_type(expr)

            # Check Flink-specific window functions first
            flink_type = get_flink_function_type(func_name)
            if flink_type:
                return flink_type
            args = list(expr.expressions)
            if func_name in ("IFNULL", "NVL", "GREATEST", "LEAST"):
                return self._merge_types(
                    [self._infer_expression_type(arg, schema) for arg in args]
                )
            if func_name == "TIMESTAMPADD":
                if len(args) >= 3:
                    return self._infer_expression_type(args[2], schema)
                return "TIMESTAMP(3)"
            if func_name == "COLLECT":
                return self._infer_collect_type(args, schema)
            if func_name == "ELEMENT":
                return self._infer_element_type(args, schema)
            if func_name in (
                "ARRAY_CONCAT",
                "ARRAY_DISTINCT",
                "ARRAY_REMOVE",
                "ARRAY_REVERSE",
                "ARRAY_SLICE",
                "ARRAY_UNION",
            ):
                return self._infer_array_return_type(args, schema)
            if func_name == "MAP_KEYS":
                return self._infer_map_keys_type(args, schema)
            if func_name == "MAP_VALUES":
                return self._infer_map_values_type(args, schema)
            if func_name == "MAP_ENTRIES":
                return self._infer_map_entries_type(args, schema)
            if func_name == "MAP_UNION":
                return self._infer_map_union_type(args, schema)
            if func_name == "MAP_FROM_ARRAYS":
                return self._infer_map_from_arrays_type(args, schema)
            if func_name == "STR_TO_MAP":
                return "MAP<STRING, STRING>"
            # Timestamp conversion functions
            if func_name in ("TO_TIMESTAMP", "FROM_UNIXTIME"):
                return "TIMESTAMP(3)"
            # String functions
            if func_name in ("UPPER", "LOWER", "CONCAT", "CONCAT_WS", "SUBSTRING", "TRIM", "REGEXP_REPLACE"):
                return "STRING"
            # Window boundaries
            if func_name in ("WINDOW_START", "WINDOW_END"):
                return "TIMESTAMP(3)"
            # JSON functions usually return STRING
            if func_name in ("JSON_VALUE", "JSON_QUERY"):
                return "STRING"
            if func_name in ("NOW", "CURRENT_ROW_TIMESTAMP"):
                return "TIMESTAMP_LTZ(3)"

            # Check user-declared UDF return types
            udf_types: dict[str, str] = getattr(self, "_udf_types", {})
            if func_name in udf_types:
                return udf_types[func_name]

        # Current timestamp/time/date functions
        if isinstance(expr, (exp.CurrentTimestamp, exp.CurrentTimestampLTZ)):
            return "TIMESTAMP_LTZ(3)"
        if isinstance(expr, exp.Localtimestamp):
            return "TIMESTAMP(3)"
        if isinstance(expr, (exp.CurrentTime, exp.Localtime)):
            return "TIME(0)"
        if isinstance(expr, exp.CurrentDate):
            return "DATE"

        # Cast - use the target type
        if isinstance(expr, (exp.Cast, exp.TryCast)):
            if expr.to is None:
                return "STRING"
            target_type = expr.to.sql().upper()
            return self._normalize_cast_type(target_type)

        # Paren - unwrap
        if isinstance(expr, exp.Paren):
            return self._infer_expression_type(expr.this, schema)

        # Neg (unary minus) - preserve type
        if isinstance(expr, exp.Neg):
            return self._infer_expression_type(expr.this, schema)

        # Subquery - infer type from the first column of the inner SELECT
        if isinstance(expr, exp.Subquery):
            inner_select = expr.find(exp.Select)
            if inner_select and inner_select.expressions:
                first_expr = inner_select.expressions[0]
                return self._infer_expression_type(first_expr, schema)
            return "STRING"

        # Default to STRING for unknown expressions
        return "STRING"

    # ------------------------------------------------------------------
    # Numeric helpers
    # ------------------------------------------------------------------

    def _is_numeric_type(self, type_name: str) -> bool:
        base_type = type_name.split("(")[0].upper()
        return base_type in {
            "TINYINT",
            "SMALLINT",
            "INT",
            "INTEGER",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "NUMERIC",
        }

    def _widen_decimal_for_aggregate(self, decimal_type: str) -> str:
        """Widen DECIMAL precision to 38 for aggregate functions (SUM/AVG).

        Per Flink SQL rules, SUM(DECIMAL(p,s)) -> DECIMAL(38,s).
        """
        match = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", decimal_type, re.IGNORECASE)
        if match:
            scale = match.group(2)
            return f"DECIMAL(38,{scale})"
        return decimal_type

    def _merge_types(self, types: list[str]) -> str:
        merged = [type_name for type_name in types if type_name and type_name != "NULL"]
        if not merged:
            return "STRING"
        if all(type_name.split("(")[0].upper() == "BOOLEAN" for type_name in merged):
            return "BOOLEAN"
        # Filter out non-numeric before checking numeric-only
        numeric_types = [t for t in merged if self._is_numeric_type(t)]
        non_numeric = [t for t in merged if not self._is_numeric_type(t)]
        if numeric_types and not non_numeric:
            result = numeric_types[0]
            for next_type in numeric_types[1:]:
                result = self._promote_numeric_types(result, next_type)
            return result
        base_types = {type_name.split("(")[0].upper() for type_name in merged}
        if len(base_types) == 1:
            return merged[0]
        # Type widening: if mix of timestamp types, widen to TIMESTAMP
        timestamp_bases = {"TIMESTAMP", "TIMESTAMP_LTZ", "DATE", "TIME"}
        if base_types.issubset(timestamp_bases):
            # Prefer TIMESTAMP_LTZ if any LTZ present, else TIMESTAMP
            if any("LTZ" in t.upper() for t in merged):
                return "TIMESTAMP_LTZ(3)"
            return "TIMESTAMP(3)"
        # If mix of numeric and string, return STRING
        return "STRING"

    def _promote_numeric_types(self, left_type: str, right_type: str) -> str:
        """Promote numeric types following Flink SQL rules."""
        type_order = {
            "TINYINT": 1,
            "SMALLINT": 2,
            "INT": 3,
            "INTEGER": 3,
            "BIGINT": 4,
            "FLOAT": 5,
            "DOUBLE": 6,
            "DECIMAL": 7,
            "NUMERIC": 7,
        }

        # Extract base type (remove precision/scale)
        left_base = left_type.split("(")[0].upper()
        right_base = right_type.split("(")[0].upper()

        left_order = type_order.get(left_base, 0)
        right_order = type_order.get(right_base, 0)

        # If neither is numeric, return STRING
        if left_order == 0 and right_order == 0:
            return "STRING"

        # Return the higher precedence type
        if left_order >= right_order:
            return left_type if left_order > 0 else right_type
        return right_type

    def _infer_division_type(self, left_type: str, right_type: str) -> str:
        left_base = left_type.split("(")[0].upper()
        right_base = right_type.split("(")[0].upper()
        if left_base in ("DECIMAL", "NUMERIC"):
            return left_type
        if right_base in ("DECIMAL", "NUMERIC"):
            return right_type
        if left_base in ("FLOAT", "DOUBLE") or right_base in ("FLOAT", "DOUBLE"):
            return "DOUBLE"
        if left_base in ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT") or right_base in (
            "TINYINT",
            "SMALLINT",
            "INT",
            "INTEGER",
            "BIGINT",
        ):
            return "DOUBLE"
        return "STRING"

    # ------------------------------------------------------------------
    # ML_PREDICT inference
    # ------------------------------------------------------------------

    def _infer_ml_predict_type(self, expr: exp.Anonymous) -> str:
        """Infer the return type for ML_PREDICT/ML_EVALUATE functions.

        Attempts to use declared ml_outputs for precise type inference.
        Falls back to opaque ROW type with a warning if not declared.

        Args:
            expr: The ML_PREDICT or ML_EVALUATE Anonymous expression

        Returns:
            ROW type string with field definitions if ml_outputs declared,
            otherwise generic "ROW" with a warning logged
        """
        # Try to extract the ML model name from the function arguments
        # ML_PREDICT syntax: ML_PREDICT(model_name, input_columns...)
        # The first argument is typically the model reference
        args = list(expr.expressions)
        ml_model_name = None

        if args:
            first_arg = args[0]
            # Model name could be a Column reference, Literal, or other expression
            if isinstance(first_arg, exp.Column):
                ml_model_name = first_arg.name
            elif isinstance(first_arg, exp.Literal) and first_arg.is_string:
                ml_model_name = first_arg.this
            elif isinstance(first_arg, exp.Anonymous) and first_arg.name.upper() == "TABLE":
                # TABLE(model_name) syntax
                inner_args = list(first_arg.expressions)
                if inner_args and hasattr(inner_args[0], "name"):
                    ml_model_name = inner_args[0].name

        # Check if we have ml_outputs declared for this model
        model = getattr(self, "_current_model", None)
        if model and model.ml_outputs and ml_model_name:
            ml_output = model.ml_outputs.get(ml_model_name)
            if ml_output and ml_output.columns:
                # Build ROW type from declared columns
                field_defs = []
                for col in ml_output.columns:
                    col_type = col.type or "STRING"
                    field_defs.append(f"{col.name} {col_type}")
                return f"ROW<{', '.join(field_defs)}>"

        # No ml_outputs declared - log warning and return opaque ROW
        model_name = model.name if model else "unknown"
        ml_model_ref = ml_model_name or "unknown"

        logger.warning(
            f"ML_PREDICT/ML_EVALUATE used in model '{model_name}' without ml_outputs declaration. "
            f"ML model '{ml_model_ref}' output schema is opaque. "
            "Declare ml_outputs in your model configuration to enable:\n"
            "  - Proper type inference for downstream consumers\n"
            "  - Lineage tracking through ML transformations\n"
            "  - Breaking change detection if the ML model schema changes\n"
            "Without ml_outputs, streamt cannot ensure schema compatibility."
        )
        return "ROW"

    # ------------------------------------------------------------------
    # Cast / type normalization
    # ------------------------------------------------------------------

    def _normalize_cast_type(self, target_type: str) -> str:
        # Map SQL standard type aliases to Flink types
        type_aliases = {
            "VARCHAR": "STRING",
            "CHAR": "STRING",
            "CHARACTER": "STRING",
            "TEXT": "STRING",
            "REAL": "FLOAT",
            "NUMBER": "DECIMAL",
        }
        base = target_type.split("(")[0].upper()
        if base in type_aliases:
            return type_aliases[base]
        ltz_match = re.match(r"^TIMESTAMPLTZ(?:\((\d+)\))?$", target_type)
        if ltz_match:
            precision = ltz_match.group(1)
            return f"TIMESTAMP_LTZ({precision})" if precision else "TIMESTAMP_LTZ"
        tz_match = re.match(r"^TIMESTAMPTZ(?:\((\d+)\))?$", target_type)
        if tz_match:
            precision = tz_match.group(1)
            if precision:
                return f"TIMESTAMP({precision}) WITH TIME ZONE"
            return "TIMESTAMP WITH TIME ZONE"
        ntz_match = re.match(r"^TIMESTAMPNTZ(?:\((\d+)\))?$", target_type)
        if ntz_match:
            precision = ntz_match.group(1)
            return f"TIMESTAMP({precision})" if precision else "TIMESTAMP"
        return target_type

    def _normalize_type_whitespace(self, type_name: str) -> str:
        return " ".join(type_name.strip().split())

    # ------------------------------------------------------------------
    # Structured type helpers (ARRAY, MAP, ROW)
    # ------------------------------------------------------------------

    def _split_type_params(self, type_body: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0

        for char in type_body:
            if char == "<":
                depth += 1
                current.append(char)
                continue
            if char == ">":
                depth -= 1
                current.append(char)
                continue
            if char == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
            current.append(char)

        if current:
            parts.append("".join(current).strip())

        return parts

    def _extract_array_element_type(self, type_name: str) -> str | None:
        normalized = self._normalize_type_whitespace(type_name)
        if not normalized.upper().startswith("ARRAY<") or not normalized.endswith(">"):
            return None
        inner = normalized[6:-1].strip()
        return inner or None

    def _extract_map_key_value_types(self, type_name: str) -> tuple[str, str] | None:
        normalized = self._normalize_type_whitespace(type_name)
        if not normalized.upper().startswith("MAP<") or not normalized.endswith(">"):
            return None
        inner = normalized[4:-1].strip()
        parts = self._split_type_params(inner)
        if len(parts) != 2:
            return None
        return parts[0], parts[1]

    def _build_array_type(self, element_type: str) -> str:
        return f"ARRAY<{element_type}>"

    def _build_map_type(self, key_type: str, value_type: str) -> str:
        return f"MAP<{key_type}, {value_type}>"

    def _build_row_type(self, fields: list[tuple[str, str]]) -> str:
        field_defs = ", ".join(f"{name} {field_type}" for name, field_type in fields)
        return f"ROW<{field_defs}>"

    def _infer_array_literal_type(self, expr: exp.Array, schema: dict[str, str]) -> str:
        if not expr.expressions:
            return "ARRAY"
        element_type = self._merge_types(
            [self._infer_expression_type(item, schema) for item in expr.expressions]
        )
        return self._build_array_type(element_type)

    def _infer_array_return_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        input_type = self._infer_expression_type(args[0], schema)
        element_type = self._extract_array_element_type(input_type)
        return input_type if element_type else "ARRAY"

    def _infer_collect_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        element_type = self._infer_expression_type(args[0], schema)
        return self._build_array_type(element_type)

    def _infer_element_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "STRING"
        input_type = self._infer_expression_type(args[0], schema)
        element_type = self._extract_array_element_type(input_type)
        return element_type or "STRING"

    def _infer_map_keys_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        map_type = self._infer_expression_type(args[0], schema)
        key_value = self._extract_map_key_value_types(map_type)
        if key_value:
            return self._build_array_type(key_value[0])
        return "ARRAY"

    def _infer_map_values_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        map_type = self._infer_expression_type(args[0], schema)
        key_value = self._extract_map_key_value_types(map_type)
        if key_value:
            return self._build_array_type(key_value[1])
        return "ARRAY"

    def _infer_map_entries_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        map_type = self._infer_expression_type(args[0], schema)
        key_value = self._extract_map_key_value_types(map_type)
        if key_value:
            row_type = self._build_row_type([("key", key_value[0]), ("value", key_value[1])])
            return self._build_array_type(row_type)
        return "ARRAY"

    def _infer_map_union_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "MAP"
        map_type = self._infer_expression_type(args[0], schema)
        if self._extract_map_key_value_types(map_type):
            return map_type
        return "MAP"

    def _infer_map_from_arrays_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if len(args) < 2:
            return "MAP"
        keys_type = self._infer_expression_type(args[0], schema)
        values_type = self._infer_expression_type(args[1], schema)
        key_element = self._extract_array_element_type(keys_type)
        value_element = self._extract_array_element_type(values_type)
        if key_element and value_element:
            return self._build_map_type(key_element, value_element)
        return "MAP"

    # ------------------------------------------------------------------
    # Regex-based fallback extraction
    # ------------------------------------------------------------------

    def _extract_select_columns_with_types_regex(
        self, sql: str, schema: dict[str, str]
    ) -> list[tuple[str, str]]:
        """Fallback regex-based extraction when sqlglot fails."""
        # Remove Jinja templates first
        clean_sql = re.sub(r'\{\{.*?\}\}', 'placeholder', sql)

        # Match SELECT ... FROM
        match = re.search(r'SELECT\s+(.+?)\s+FROM', clean_sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return []

        select_clause = match.group(1)

        # Handle SELECT *
        if select_clause.strip() == '*':
            return []

        columns = []
        parts = self._split_select_columns(select_clause)

        for part in parts:
            part = part.strip()
            column_name = None
            column_type = "STRING"

            if ' AS ' in part.upper():
                alias_match = re.search(r'\s+AS\s+[`"]?(\w+)[`"]?\s*$', part, re.IGNORECASE)
                if alias_match:
                    column_name = alias_match.group(1)
                    expr = part[:part.upper().rfind(' AS ')].strip()
                    column_type = self._infer_flink_type_regex(expr, schema)
            else:
                col_match = re.match(r'^[`"]?(\w+)[`"]?$', part)
                if col_match:
                    column_name = col_match.group(1)
                    column_type = schema.get(column_name, "STRING")

            if column_name:
                columns.append((column_name, column_type))

        return columns

    def _split_select_columns(self, select_clause: str) -> list[str]:
        """Split SELECT clause into columns, respecting nested parentheses."""
        parts = []
        current = []
        depth = 0

        for char in select_clause:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current).strip())
                current = []
            else:
                current.append(char)

        if current:
            parts.append(''.join(current).strip())

        return parts

    def _infer_flink_type_regex(self, expr: str, schema: dict[str, str]) -> str:
        """Infer Flink SQL type from an expression using regex (fallback)."""
        expr_upper = expr.upper().strip()

        # Boolean: CASE WHEN with TRUE/FALSE
        if 'CASE' in expr_upper and ('THEN TRUE' in expr_upper or 'THEN FALSE' in expr_upper):
            return "BOOLEAN"

        # Aggregate functions that return BIGINT
        if re.match(r'^COUNT\s*\(', expr_upper):
            return "BIGINT"

        sum_match = re.match(r'^SUM\s*\((.+)\)$', expr_upper)
        if sum_match:
            arg = sum_match.group(1).strip()
            col_match = re.match(r'^[`"]?(\w+)[`"]?$', arg)
            if col_match:
                col_type = schema.get(col_match.group(1), "DOUBLE")
                base_type = col_type.split("(")[0].upper()
                if base_type in ("DECIMAL", "NUMERIC"):
                    return col_type
                if base_type in ("FLOAT", "DOUBLE"):
                    return "DOUBLE"
                if base_type in ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"):
                    return "BIGINT"
            return "DOUBLE"

        avg_match = re.match(r'^AVG\s*\((.+)\)$', expr_upper)
        if avg_match:
            arg = avg_match.group(1).strip()
            col_match = re.match(r'^[`"]?(\w+)[`"]?$', arg)
            if col_match:
                col_type = schema.get(col_match.group(1), "DOUBLE")
                base_type = col_type.split("(")[0].upper()
                if base_type in ("DECIMAL", "NUMERIC"):
                    return col_type
            return "DOUBLE"

        min_max_match = re.match(r'^(MIN|MAX)\s*\((.+)\)$', expr_upper)
        if min_max_match:
            arg = min_max_match.group(2).strip()
            col_match = re.match(r'^[`"]?(\w+)[`"]?$', arg)
            if col_match:
                return schema.get(col_match.group(1), "STRING")
            return "DOUBLE"

        # Window functions that return TIMESTAMP
        if re.match(
            r'^(TUMBLE_START|TUMBLE_END|TUMBLE_ROWTIME|HOP_START|HOP_END|HOP_ROWTIME|SESSION_START|SESSION_END|SESSION_ROWTIME|WINDOW_START|WINDOW_END)\s*\(',
            expr_upper,
        ):
            return "TIMESTAMP(3)"
        if re.match(r'^(TUMBLE_PROCTIME|HOP_PROCTIME|SESSION_PROCTIME)\s*\(', expr_upper):
            return "TIMESTAMP_LTZ(3)"

        # String functions
        if re.match(r'^(UPPER|LOWER|CONCAT|SUBSTRING|TRIM|LTRIM|RTRIM|REPLACE|REGEXP_REPLACE)\s*\(', expr_upper):
            return "STRING"

        # PROCTIME()
        if expr_upper.startswith('PROCTIME('):
            return "TIMESTAMP_LTZ(3)"

        if (
            expr_upper.startswith("CURRENT_TIMESTAMP")
            or expr_upper.startswith("NOW(")
            or expr_upper.startswith("CURRENT_ROW_TIMESTAMP(")
        ):
            return "TIMESTAMP_LTZ(3)"

        # Numeric literals
        if re.match(r'^-?\d+$', expr.strip()):
            return "INT"
        if re.match(r'^-?\d+\.\d*$', expr.strip()):
            return "DOUBLE"

        # Simple column reference - look up in schema
        col_match = re.match(r'^[`"]?(\w+)[`"]?$', expr.strip())
        if col_match:
            col_name = col_match.group(1)
            if col_name in schema:
                return schema[col_name]
            upper_name = col_name.upper()
            if upper_name == "$ROWTIME":
                return "TIMESTAMP_LTZ(3)"
            if upper_name == "ROWTIME":
                return "TIMESTAMP(3)"
            if upper_name in ("$PROCTIME", "PROCTIME"):
                return "TIMESTAMP_LTZ(3)"
            if upper_name in ("WINDOW_START", "WINDOW_END", "WINDOW_TIME"):
                return "TIMESTAMP(3)"
            return "STRING"

        # Default to STRING for unknown expressions
        return "STRING"
