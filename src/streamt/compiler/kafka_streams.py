"""Provider-free compiler and strict wire contract for the bounded JSON runner.

Plan v1 preserves the isolated execution proof's closed SQL semantics. It is
neither Flink SQL nor a general Kafka Streams topology description.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import TypedDict, cast

import sqlglot
from sqlglot import exp

NAME = re.compile(r"[a-z_][a-z0-9_]*\Z")
TOPIC = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,248}\Z")
TYPES = frozenset({"STRING", "BOOLEAN", "BIGINT"})
MAX_RUNNER_INPUT_BYTES = 1_048_576
OPS: dict[type[exp.Expression], str] = {
    exp.EQ: "eq", exp.NEQ: "ne", exp.GT: "gt", exp.GTE: "ge", exp.LT: "lt", exp.LTE: "le",
}


class FieldSchema(TypedDict):
    type: str
    nullable: bool


class KafkaStreamsPlanError(ValueError):
    """A declaration or plan is outside the runner's exact capabilities."""


def application_id(project: str, environment: str, model: str) -> str:
    """Stable identity independent of SQL, image, paths, or Python hash seeds."""
    parts = (project, environment, model)
    if any(type(part) is not str or not part.strip() for part in parts):
        raise KafkaStreamsPlanError("Application identity requires project, environment, and model")
    payload = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return "streamt-" + hashlib.sha256(payload).hexdigest()[:32]


def validate_schema(value: object) -> dict[str, FieldSchema]:
    """Copy one explicit schema, rejecting coercion and unknown record fields."""
    if type(value) is not dict or not value:
        raise KafkaStreamsPlanError("Kafka Streams requires nonempty declared columns for raw JSON")
    result: dict[str, FieldSchema] = {}
    for name, field in value.items():
        if (
            type(name) is not str or not NAME.fullmatch(name)
            or type(field) is not dict or set(field) != {"type", "nullable"}
            or type(field["type"]) is not str or field["type"] not in TYPES
            or type(field["nullable"]) is not bool
        ):
            raise KafkaStreamsPlanError("Kafka Streams columns require lowercase names and explicit STRING, BOOLEAN, or BIGINT types")
        result[name] = {"type": field["type"], "nullable": field["nullable"]}
    return result


def validate_plan(value: object) -> dict[str, object]:
    """Independently validate a loaded plan; never trust its compiler provenance."""
    if type(value) is not dict or set(value) != {
        "version", "input_topic", "output_topic", "schema", "projection", "predicates",
    }:
        raise KafkaStreamsPlanError("Kafka Streams plan must contain exactly the version 1 fields")
    if type(value["version"]) is not int or value["version"] != 1:
        raise KafkaStreamsPlanError("Kafka Streams plan version must be 1")
    for key in ("input_topic", "output_topic"):
        topic = value[key]
        if type(topic) is not str or not TOPIC.fullmatch(topic) or topic.startswith("__"):
            raise KafkaStreamsPlanError("Invalid or internal Kafka topic in runner plan")
    if value["input_topic"] == value["output_topic"]:
        raise KafkaStreamsPlanError("Kafka Streams input and output topics must differ")
    schema = validate_schema(value["schema"])
    projection = value["projection"]
    if type(projection) is not list or not projection:
        raise KafkaStreamsPlanError("Kafka Streams plan requires a nonempty projection")
    aliases: set[str] = set()
    projected: list[dict[str, str]] = []
    for item in projection:
        if (
            type(item) is not dict or set(item) != {"column", "as"}
            or type(item["column"]) is not str or item["column"] not in schema
            or type(item["as"]) is not str or not NAME.fullmatch(item["as"])
            or item["as"] in aliases
        ):
            raise KafkaStreamsPlanError("Kafka Streams plan has an invalid or duplicate projection")
        aliases.add(item["as"])
        projected.append({"column": item["column"], "as": item["as"]})
    predicates = value["predicates"]
    if type(predicates) is not list:
        raise KafkaStreamsPlanError("Kafka Streams predicates must be a list")
    copied_predicates: list[dict[str, object]] = []
    for predicate in predicates:
        if type(predicate) is not dict:
            raise KafkaStreamsPlanError("Kafka Streams predicate must be an object")
        name, op = predicate.get("column"), predicate.get("op")
        if type(name) is not str or name not in schema or type(op) is not str:
            raise KafkaStreamsPlanError("Kafka Streams predicate has an invalid column or operator")
        kind = schema[name]["type"]
        if op in {"is_null", "not_null"}:
            if set(predicate) != {"column", "op"}:
                raise KafkaStreamsPlanError("Kafka Streams null predicate has unsupported fields")
        else:
            if set(predicate) != {"column", "op", "value"} or op not in set(OPS.values()):
                raise KafkaStreamsPlanError("Kafka Streams predicate has unsupported fields or operator")
            literal = predicate["value"]
            valid_literal = (
                (kind == "STRING" and type(literal) is str)
                or (kind == "BOOLEAN" and type(literal) is bool)
                or (kind == "BIGINT" and type(literal) is int and -(2**63) <= literal < 2**63)
            )
            if not valid_literal or (op not in {"eq", "ne"} and kind != "BIGINT"):
                raise KafkaStreamsPlanError("Kafka Streams predicate literal does not match its exact type")
        copied_predicates.append(dict(predicate))
    result: dict[str, object] = {
        "version": 1,
        "input_topic": value["input_topic"],
        "output_topic": value["output_topic"],
        "schema": schema,
        "projection": projected,
        "predicates": copied_predicates,
    }
    if len(json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=True)) + 1 > MAX_RUNNER_INPUT_BYTES:
        raise KafkaStreamsPlanError("Kafka Streams plan exceeds the fixed runner's 1 MiB limit")
    return result


def output_schema(plan: object) -> dict[str, FieldSchema]:
    validated = validate_plan(plan)
    schema = cast(dict[str, FieldSchema], validated["schema"])
    projection = cast(list[dict[str, str]], validated["projection"])
    return {
        item["as"]: {
            "type": schema[item["column"]]["type"],
            "nullable": schema[item["column"]]["nullable"],
        }
        for item in projection
    }


def _only(node: exp.Expression, allowed: set[str]) -> None:
    if any(
        value is not None and value is not False and value != []
        for key, value in node.args.items() if key not in allowed
    ):
        raise KafkaStreamsPlanError(f"Unsupported {node.key} options")


def _identifier(node: exp.Expression | None) -> str:
    if not isinstance(node, exp.Identifier) or node.args.get("quoted") or not NAME.fullmatch(node.name):
        raise KafkaStreamsPlanError("Identifiers must be unquoted lowercase ASCII names")
    _only(node, {"this", "quoted"})
    return node.name


def _column(node: exp.Expression, schema: dict[str, FieldSchema]) -> str:
    if not isinstance(node, exp.Column):
        raise KafkaStreamsPlanError("Only direct column references are supported")
    _only(node, {"this"})
    name = _identifier(node.this)
    if name not in schema:
        raise KafkaStreamsPlanError(f"Unknown column: {name}")
    return name


def _literal(node: exp.Expression, kind: str) -> str | bool | int:
    if kind == "BOOLEAN" and isinstance(node, exp.Boolean):
        return bool(node.this)
    if kind == "STRING" and isinstance(node, exp.Literal) and node.is_string:
        return str(node.this)
    sign = 1
    if isinstance(node, exp.Neg):
        sign, node = -1, node.this
    if kind == "BIGINT" and isinstance(node, exp.Literal) and not node.is_string:
        if re.fullmatch(r"[0-9]+", node.this):
            value = sign * int(node.this)
            if -(2**63) <= value < 2**63:
                return value
    raise KafkaStreamsPlanError(f"Expected a {kind} literal; casts and implicit coercion are unsupported")


def _predicates(node: exp.Expression, schema: dict[str, FieldSchema]) -> list[dict[str, object]]:
    if isinstance(node, exp.And):
        return _predicates(node.this, schema) + _predicates(node.expression, schema)
    negated = isinstance(node, exp.Not)
    check = node.this if negated else node
    if isinstance(check, exp.Is) and isinstance(check.expression, exp.Null):
        return [{"column": _column(check.this, schema), "op": "not_null" if negated else "is_null"}]
    if type(node) not in OPS:
        raise KafkaStreamsPlanError(f"Unsupported predicate: {node.key}")
    name = _column(node.this, schema)
    kind = schema[name]["type"]
    op = OPS[type(node)]
    if op not in {"eq", "ne"} and kind != "BIGINT":
        raise KafkaStreamsPlanError("Ordering comparisons require BIGINT")
    return [{"column": name, "op": op, "value": _literal(node.expression, kind)}]


def compile_plan(
    sql: str, schema: dict[str, FieldSchema], source: str, input_topic: str, output_topic: str
) -> dict[str, object]:
    """Compile exactly the supported stateless projection/filter subset."""
    if type(sql) is not str or type(source) is not str or not NAME.fullmatch(source):
        raise KafkaStreamsPlanError("SQL and a lowercase source name are required")
    schema = validate_schema(schema)
    try:
        statements = sqlglot.parse(sql)
    except sqlglot.errors.ParseError as error:
        raise KafkaStreamsPlanError("Invalid SQL") from error
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise KafkaStreamsPlanError("Exactly one SELECT is supported")
    query = statements[0]
    if any(node.comments for node in query.walk()):
        raise KafkaStreamsPlanError("SQL comments and hints are outside the Kafka Streams subset")
    _only(query, {"expressions", "from_", "where"})
    from_clause = query.args.get("from_")
    if not isinstance(from_clause, exp.From) or not isinstance(from_clause.this, exp.Table):
        raise KafkaStreamsPlanError("Exactly one source table is required")
    _only(from_clause, {"this"})
    _only(from_clause.this, {"this"})
    if _identifier(from_clause.this.this) != source:
        raise KafkaStreamsPlanError(f"Expected source: {source}")
    projections = []
    for item in query.expressions:
        if isinstance(item, exp.Alias):
            _only(item, {"this", "alias"})
            alias = _identifier(item.args["alias"])
            name = _column(item.this, schema)
        else:
            name = alias = _column(item, schema)
        projections.append({"column": name, "as": alias})
    where = query.args.get("where")
    return validate_plan({
        "version": 1,
        "input_topic": input_topic,
        "output_topic": output_topic,
        "schema": schema,
        "projection": projections,
        "predicates": _predicates(where.this, schema) if where else [],
    })
