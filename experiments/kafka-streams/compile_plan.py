"""Compile the deliberately small SQL subset used by this isolated experiment."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import TypedDict

import sqlglot
from sqlglot import exp

NAME = re.compile(r"[a-z_][a-z0-9_]*\Z")
TOPIC = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,248}\Z")
TYPES = {"STRING", "BOOLEAN", "BIGINT"}
OPS: dict[type[exp.Expression], str] = {
    exp.EQ: "eq",
    exp.NEQ: "ne",
    exp.GT: "gt",
    exp.GTE: "ge",
    exp.LT: "lt",
    exp.LTE: "le",
}


class FieldSchema(TypedDict):
    type: str
    nullable: bool


class UnsupportedSQLError(ValueError):
    """The proof has no execution semantics for this construct."""


def _only(node: exp.Expression, allowed: set[str]) -> None:
    if any(
        value is not None and value is not False and value != []
        for key, value in node.args.items()
        if key not in allowed
    ):
        raise UnsupportedSQLError(f"Unsupported {node.key} options")


def _identifier(node: exp.Expression | None) -> str:
    if (
        not isinstance(node, exp.Identifier)
        or node.args.get("quoted")
        or not NAME.fullmatch(node.name)
    ):
        raise UnsupportedSQLError("Identifiers must be unquoted lowercase ASCII names")
    _only(node, {"this", "quoted"})
    return node.name


def _column(node: exp.Expression, schema: dict[str, FieldSchema]) -> str:
    if not isinstance(node, exp.Column):
        raise UnsupportedSQLError("Only direct column references are supported")
    _only(node, {"this"})
    name = _identifier(node.this)
    if name not in schema:
        raise UnsupportedSQLError(f"Unknown column: {name}")
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
    raise UnsupportedSQLError(
        f"Expected a {kind} literal; casts and implicit coercion are unsupported"
    )


def _predicates(node: exp.Expression, schema: dict[str, FieldSchema]) -> list[dict[str, object]]:
    if isinstance(node, exp.And):
        return _predicates(node.this, schema) + _predicates(node.expression, schema)
    negated = isinstance(node, exp.Not)
    check = node.this if negated else node
    if isinstance(check, exp.Is) and isinstance(check.expression, exp.Null):
        return [{"column": _column(check.this, schema), "op": "not_null" if negated else "is_null"}]
    if type(node) not in OPS:
        raise UnsupportedSQLError(f"Unsupported predicate: {node.key}")
    name = _column(node.this, schema)
    kind = schema[name]["type"]
    op = OPS[type(node)]
    if op not in {"eq", "ne"} and kind != "BIGINT":
        raise UnsupportedSQLError("Ordering comparisons require BIGINT")
    return [{"column": name, "op": op, "value": _literal(node.expression, kind)}]


def compile_plan(
    sql: str, schema: dict[str, FieldSchema], source: str, input_topic: str, output_topic: str
) -> dict[str, object]:
    if not NAME.fullmatch(source) or not isinstance(schema, dict) or not schema:
        raise UnsupportedSQLError("A lowercase source name and nonempty schema are required")
    for name, field in schema.items():
        if (
            not NAME.fullmatch(name)
            or not isinstance(field, dict)
            or set(field) != {"type", "nullable"}
            or not isinstance(field["type"], str)
            or field["type"] not in TYPES
            or type(field["nullable"]) is not bool
        ):
            raise UnsupportedSQLError(f"Invalid schema field: {name}")
    if any(
        not TOPIC.fullmatch(topic) or topic.startswith("__")
        for topic in (input_topic, output_topic)
    ):
        raise UnsupportedSQLError("Invalid or internal Kafka topic")
    if input_topic == output_topic:
        raise UnsupportedSQLError("Input and output topics must differ")
    try:
        statements = sqlglot.parse(sql)
    except sqlglot.errors.ParseError as error:
        raise UnsupportedSQLError("Invalid SQL") from error
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise UnsupportedSQLError("Exactly one SELECT is supported")
    query = statements[0]
    if any(node.comments for node in query.walk()):
        raise UnsupportedSQLError("SQL comments and hints are outside this proof")
    _only(query, {"expressions", "from_", "where"})
    from_clause = query.args.get("from_")
    if not isinstance(from_clause, exp.From) or not isinstance(from_clause.this, exp.Table):
        raise UnsupportedSQLError("Exactly one source table is required")
    _only(from_clause, {"this"})
    _only(from_clause.this, {"this"})
    if _identifier(from_clause.this.this) != source:
        raise UnsupportedSQLError(f"Expected source: {source}")
    projections = []
    aliases: set[str] = set()
    for item in query.expressions:
        if isinstance(item, exp.Alias):
            _only(item, {"this", "alias"})
            alias = _identifier(item.args["alias"])
            name = _column(item.this, schema)
        else:
            name = alias = _column(item, schema)
        if alias in aliases:
            raise UnsupportedSQLError(f"Duplicate output column: {alias}")
        aliases.add(alias)
        projections.append({"column": name, "as": alias})
    if not projections:
        raise UnsupportedSQLError("At least one output column is required")
    where = query.args.get("where")
    predicates = _predicates(where.this, schema) if where else []
    return {
        "version": 1,
        "input_topic": input_topic,
        "output_topic": output_topic,
        "schema": schema,
        "projection": projections,
        "predicates": predicates,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sql", type=Path)
    parser.add_argument("schema", type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--input-topic", required=True)
    parser.add_argument("--output-topic", required=True)
    args = parser.parse_args()
    try:
        plan = compile_plan(
            args.sql.read_text(),
            json.loads(args.schema.read_text()),
            args.source,
            args.input_topic,
            args.output_topic,
        )
    except (UnsupportedSQLError, OSError, json.JSONDecodeError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(plan, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()
