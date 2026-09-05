"""Offline rejection tests: compiling SQL has no Kafka dependency or client."""

import json
from pathlib import Path

import pytest
from compile_plan import UnsupportedSQLError, compile_plan

SCHEMA = json.loads((Path(__file__).parent / "examples/schema.json").read_text())


def compile_sql(sql):
    return compile_plan(sql, SCHEMA, "orders", "proof.input", "proof.output")


def test_example_and_update_share_shape():
    folder = Path(__file__).parent / "examples"
    original = compile_sql((folder / "orders.sql").read_text())
    changed = compile_sql((folder / "orders_updated.sql").read_text())
    assert original["predicates"][0] == {"column": "amount", "op": "ge", "value": 50}
    assert changed["predicates"][0]["value"] == 100
    assert original["projection"] == [
        {"column": "id", "as": "order_id"},
        {"column": "amount", "as": "amount"},
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM orders",
        "SELECT id FROM orders LIMIT 1",
        "SELECT DISTINCT id FROM orders",
        "SELECT COUNT(id) FROM orders",
        "SELECT id FROM orders GROUP BY id",
        "SELECT id FROM orders ORDER BY id",
        "SELECT id FROM orders UNION SELECT id FROM orders",
        "SELECT id FROM orders, orders b",
        "SELECT id FROM orders JOIN other ON orders.id = other.id",
        "SELECT id + 1 FROM orders",
        "SELECT UPPER(id) FROM orders",
        "SELECT CAST(amount AS STRING) FROM orders",
        "SELECT id FROM orders WHERE active = TRUE OR amount > 1",
        "SELECT id FROM orders WHERE NOT active",
        "SELECT id FROM orders WHERE amount IN (1, 2)",
        "SELECT id FROM orders WHERE amount BETWEEN 1 AND 2",
        "SELECT id FROM orders WHERE id LIKE 'a%'",
        "SELECT id FROM orders WHERE amount = NULL",
        "SELECT id FROM orders WHERE amount = '1'",
        "SELECT id FROM orders WHERE active = 1",
        "SELECT id FROM orders WHERE id > 'a'",
        "SELECT id FROM orders WHERE amount = 1.5",
        "SELECT id FROM orders WHERE amount = 9223372036854775808",
        "SELECT id FROM orders WHERE amount = -9223372036854775809",
        "SELECT id FROM orders WHERE amount = amount",
        "SELECT other FROM orders",
        "SELECT id FROM unknown",
        "SELECT id FROM db.orders",
        "SELECT orders.id FROM orders",
        'SELECT "id" FROM orders',
        "SELECT ID FROM orders",
        "SELECT id FROM orders o",
        "SELECT id, amount AS id FROM orders",
        "SELECT 1",
        "SELECT id FROM (SELECT id FROM orders)",
        "WITH o AS (SELECT id FROM orders) SELECT id FROM o",
        "DELETE FROM orders",
        "SELECT id FROM orders; SELECT id FROM orders",
        "SELECT id FROM orders -- comment",
        "SELECT id FROM orders WHERE (amount > 1)",
        "SELECT id FROM orders WHERE amount / 2 > 1",
    ],
)
def test_rejects_sql_outside_proof(sql):
    with pytest.raises(UnsupportedSQLError):
        compile_sql(sql)


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        ("amount IS NULL", {"column": "amount", "op": "is_null"}),
        ("amount IS NOT NULL", {"column": "amount", "op": "not_null"}),
        ("amount = -9223372036854775808", {"column": "amount", "op": "eq", "value": -(2**63)}),
        ("id = 'été'", {"column": "id", "op": "eq", "value": "été"}),
        ("active != FALSE", {"column": "active", "op": "ne", "value": False}),
    ],
)
def test_supported_predicates(predicate, expected):
    assert compile_sql(f"SELECT id FROM orders WHERE {predicate}")["predicates"] == [expected]


@pytest.mark.parametrize(
    ("input_topic", "output_topic"),
    [
        ("same", "same"),
        ("__internal", "out"),
        ("in", "__internal"),
        ("bad topic", "out"),
        ("", "out"),
    ],
)
def test_rejects_unsafe_topics(input_topic, output_topic):
    with pytest.raises(UnsupportedSQLError):
        compile_plan("SELECT id FROM orders", SCHEMA, "orders", input_topic, output_topic)


@pytest.mark.parametrize(
    "schema",
    [
        {},
        {"id": {"type": "DECIMAL", "nullable": False}},
        {"id": {"type": "STRING"}},
        {"id": {"type": "STRING", "nullable": "false"}},
    ],
)
def test_rejects_unsupported_schemas(schema):
    with pytest.raises(UnsupportedSQLError):
        compile_plan("SELECT id FROM orders", schema, "orders", "in", "out")
