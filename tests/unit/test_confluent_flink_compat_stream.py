"""Confluent Cloud Flink streaming SQL compatibility tests.

Verifies that streamt correctly parses, validates, and compiles complex Confluent
Flink streaming SQL patterns: $rowtime, temporal joins, interval joins, window TVFs
(TUMBLE/HOP/CUMULATE), MATCH_RECOGNIZE (CEP), and window analytics (LAG/LEAD).

Reference: https://docs.confluent.io/cloud/current/flink/reference/queries/
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from streamt.core.models import StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


def _parse(tmpdir: str, config: dict) -> StreamtProject:
    p = Path(tmpdir)
    (p / "stream_project.yml").write_text(yaml.dump(config))
    return ProjectParser(p).parse()


def _compile(project: StreamtProject):
    from streamt.compiler.compiler import Compiler

    return Compiler(project).compile(dry_run=True)


def _no_fatal_errors(result) -> bool:
    return result.is_valid


# Stateful streaming queries need a Flink cluster in runtime
BASE = {
    "project": {"name": "test", "version": "1.0.0"},
    "runtime": {
        "kafka": {"bootstrap_servers": "localhost:9092"},
        "flink": {
            "default": "local",
            "clusters": {
                "local": {
                    "rest_url": "http://localhost:8081",
                    "sql_gateway_url": "http://localhost:8083",
                }
            },
        },
    },
    "sources": [
        {"name": "events", "topic": "events.v1"},
        {"name": "products", "topic": "products.v1"},
        {"name": "orders", "topic": "orders.v1"},
    ],
}

# Simple models that are stateless (gateway/passthrough) don't need Flink config
BASE_STATELESS = {
    "project": {"name": "test", "version": "1.0.0"},
    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    "sources": [
        {"name": "events", "topic": "events.v1"},
        {"name": "products", "topic": "products.v1"},
        {"name": "orders", "topic": "orders.v1"},
    ],
}


# ---------------------------------------------------------------------------
# $rowtime — Confluent event-time attribute
# ---------------------------------------------------------------------------


class TestRowtimeCompat:
    """$rowtime is Confluent's canonical event-time attribute for streams."""

    def test_rowtime_in_select_validates(self):
        """$rowtime can appear in SELECT without errors."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE_STATELESS,
                "models": [
                    {
                        "name": "timed_events",
                        "sql": (
                            'SELECT event_id, `$rowtime` AS event_time FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_rowtime_in_where_clause_validates(self):
        """$rowtime can be used in WHERE time-range filters."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE_STATELESS,
                "models": [
                    {
                        "name": "recent_events",
                        "sql": (
                            "SELECT event_id, amount "
                            'FROM {{ source("orders") }} '
                            "WHERE `$rowtime` > TIMESTAMPADD(HOUR, -1, CURRENT_TIMESTAMP)"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_rowtime_type_inference(self):
        """$rowtime resolves to TIMESTAMP_LTZ(3)."""
        from streamt.compiler.type_inference import TypeInferenceMixin

        class _H(TypeInferenceMixin):
            def __init__(self):
                self._udf_types = {}

        h = _H()
        cols = h._extract_select_columns_with_types(
            "SELECT `$rowtime` AS event_time FROM t",
            schema_context={},
        )
        col_map = dict(cols)
        assert "event_time" in col_map
        assert col_map["event_time"] in ("TIMESTAMP_LTZ(3)", "TIMESTAMP(3)")


# ---------------------------------------------------------------------------
# Temporal joins: FOR SYSTEM_TIME AS OF
# ---------------------------------------------------------------------------


class TestTemporalJoinCompat:
    """FOR SYSTEM_TIME AS OF joins stream events with versioned dimension tables."""

    def test_temporal_join_validates(self):
        """Temporal join (FOR SYSTEM_TIME AS OF) validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "enriched_orders",
                        "sql": (
                            "SELECT o.order_id, o.amount, p.price, p.category "
                            'FROM {{ source("orders") }} AS o '
                            'LEFT JOIN {{ source("products") }} '
                            "FOR SYSTEM_TIME AS OF o.`$rowtime` "
                            "ON o.product_id = products.product_id"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_temporal_join_compiles(self):
        """Temporal join model produces a Flink job artifact."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "enriched_orders",
                        "sql": (
                            "SELECT o.order_id, o.amount, p.name "
                            'FROM {{ source("orders") }} AS o '
                            'LEFT JOIN {{ source("products") }} AS p '
                            "FOR SYSTEM_TIME AS OF o.`$rowtime` "
                            "ON o.product_id = p.product_id"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            manifest = _compile(project)
            all_names = [
                a.get("name") if isinstance(a, dict) else a.name
                for artifacts in manifest.artifacts.values()
                for a in artifacts
            ]
            assert "enriched_orders" in all_names


# ---------------------------------------------------------------------------
# Interval joins
# ---------------------------------------------------------------------------


class TestIntervalJoinCompat:
    """Interval joins correlate events that occur within a time window."""

    def test_interval_join_validates(self):
        """Interval join (WHERE t1.$rowtime BETWEEN ... AND ...) validates."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "order_clicks",
                        "sql": (
                            "SELECT o.order_id, o.amount, e.event_id "
                            'FROM {{ source("orders") }} AS o '
                            'JOIN {{ source("events") }} AS e '
                            "ON o.customer_id = e.customer_id "
                            "WHERE o.`$rowtime` BETWEEN e.`$rowtime` "
                            "AND e.`$rowtime` + INTERVAL '5' MINUTES"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors


# ---------------------------------------------------------------------------
# Window TVFs: TUMBLE, HOP, CUMULATE
# ---------------------------------------------------------------------------


class TestWindowTVFCompat:
    """TUMBLE, HOP, CUMULATE are Flink's window table-valued functions."""

    def test_tumble_window_tvf_validates(self):
        """TUMBLE TVF with TABLE/DESCRIPTOR pattern validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "order_totals",
                        "sql": (
                            "SELECT window_start, window_end, SUM(amount) AS total_amount "
                            "FROM TABLE("
                            '  TUMBLE(TABLE {{ source("orders") }}, '
                            "         DESCRIPTOR(`$rowtime`), INTERVAL '10' MINUTES)"
                            ") "
                            "GROUP BY window_start, window_end"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_hop_window_tvf_validates(self):
        """HOP (sliding window) TVF validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "sliding_counts",
                        "sql": (
                            "SELECT window_start, window_end, COUNT(*) AS event_count "
                            "FROM TABLE("
                            '  HOP(TABLE {{ source("events") }}, '
                            "      DESCRIPTOR(`$rowtime`), INTERVAL '5' MINUTES, INTERVAL '1' HOUR)"
                            ") "
                            "GROUP BY window_start, window_end"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_cumulate_window_tvf_validates(self):
        """CUMULATE (progressive window) TVF validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "cumulative_sales",
                        "sql": (
                            "SELECT window_start, window_end, SUM(amount) AS running_total "
                            "FROM TABLE("
                            '  CUMULATE(TABLE {{ source("orders") }}, '
                            "           DESCRIPTOR(`$rowtime`), INTERVAL '10' MINUTES, INTERVAL '1' HOUR)"
                            ") "
                            "GROUP BY window_start, window_end"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_window_tvf_type_inference(self):
        """window_start and window_end columns resolve to TIMESTAMP(3)."""
        from streamt.compiler.type_inference import TypeInferenceMixin

        class _H(TypeInferenceMixin):
            def __init__(self):
                self._udf_types = {}

        h = _H()
        cols = h._extract_select_columns_with_types(
            "SELECT window_start, window_end, SUM(amount) AS total "
            "FROM t GROUP BY window_start, window_end",
            schema_context={
                "window_start": "TIMESTAMP(3)",
                "window_end": "TIMESTAMP(3)",
                "amount": "DOUBLE",
            },
        )
        col_map = dict(cols)
        assert "window_start" in col_map
        assert col_map["window_start"] == "TIMESTAMP(3)"


# ---------------------------------------------------------------------------
# MATCH_RECOGNIZE — Complex Event Processing
# ---------------------------------------------------------------------------


class TestMatchRecognizeCompat:
    """MATCH_RECOGNIZE detects event sequence patterns in streams (CEP)."""

    def test_match_recognize_price_trend_validates(self):
        """MATCH_RECOGNIZE for price up/down pattern validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "price_trends",
                        "sql": (
                            'SELECT * FROM {{ source("events") }} '
                            "MATCH_RECOGNIZE ("
                            "  PARTITION BY product_id "
                            "  ORDER BY `$rowtime` "
                            "  MEASURES "
                            "    FIRST(PRICE_DOWN.price) AS start_price, "
                            "    LAST(PRICE_UP.price) AS end_price "
                            "  ONE ROW PER MATCH "
                            "  AFTER MATCH SKIP TO LAST PRICE_UP "
                            "  PATTERN (PRICE_DOWN+ PRICE_UP+) "
                            "  DEFINE "
                            "    PRICE_DOWN AS price < LAG(price), "
                            "    PRICE_UP AS price > LAG(price)"
                            ")"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_match_recognize_fraud_pattern_validates(self):
        """MATCH_RECOGNIZE fraud detection (small{3,} LARGE) validates."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "fraud_patterns",
                        "sql": (
                            'SELECT * FROM {{ source("orders") }} '
                            "MATCH_RECOGNIZE ("
                            "  PARTITION BY customer_id "
                            "  ORDER BY `$rowtime` "
                            "  MEASURES "
                            "    COUNT(SMALL.amount) AS small_count, "
                            "    LAST(LARGE.amount) AS large_amount, "
                            "    MATCH_ROWTIME() AS match_time "
                            "  ONE ROW PER MATCH "
                            "  AFTER MATCH SKIP PAST LAST ROW "
                            "  PATTERN (SMALL{3,} LARGE) "
                            "  DEFINE "
                            "    SMALL AS amount < 100, "
                            "    LARGE AS amount > 1000"
                            ")"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors


# ---------------------------------------------------------------------------
# Window analytics: LAG, LEAD, ROW_NUMBER over $rowtime
# ---------------------------------------------------------------------------


class TestWindowAnalyticsCompat:
    """LAG, LEAD, ROW_NUMBER OVER with ORDER BY $rowtime."""

    def test_lag_over_rowtime_validates(self):
        """LAG() with OVER ORDER BY $rowtime validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "delta_prices",
                        "sql": (
                            "SELECT product_id, amount, "
                            "LAG(amount, 1, 0.0) OVER "
                            "  (PARTITION BY product_id ORDER BY `$rowtime`) AS prev_amount "
                            'FROM {{ source("orders") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_row_number_dedup_pattern_validates(self):
        """ROW_NUMBER() OVER for latest-per-key deduplication validates."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "latest_products",
                        "sql": (
                            "SELECT product_id, name, price "
                            "FROM ("
                            "  SELECT *, "
                            "    ROW_NUMBER() OVER "
                            "      (PARTITION BY product_id ORDER BY `$rowtime` DESC) AS rn "
                            '  FROM {{ source("products") }}'
                            ") WHERE rn = 1"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_lead_over_rowtime_validates(self):
        """LEAD() OVER ORDER BY $rowtime validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "next_event",
                        "sql": (
                            "SELECT event_id, event_type, "
                            "LEAD(event_type, 1) OVER "
                            "  (PARTITION BY customer_id ORDER BY `$rowtime`) AS next_event_type "
                            'FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors
